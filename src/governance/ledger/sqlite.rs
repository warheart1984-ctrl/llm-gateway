//! The durable single-node ledger on SQLite.
//!
//! For one gateway without a Postgres server, and for each replica in
//! quota-split mode (`[ledger] quota_split_replicas`). Same schema, same
//! rules, and the same guarantees as [`super::PostgresLedger`] within one
//! host:
//!
//! * **Durable before the provider is called.** WAL journal with
//!   `synchronous = FULL`: a reservation's commit is on disk when
//!   [`Ledger::try_reserve`] returns.
//! * **One writer at a time.** Every transaction starts with
//!   `BEGIN IMMEDIATE`, which takes the database's write lock up front, so
//!   check-and-reserve is serialized even across processes sharing the file,
//!   and a read-then-write can never deadlock on a lock upgrade.
//! * **Conditional transitions.** Admission is
//!   `UPDATE … WHERE spent < budget AND spent + amount <= budget`; closing is
//!   `UPDATE … WHERE id = ? AND state = 'open'`.
//!
//! What it is not: shared between hosts. Keep the file on a local disk (SQLite
//! over a network filesystem does not lock reliably), and give each replica
//! its own file.
//!
//! "Today" comes from the host clock, via SQLite's `strftime`.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use sqlx::{
    Row, SqlitePool,
    error::ErrorKind,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

use super::{
    Closing, Ledger, LedgerRefusal, NewReservation,
    decisions::{DECISION_QUEUE, Decision, DecisionKind, DecisionQuery, DecisionRecord},
    sealed::{ResponseSealer, binding},
};

const NOW: &str = "CAST(strftime('%s', 'now') AS INTEGER)";
const TODAY: &str = "(CAST(strftime('%s', 'now') AS INTEGER) / 86400)";

#[derive(Debug, Clone)]
pub struct SqliteOptions {
    /// The database file. Created if missing; its directory is not.
    pub path: PathBuf,
    /// Bound on every admission and snapshot, including waiting for the
    /// write lock. A busy or broken database refuses requests.
    pub timeout: Duration,
    pub sweep_after: Duration,
    pub sweep_interval: Duration,
    pub idempotency_retention: Duration,
    pub sealer: Option<Arc<ResponseSealer>>,
    /// Decision records older than this are deleted by the sweeper.
    pub decision_retention: Duration,
}

enum WriterMsg {
    Close(Closing),
    Flush(oneshot::Sender<()>),
}

impl std::fmt::Debug for WriterMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterMsg::Close(c) => f.debug_tuple("Close").field(&c.id).finish(),
            WriterMsg::Flush(_) => f.write_str("Flush"),
        }
    }
}

#[derive(Debug)]
pub struct SqliteLedger {
    pool: SqlitePool,
    writer: mpsc::UnboundedSender<WriterMsg>,
    pending: Arc<AtomicU64>,
    timeout: Duration,
    retention: Duration,
    sealer: Option<Arc<ResponseSealer>>,
    sweeper: JoinHandle<()>,
    decision_queue: mpsc::Sender<Decision>,
    decisions_dropped: Arc<AtomicU64>,
}

impl Drop for SqliteLedger {
    fn drop(&mut self) {
        self.sweeper.abort();
    }
}

fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

fn clamp_delta(v: i128) -> i64 {
    v.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

fn unavailable(op: &str, err: impl std::fmt::Display) -> LedgerRefusal {
    tracing::warn!(op, error = %err, "ledger operation failed");
    LedgerRefusal::Unavailable(format!("the spend ledger could not complete `{op}`"))
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .is_some_and(|e| e.kind() == ErrorKind::UniqueViolation)
}

impl SqliteLedger {
    /// Open (or create) the database file, run migrations, and start the
    /// writer and the sweeper.
    pub async fn connect(opts: SqliteOptions) -> Result<Arc<Self>, String> {
        // Built from the path, never from a `sqlite::memory:` URL: that keeps
        // the in-memory flag even after `filename`, and nothing reaches disk.
        let connect = SqliteConnectOptions::new()
            .filename(&opts.path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5));
        // Several connections: under WAL, reads (snapshots, readiness) run
        // alongside the one writer instead of queueing behind it. Writers
        // are serialized by `BEGIN IMMEDIATE`, within this process and across
        // processes alike, so more connections never mean more writers.
        // All of them opened at boot: opening a connection under write
        // contention can outlast a request's deadline.
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .min_connections(4)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(connect)
            .await
            .map_err(|e| format!("cannot open the ledger database `{}`: {e}", opts.path.display()))?;
        sqlx::migrate!("./migrations/sqlite")
            .run(&pool)
            .await
            .map_err(|e| format!("ledger migration failed: {e}"))?;

        let (writer, inbox) = mpsc::unbounded_channel();
        let pending = Arc::new(AtomicU64::new(0));
        tokio::spawn(run_writer(pool.clone(), inbox, Arc::clone(&pending), opts.sealer.clone()));
        let sweeper = tokio::spawn(run_sweeper(
            pool.clone(),
            opts.sweep_after,
            opts.sweep_interval,
            opts.decision_retention,
        ));
        let (decision_queue, decision_inbox) = mpsc::channel(DECISION_QUEUE);
        let decisions_dropped = Arc::new(AtomicU64::new(0));
        tokio::spawn(run_decision_writer(pool.clone(), decision_inbox, Arc::clone(&decisions_dropped)));

        Ok(Arc::new(Self {
            pool,
            writer,
            pending,
            timeout: opts.timeout,
            retention: opts.idempotency_retention,
            sealer: opts.sealer,
            sweeper,
            decision_queue,
            decisions_dropped,
        }))
    }

    /// The connection pool, for operational queries and tests.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub fn pending_closings(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }

    /// Close every reservation left `open` for longer than `older_than` as
    /// `swept`, keeping its full reservation as the bill. Returns how many.
    pub async fn sweep(&self, older_than: Duration) -> Result<u64, String> {
        sweep(&self.pool, older_than).await.map_err(|e| e.to_string())
    }

    fn open_response(&self, tenant_id: &str, id: Uuid, stored: Option<String>) -> Option<String> {
        let stored = stored?;
        let opened = self.sealer.as_ref().and_then(|s| s.open(&binding(tenant_id, id), &stored));
        if opened.is_none() {
            tracing::warn!(
                reservation = %id,
                "a stored answer did not open (unknown key id, tampered, or no key configured); not replaying it"
            );
        }
        opened
    }

    async fn reserve_tx(&self, r: &NewReservation<'_>) -> Result<Result<u64, LedgerRefusal>, sqlx::Error> {
        // Dropping `tx` on any early return rolls back, including when the
        // caller's deadline cancels this future.
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let today: i64 = sqlx::query_scalar(&format!("SELECT {TODAY}")).fetch_one(&mut *tx).await?;

        if let Some(claim) = &r.idempotency {
            let held = sqlx::query(&format!(
                "SELECT id, state, fingerprint, reserved_nano_usd, delta_nano_usd, response,
                        (closed_at IS NOT NULL AND closed_at < {NOW} - ?3) AS expired
                   FROM reservations
                  WHERE tenant_id = ?1 AND idempotency_key = ?2"
            ))
            .bind(r.tenant_id)
            .bind(claim.key)
            .bind(self.retention.as_secs() as i64)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(row) = held {
                let id_text: String = row.get("id");
                let id = Uuid::parse_str(&id_text).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
                let expired: i64 = row.get("expired");
                if expired == 0 {
                    let fingerprint: Option<Vec<u8>> = row.get("fingerprint");
                    if !fingerprint.as_deref().is_some_and(|stored| claim.matches(stored)) {
                        return Ok(Err(LedgerRefusal::IdempotencyKeyReused));
                    }
                    match row.get::<String, _>("state").as_str() {
                        "open" => return Ok(Err(LedgerRefusal::InProgress { original: Some(id) })),
                        "released" => {}
                        _ => {
                            let reserved: i64 = row.get("reserved_nano_usd");
                            let delta: Option<i64> = row.get("delta_nano_usd");
                            return Ok(Err(LedgerRefusal::Duplicate {
                                original: id,
                                billed_nano_usd: (reserved + delta.unwrap_or(0)).max(0) as u64,
                                response: self.open_response(r.tenant_id, id, row.get("response")),
                            }));
                        }
                    }
                }
                sqlx::query("UPDATE reservations SET idempotency_key = NULL WHERE id = ?1")
                    .bind(&id_text)
                    .execute(&mut *tx)
                    .await?;
            }
        }

        sqlx::query("INSERT INTO spend_days (tenant_id, day) VALUES (?1, ?2) ON CONFLICT DO NOTHING")
            .bind(r.tenant_id)
            .bind(today)
            .execute(&mut *tx)
            .await?;
        let amount = to_i64(r.amount_nano_usd);
        let budget = to_i64(r.budget_nano_usd);
        // The same rule as Postgres, including `spent < budget`: the short
        // form admits a zero-cost request at exactly the budget.
        let charged: Option<i64> = sqlx::query_scalar(
            "UPDATE spend_days SET spent_nano_usd = spent_nano_usd + ?3
              WHERE tenant_id = ?1 AND day = ?2
                AND (?4 = 0 OR (spent_nano_usd < ?4 AND spent_nano_usd + ?3 <= ?4))
          RETURNING spent_nano_usd",
        )
        .bind(r.tenant_id)
        .bind(today)
        .bind(amount)
        .bind(budget)
        .fetch_optional(&mut *tx)
        .await?;
        if charged.is_none() {
            let spent: i64 =
                sqlx::query_scalar("SELECT spent_nano_usd FROM spend_days WHERE tenant_id = ?1 AND day = ?2")
                    .bind(r.tenant_id)
                    .bind(today)
                    .fetch_one(&mut *tx)
                    .await?;
            let (spent, budget) = (spent.max(0) as u64, r.budget_nano_usd);
            return Ok(Err(if spent >= budget {
                LedgerRefusal::BudgetExhausted { spent, budget }
            } else {
                LedgerRefusal::BudgetWouldBeExceeded {
                    estimate: r.amount_nano_usd,
                    remaining: budget - spent,
                }
            }));
        }

        let inserted = sqlx::query(&format!(
            "INSERT INTO reservations
                 (id, tenant_id, day, reserved_nano_usd, prompt_nano_usd, idempotency_key, fingerprint, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, {NOW})"
        ))
        .bind(r.id.to_string())
        .bind(r.tenant_id)
        .bind(today)
        .bind(amount)
        .bind(to_i64(r.prompt_nano_usd))
        .bind(r.idempotency.as_ref().map(|c| c.key))
        .bind(r.idempotency.as_ref().map(|c| c.fingerprint.clone()))
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_) => {}
            Err(e) if is_unique_violation(&e) => {
                return Ok(Err(LedgerRefusal::InProgress { original: None }));
            }
            Err(e) => return Err(e),
        }
        tx.commit().await?;
        Ok(Ok(today.max(0) as u64))
    }
}

async fn apply_close(pool: &SqlitePool, c: &Closing, sealed_response: Option<&str>) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let delta = clamp_delta(c.delta_nano_usd);
    let closed = sqlx::query(&format!(
        "UPDATE reservations
            SET state = ?2, delta_nano_usd = ?3, response = ?4, closed_at = {NOW}
          WHERE id = ?1 AND state = 'open'
      RETURNING tenant_id, day"
    ))
    .bind(c.id.to_string())
    .bind(c.outcome.as_str())
    .bind(delta)
    .bind(sealed_response)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = closed else {
        return tx.commit().await;
    };
    let tenant: String = row.get("tenant_id");
    let day: i64 = row.get("day");
    let today: i64 = sqlx::query_scalar(&format!("SELECT {TODAY}")).fetch_one(&mut *tx).await?;

    // The rollover rule, identical to the other backends.
    if delta < 0 && day == today {
        sqlx::query(
            "UPDATE spend_days SET spent_nano_usd = MAX(spent_nano_usd + ?3, 0)
              WHERE tenant_id = ?1 AND day = ?2",
        )
        .bind(&tenant)
        .bind(day)
        .bind(delta)
        .execute(&mut *tx)
        .await?;
    } else if delta > 0 {
        sqlx::query("INSERT INTO spend_days (tenant_id, day) VALUES (?1, ?2) ON CONFLICT DO NOTHING")
            .bind(&tenant)
            .bind(today)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE spend_days SET spent_nano_usd = spent_nano_usd + ?3 WHERE tenant_id = ?1 AND day = ?2")
            .bind(&tenant)
            .bind(today)
            .bind(delta)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await
}

async fn run_writer(
    pool: SqlitePool,
    mut inbox: mpsc::UnboundedReceiver<WriterMsg>,
    pending: Arc<AtomicU64>,
    sealer: Option<Arc<ResponseSealer>>,
) {
    while let Some(msg) = inbox.recv().await {
        match msg {
            WriterMsg::Flush(done) => {
                let _ = done.send(());
            }
            WriterMsg::Close(closing) => {
                let sealed = match (&sealer, &closing.response) {
                    (Some(s), Some(answer)) => Some(s.seal(&binding(&closing.tenant_id, closing.id), answer)),
                    _ => None,
                };
                let mut backoff = Duration::from_millis(100);
                loop {
                    match apply_close(&pool, &closing, sealed.as_deref()).await {
                        Ok(()) => break,
                        Err(error) => {
                            tracing::warn!(
                                reservation = %closing.id,
                                %error,
                                retry_in_ms = backoff.as_millis() as u64,
                                "ledger closing failed; retrying"
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                        }
                    }
                }
                pending.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

async fn sweep(pool: &SqlitePool, older_than: Duration) -> Result<u64, sqlx::Error> {
    let swept = sqlx::query(&format!(
        "UPDATE reservations SET state = 'swept', delta_nano_usd = 0, closed_at = {NOW}
          WHERE state = 'open' AND created_at < {NOW} - ?1"
    ))
    .bind(older_than.as_secs() as i64)
    .execute(pool)
    .await?;
    Ok(swept.rows_affected())
}

async fn insert_decision(pool: &SqlitePool, d: &Decision) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "INSERT INTO decisions (request_id, tenant_id, key_id, kind, endpoint, model, code, reason, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, {NOW})"
    ))
    .bind(d.request_id.to_string())
    .bind(&d.tenant_id)
    .bind(&d.key_id)
    .bind(d.kind.as_str())
    .bind(&d.endpoint)
    .bind(d.model.as_deref())
    .bind(&d.code)
    .bind(&d.reason)
    .execute(pool)
    .await
    .map(|_| ())
}

/// Routine records: one attempt each. A failure drops and counts the record
/// instead of retrying, so a struggling database never grows a backlog.
async fn run_decision_writer(pool: SqlitePool, mut inbox: mpsc::Receiver<Decision>, dropped: Arc<AtomicU64>) {
    while let Some(decision) = inbox.recv().await {
        if let Err(error) = insert_decision(&pool, &decision).await {
            dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, "a decision record was dropped");
        }
    }
}

async fn run_sweeper(pool: SqlitePool, sweep_after: Duration, interval: Duration, decision_retention: Duration) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
    loop {
        ticker.tick().await;
        if let Err(error) = sqlx::query(&format!("DELETE FROM decisions WHERE created_at < {NOW} - ?1"))
            .bind(decision_retention.as_secs() as i64)
            .execute(&pool)
            .await
        {
            tracing::warn!(%error, "decision retention sweep failed; will retry");
        }
        match sweep(&pool, sweep_after).await {
            Ok(0) => {}
            Ok(n) => tracing::warn!(
                reservations = n,
                "swept reservations left open by a crash; each keeps its full reservation as the bill"
            ),
            Err(error) => tracing::warn!(%error, "ledger sweep failed; will retry"),
        }
    }
}

#[async_trait::async_trait]
impl Ledger for SqliteLedger {
    async fn try_reserve(&self, r: NewReservation<'_>) -> Result<u64, LedgerRefusal> {
        match tokio::time::timeout(self.timeout, self.reserve_tx(&r)).await {
            Ok(Ok(decision)) => decision,
            Ok(Err(error)) => Err(unavailable("reserve", error)),
            Err(_) => Err(unavailable("reserve", "timed out")),
        }
    }

    fn close(&self, c: Closing) {
        self.pending.fetch_add(1, Ordering::Relaxed);
        if self.writer.send(WriterMsg::Close(c)).is_err() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            tracing::error!("ledger writer has stopped; the sweeper will bill this reservation in full");
        }
    }

    async fn snapshot(&self, tenant_id: &str, _now: SystemTime) -> Result<(u64, u64), LedgerRefusal> {
        let sql = format!(
            "SELECT {TODAY}, COALESCE(
                 (SELECT spent_nano_usd FROM spend_days WHERE tenant_id = ?1 AND day = {TODAY}), 0)"
        );
        let query = sqlx::query_as::<_, (i64, i64)>(&sql).bind(tenant_id).fetch_one(&self.pool);
        match tokio::time::timeout(self.timeout, query).await {
            Ok(Ok((day, spent))) => Ok((day.max(0) as u64, spent.max(0) as u64)),
            Ok(Err(error)) => Err(unavailable("snapshot", error)),
            Err(_) => Err(unavailable("snapshot", "timed out")),
        }
    }

    async fn flush(&self) {
        let (done, wait) = oneshot::channel();
        if self.writer.send(WriterMsg::Flush(done)).is_ok() {
            let _ = wait.await;
        }
    }

    async fn healthy(&self) -> bool {
        let ping = sqlx::query_scalar::<_, i64>("SELECT 1").fetch_one(&self.pool);
        matches!(tokio::time::timeout(self.timeout, ping).await, Ok(Ok(1)))
    }

    fn backend(&self) -> &'static str {
        "sqlite"
    }

    fn record_decision(&self, decision: Decision) {
        if self.decision_queue.try_send(decision).is_err() {
            self.decisions_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn record_decision_durably(&self, decision: Decision) -> Result<(), LedgerRefusal> {
        match tokio::time::timeout(self.timeout, insert_decision(&self.pool, &decision)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(unavailable("record decision", error)),
            Err(_) => Err(unavailable("record decision", "timed out")),
        }
    }

    async fn decisions(&self, q: DecisionQuery) -> Result<Vec<DecisionRecord>, LedgerRefusal> {
        let rows = sqlx::query(
            "SELECT request_id, tenant_id, key_id, kind, endpoint, model, code, reason, created_at AS at
               FROM decisions
              WHERE (?1 IS NULL OR tenant_id = ?1) AND created_at >= ?2
           ORDER BY created_at DESC, id DESC
              LIMIT ?3",
        )
        .bind(q.tenant_id.as_deref())
        .bind(q.since)
        .bind(i64::from(q.limit))
        .fetch_all(&self.pool);
        let rows = match tokio::time::timeout(self.timeout, rows).await {
            Ok(Ok(rows)) => rows,
            Ok(Err(error)) => return Err(unavailable("decisions", error)),
            Err(_) => return Err(unavailable("decisions", "timed out")),
        };
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                Some(DecisionRecord {
                    decision: Decision {
                        request_id: Uuid::parse_str(&row.get::<String, _>("request_id")).ok()?,
                        tenant_id: row.get("tenant_id"),
                        key_id: row.get("key_id"),
                        kind: DecisionKind::parse(row.get::<String, _>("kind").as_str())?,
                        endpoint: row.get("endpoint"),
                        model: row.get("model"),
                        code: row.get("code"),
                        reason: row.get("reason"),
                    },
                    at: row.get("at"),
                })
            })
            .collect())
    }

    fn decisions_dropped(&self) -> u64 {
        self.decisions_dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::ledger::{IdempotencyClaim, Outcome};

    struct TempFile(PathBuf);

    impl TempFile {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("llm-gateway-sqlite-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir.join("ledger.sqlite3"))
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            if let Some(dir) = self.0.parent() {
                let _ = std::fs::remove_dir_all(dir);
            }
        }
    }

    const KEYS: &str = "k1:AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

    async fn open(file: &TempFile) -> Arc<SqliteLedger> {
        SqliteLedger::connect(SqliteOptions {
            path: file.0.clone(),
            timeout: Duration::from_secs(5),
            sweep_after: Duration::from_secs(3_600),
            sweep_interval: Duration::from_secs(3_600),
            idempotency_retention: Duration::from_secs(86_400),
            sealer: Some(Arc::new(ResponseSealer::from_keys(KEYS).unwrap())),
            decision_retention: Duration::from_secs(30 * 86_400),
        })
        .await
        .unwrap()
    }

    fn reservation<'a>(tenant: &'a str, amount: u64, budget: u64) -> NewReservation<'a> {
        NewReservation {
            id: Uuid::new_v4(),
            tenant_id: tenant,
            now: SystemTime::now(),
            amount_nano_usd: amount,
            prompt_nano_usd: amount / 4,
            budget_nano_usd: budget,
            idempotency: None,
        }
    }

    fn closing(id: Uuid, tenant: &str, bucket: u64, delta: i128, outcome: Outcome) -> Closing {
        Closing {
            id,
            tenant_id: Arc::from(tenant),
            bucket,
            delta_nano_usd: delta,
            outcome,
            at: SystemTime::now(),
            response: None,
        }
    }

    async fn spent(ledger: &SqliteLedger, tenant: &str) -> u64 {
        ledger.snapshot(tenant, SystemTime::now()).await.unwrap().1
    }

    #[tokio::test]
    async fn a_settlement_applies_once_however_often_it_is_sent() {
        let file = TempFile::new();
        let ledger = open(&file).await;
        let r = reservation("t", 1_000, 0);
        let bucket = ledger.try_reserve(r.clone()).await.unwrap();
        for _ in 0..3 {
            ledger.close(closing(r.id, "t", bucket, -600, Outcome::Settled));
        }
        ledger.flush().await;
        assert_eq!(spent(&ledger, "t").await, 400);
    }

    #[tokio::test]
    async fn a_zero_cost_request_is_refused_at_exactly_the_budget() {
        let file = TempFile::new();
        let ledger = open(&file).await;
        ledger.try_reserve(reservation("t", 1_000, 1_000)).await.unwrap();
        assert_eq!(
            ledger.try_reserve(reservation("t", 0, 1_000)).await.unwrap_err(),
            LedgerRefusal::BudgetExhausted { spent: 1_000, budget: 1_000 }
        );
    }

    #[tokio::test]
    async fn spend_survives_a_restart() {
        let file = TempFile::new();
        {
            let ledger = open(&file).await;
            let r = reservation("t", 1_000, 0);
            let bucket = ledger.try_reserve(r.clone()).await.unwrap();
            ledger.close(closing(r.id, "t", bucket, -250, Outcome::Settled));
            ledger.flush().await;
        }
        let reopened = open(&file).await;
        assert_eq!(spent(&reopened, "t").await, 750);
    }

    #[tokio::test]
    async fn a_crash_before_settlement_is_billed_in_full_by_the_sweeper() {
        let file = TempFile::new();
        let id = {
            let ledger = open(&file).await;
            let r = reservation("t", 1_000, 0);
            ledger.try_reserve(r.clone()).await.unwrap();
            r.id
            // Dropped without a closing: the process died mid-request.
        };
        let reopened = open(&file).await;
        assert_eq!(spent(&reopened, "t").await, 1_000, "the reservation was durable before the crash");
        sqlx::query(&format!("UPDATE reservations SET created_at = {NOW} - 7200 WHERE id = ?1"))
            .bind(id.to_string())
            .execute(reopened.pool())
            .await
            .unwrap();
        // The background sweeper also runs at boot and may get there first;
        // what matters is the end state, whoever swept it.
        assert!(reopened.sweep(Duration::from_secs(3_600)).await.unwrap() <= 1);
        let state: String = sqlx::query_scalar("SELECT state FROM reservations WHERE id = ?1")
            .bind(id.to_string())
            .fetch_one(reopened.pool())
            .await
            .unwrap();
        assert_eq!(state, "swept");
        assert_eq!(spent(&reopened, "t").await, 1_000, "swept keeps the full reservation as the bill");
        // A late closing from the dead process's peer changes nothing now.
        reopened.close(closing(id, "t", 0, -1_000, Outcome::Released));
        reopened.flush().await;
        assert_eq!(spent(&reopened, "t").await, 1_000);
    }

    #[tokio::test]
    async fn refunds_to_a_closed_day_are_dropped_and_overages_charged_today() {
        let file = TempFile::new();
        let ledger = open(&file).await;
        let today: i64 = sqlx::query_scalar(&format!("SELECT {TODAY}")).fetch_one(ledger.pool()).await.unwrap();
        ledger.try_reserve(reservation("t", 500, 0)).await.unwrap();
        for (id, _) in [(Uuid::new_v4(), ()), (Uuid::new_v4(), ())] {
            sqlx::query(&format!(
                "INSERT INTO reservations (id, tenant_id, day, reserved_nano_usd, prompt_nano_usd, created_at)
                 VALUES (?1, 't', ?2, 1000, 100, {NOW})"
            ))
            .bind(id.to_string())
            .bind(today - 1)
            .execute(ledger.pool())
            .await
            .unwrap();
            let refund = closing(id, "t", (today - 1) as u64, -900, Outcome::Settled);
            ledger.close(refund);
        }
        ledger.flush().await;
        assert_eq!(spent(&ledger, "t").await, 500, "yesterday's refunds never reach today");

        let late = Uuid::new_v4();
        sqlx::query(&format!(
            "INSERT INTO reservations (id, tenant_id, day, reserved_nano_usd, prompt_nano_usd, created_at)
             VALUES (?1, 't', ?2, 1000, 100, {NOW})"
        ))
        .bind(late.to_string())
        .bind(today - 1)
        .execute(ledger.pool())
        .await
        .unwrap();
        ledger.close(closing(late, "t", (today - 1) as u64, 70, Outcome::Settled));
        ledger.flush().await;
        assert_eq!(spent(&ledger, "t").await, 570, "an overage is real spend, charged today");
    }

    #[tokio::test]
    async fn idempotency_rules_hold_on_disk() {
        let file = TempFile::new();
        let ledger = open(&file).await;
        let claim = |fp: u8| IdempotencyClaim { key: "k", fingerprint: vec![fp; 32], also_matches: vec![] };
        let mut first = reservation("t", 1_000, 0);
        first.idempotency = Some(claim(1));
        let bucket = ledger.try_reserve(first.clone()).await.unwrap();

        let mut again = reservation("t", 1_000, 0);
        again.idempotency = Some(claim(1));
        assert_eq!(
            ledger.try_reserve(again.clone()).await.unwrap_err(),
            LedgerRefusal::InProgress { original: Some(first.id) }
        );
        let mut other = reservation("t", 1_000, 0);
        other.idempotency = Some(claim(2));
        assert_eq!(ledger.try_reserve(other).await.unwrap_err(), LedgerRefusal::IdempotencyKeyReused);

        let mut done = closing(first.id, "t", bucket, -200, Outcome::Settled);
        done.response = Some("the answer".into());
        ledger.close(done);
        ledger.flush().await;
        match ledger.try_reserve(again).await.unwrap_err() {
            LedgerRefusal::Duplicate { original, billed_nano_usd, response } => {
                assert_eq!(original, first.id);
                assert_eq!(billed_nano_usd, 800);
                assert_eq!(response.as_deref(), Some("the answer"));
            }
            other => panic!("expected a duplicate, got {other:?}"),
        }
        assert_eq!(spent(&ledger, "t").await, 800, "the duplicate charged nothing");

        // The answer is sealed on disk.
        drop(ledger);
        let bytes = std::fs::read(&file.0).unwrap();
        let wal = std::fs::read(file.0.with_extension("sqlite3-wal")).unwrap_or_default();
        let needle = b"the answer";
        let contains = |hay: &[u8]| hay.windows(needle.len()).any(|w| w == needle);
        assert!(!contains(&bytes) && !contains(&wal), "the answer must not be on disk in the clear");
    }

    #[tokio::test]
    async fn a_released_attempt_frees_its_key() {
        let file = TempFile::new();
        let ledger = open(&file).await;
        let claim = IdempotencyClaim { key: "k", fingerprint: vec![7; 32], also_matches: vec![] };
        let mut first = reservation("t", 1_000, 0);
        first.idempotency = Some(claim.clone());
        let bucket = ledger.try_reserve(first.clone()).await.unwrap();
        ledger.close(closing(first.id, "t", bucket, -1_000, Outcome::Released));
        ledger.flush().await;
        let mut retry = reservation("t", 1_000, 0);
        retry.idempotency = Some(claim);
        ledger.try_reserve(retry).await.expect("nothing ran, so the key may run again");
    }

    #[tokio::test]
    async fn two_processes_on_one_file_never_overshoot_the_budget() {
        // Two ledger instances are two connections, as two processes on one
        // host would be. `BEGIN IMMEDIATE` serializes their admissions.
        let file = TempFile::new();
        let (a, b) = (open(&file).await, open(&file).await);
        let tasks: Vec<_> = (0..40)
            .map(|i| {
                let ledger = if i % 2 == 0 { Arc::clone(&a) } else { Arc::clone(&b) };
                tokio::spawn(async move { ledger.try_reserve(reservation("t", 100, 1_050)).await.is_ok() })
            })
            .collect();
        let admitted = futures_util::future::join_all(tasks)
            .await
            .into_iter()
            .filter(|r| *r.as_ref().unwrap())
            .count();
        assert_eq!(admitted, 10);
        assert_eq!(spent(&a, "t").await, 1_000);
    }
}
