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
//! * **Leases, rate and concurrency limits** exactly as in the Postgres
//!   ledger: a held reservation's lease is renewed until its closing is
//!   durable, the sweeper takes only lapsed leases, and requests, tokens and
//!   live reservations are counted in the admission transaction.
//!
//! What it is not: shared between hosts. Keep the file on a local disk (SQLite
//! over a network filesystem does not lock reliably), and give each replica
//! its own file.
//!
//! "Today" comes from the host clock, via SQLite's `strftime`.

use std::{
    future::Future,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use dashmap::DashMap;
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
    Closing, CommitOutcome, Ledger, LedgerRefusal, NewReservation, Outcome, Upkeep, UpkeepCounts, commit_within,
    refuse_if_backlogged,
    release_clean,
    decisions::{DECISION_QUEUE, Decision, DecisionKind, DecisionQuery, DecisionRecord},
    holds::{self, HoldClaim, HoldDecision, HoldProblem, HoldQuery, HoldRecord, HoldState, NewHold, Transition},
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
    /// How long a reservation's lease lasts without renewal.
    pub lease: Duration,
    pub sweep_interval: Duration,
    pub idempotency_retention: Duration,
    pub sealer: Option<Arc<ResponseSealer>>,
    /// Decision records older than this are deleted by the sweeper.
    pub decision_retention: Duration,
    /// Closings waiting to be written, at most, before admissions are
    /// refused. 0: no bound.
    pub max_pending_closings: u64,
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
    renewer: JoinHandle<()>,
    lease: Duration,
    live: Arc<DashMap<Uuid, ()>>,
    decision_queue: mpsc::Sender<Decision>,
    decisions_dropped: Arc<AtomicU64>,
    upkeep: Arc<Upkeep>,
    max_pending_closings: u64,
}

impl Drop for SqliteLedger {
    fn drop(&mut self) {
        self.sweeper.abort();
        self.renewer.abort();
    }
}

/// Direct attempts to release a late commit before handing it to the writer.
const LATE_RELEASE_ATTEMPTS: u32 = 5;

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
        let upkeep = Arc::new(Upkeep::default());
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .min_connections(4)
            .acquire_timeout(Duration::from_secs(10))
            .after_release({
                let upkeep = Arc::clone(&upkeep);
                move |conn, _| {
                    let upkeep = Arc::clone(&upkeep);
                    Box::pin(async move { Ok(release_clean(conn, &upkeep).await) })
                }
            })
            .connect_with(connect)
            .await
            .map_err(|e| format!("cannot open the ledger database `{}`: {e}", opts.path.display()))?;
        sqlx::migrate!("./migrations/sqlite")
            .run(&pool)
            .await
            .map_err(|e| format!("ledger migration failed: {e}"))?;

        let (writer, inbox) = mpsc::unbounded_channel();
        let pending = Arc::new(AtomicU64::new(0));
        let live: Arc<DashMap<Uuid, ()>> = Arc::default();
        tokio::spawn(run_writer(
            pool.clone(),
            inbox,
            Arc::clone(&pending),
            opts.sealer.clone(),
            Arc::clone(&live),
        ));
        let sweeper = tokio::spawn(run_sweeper(
            pool.clone(),
            opts.sweep_interval,
            opts.decision_retention,
            Arc::clone(&upkeep),
        ));
        let renewer = tokio::spawn(run_renewer(pool.clone(), Arc::clone(&live), opts.lease));
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
            renewer,
            lease: opts.lease,
            live,
            decision_queue,
            decisions_dropped,
            upkeep,
            max_pending_closings: opts.max_pending_closings,
        }))
    }

    /// The connection pool, for operational queries and tests.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub fn pending_closings(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }

    /// Close every reservation whose lease has lapsed as `swept`, keeping
    /// its full reservation as the bill. Returns how many.
    pub async fn sweep(&self) -> Result<u64, String> {
        sweep(&self.pool).await.map_err(|e| e.to_string())
    }

    /// Mark every hold whose time ran out as expired, recording each. The
    /// sweeper does this on its interval. Returns how many.
    pub async fn expire_holds(&self) -> Result<u64, String> {
        expire_holds(&self.pool).await.map_err(|e| e.to_string())
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

    /// What to do if this admission's commit lands after it was refused:
    /// release the reservation, exactly as for a request the provider never
    /// saw. Lazy: it runs only if awaited.
    ///
    /// Written straight to the database, not queued: a late commit happens
    /// when the ledger is overloaded, which is when the writer's queue is
    /// deepest, and a release that waited behind it could lose to the
    /// sweeper and become the full bill it exists to prevent. If the direct
    /// write keeps failing, the writer takes it, and retries until durable.
    fn release_if_late(&self, r: &NewReservation<'_>, bucket: u64) -> impl Future<Output = ()> + Send + 'static {
        let closing = Closing {
            id: r.id,
            tenant_id: Arc::from(r.tenant_id),
            bucket,
            delta_nano_usd: -(r.amount_nano_usd as i128),
            outcome: Outcome::Released,
            at: SystemTime::now(),
            response: None,
            completion_tokens: 0,
        };
        let pool = self.pool.clone();
        let (writer, pending, upkeep) = (self.writer.clone(), Arc::clone(&self.pending), Arc::clone(&self.upkeep));
        async move {
            upkeep.late_commit_released();
            tracing::warn!(
                reservation = %closing.id,
                tenant = %closing.tenant_id,
                "a reservation committed after its admission timed out; releasing it at no charge"
            );
            for attempt in 0..LATE_RELEASE_ATTEMPTS {
                match apply_close(&pool, &closing, None).await {
                    Ok(()) => return,
                    Err(error) => {
                        tracing::warn!(%error, attempt, reservation = %closing.id, "releasing a late commit failed; retrying");
                        tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                    }
                }
            }
            pending.fetch_add(1, Ordering::Relaxed);
            if writer.send(WriterMsg::Close(closing)).is_err() {
                pending.fetch_sub(1, Ordering::Relaxed);
                tracing::error!("ledger writer has stopped; the sweeper will bill this reservation in full");
            }
        }
    }

    /// Everything an admission decides, up to but not including the commit.
    /// Returns the open transaction and the day charged; dropping the
    /// transaction rolls the whole admission back.
    async fn decide(
        &self,
        r: &NewReservation<'_>,
    ) -> Result<Result<(sqlx::Transaction<'static, sqlx::Sqlite>, i64), LedgerRefusal>, sqlx::Error> {
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
        // After the key, so a repeat of an executed request is answered as a
        // repeat; before the money, and rolled back with it on any refusal.
        if let Some(claim) = &r.hold
            && let Some(problem) = consume_hold(&mut tx, r, claim).await?
        {
            return Ok(Err(LedgerRefusal::Hold(problem)));
        }

        sqlx::query("INSERT INTO spend_days (tenant_id, day) VALUES (?1, ?2) ON CONFLICT DO NOTHING")
            .bind(r.tenant_id)
            .bind(today)
            .execute(&mut *tx)
            .await?;
        // `BEGIN IMMEDIATE` already holds the write lock, so counting here
        // cannot be raced by any other admission, in or out of this process.
        if r.limits.max_concurrent > 0 {
            let live: i64 = sqlx::query_scalar(&format!(
                "SELECT count(*) FROM reservations
                  WHERE tenant_id = ?1 AND state = 'open' AND lease_expires_at > {NOW}"
            ))
            .bind(r.tenant_id)
            .fetch_one(&mut *tx)
            .await?;
            if live >= i64::from(r.limits.max_concurrent) {
                return Ok(Err(LedgerRefusal::ConcurrencyLimited { limit: r.limits.max_concurrent }));
            }
        }
        if let Some(refusal) = charge_rate(&mut tx, r).await? {
            return Ok(Err(refusal));
        }
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
                 (id, tenant_id, day, reserved_nano_usd, prompt_nano_usd, idempotency_key, fingerprint,
                  created_at, lease_expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, {NOW}, {NOW} + ?8)"
        ))
        .bind(r.id.to_string())
        .bind(r.tenant_id)
        .bind(today)
        .bind(amount)
        .bind(to_i64(r.prompt_nano_usd))
        .bind(r.idempotency.as_ref().map(|c| c.key))
        .bind(r.idempotency.as_ref().map(|c| c.fingerprint.clone()))
        .bind(lease_secs(self.lease))
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_) => {}
            Err(e) if is_unique_violation(&e) => {
                return Ok(Err(LedgerRefusal::InProgress { original: None }));
            }
            Err(e) => return Err(e),
        }
        Ok(Ok((tx, today)))
    }
}

/// Mark an approved hold used by this reservation, or say why it cannot be.
async fn consume_hold(
    tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
    r: &NewReservation<'_>,
    claim: &HoldClaim,
) -> Result<Option<HoldProblem>, sqlx::Error> {
    let row = sqlx::query(&format!(
        "SELECT state, fingerprint, model, endpoint, exposure_nano_usd, (expires_at <= {NOW}) AS lapsed
           FROM holds WHERE id = ?1 AND tenant_id = ?2"
    ))
    .bind(claim.id.to_string())
    .bind(r.tenant_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Ok(Some(HoldProblem::NotFound));
    };
    let state = HoldState::parse(&row.get::<String, _>("state"))
        .unwrap_or(HoldState::Expired)
        .effective(row.get::<i64, _>("lapsed") != 0);
    let fingerprint: Vec<u8> = row.get("fingerprint");
    let model: String = row.get("model");
    let approved: i64 = row.get("exposure_nano_usd");
    if let Err(problem) = claim.check(state, &fingerprint, &model, approved.max(0) as u64) {
        return Ok(Some(problem));
    }
    sqlx::query("UPDATE holds SET state = 'consumed', reservation_id = ?2 WHERE id = ?1")
        .bind(claim.id.to_string())
        .bind(r.id.to_string())
        .execute(&mut **tx)
        .await?;
    let used = Decision {
        request_id: claim.id,
        tenant_id: r.tenant_id.to_string(),
        key_id: claim.key_id.clone(),
        kind: DecisionKind::Hold,
        endpoint: row.get("endpoint"),
        model: Some(model),
        code: holds::CODE_CONSUMED.to_string(),
        reason: String::new(),
    }
    .with_reason(&format!("executed as request {}", r.id));
    insert_decision(&mut **tx, &used).await?;
    Ok(None)
}

const HOLD_COLUMNS: &str = "id, tenant_id, requested_by, endpoint, model, max_output_tokens, exposure_nano_usd,
    reason, state, created_at, expires_at, decided_by, decided_at, note, reservation_id";

/// A hold's state as callers see it, in SQL.
const HOLD_EFFECTIVE: &str =
    "(CASE WHEN state IN ('pending', 'approved') AND expires_at <= CAST(strftime('%s', 'now') AS INTEGER)
           THEN 'expired' ELSE state END)";

fn hold_from_row(row: &sqlx::sqlite::SqliteRow) -> Option<HoldRecord> {
    Some(HoldRecord {
        id: Uuid::parse_str(&row.get::<String, _>("id")).ok()?,
        tenant_id: row.get("tenant_id"),
        requested_by: row.get("requested_by"),
        endpoint: row.get("endpoint"),
        model: row.get("model"),
        max_output_tokens: row.get::<i64, _>("max_output_tokens").clamp(0, i64::from(u32::MAX)) as u32,
        exposure_nano_usd: row.get::<i64, _>("exposure_nano_usd").max(0) as u64,
        reason: row.get("reason"),
        state: HoldState::parse(&row.get::<String, _>("state"))?.effective(row.get::<i64, _>("lapsed") != 0),
        created_at: row.get("created_at"),
        expires_at: row.get("expires_at"),
        decided_by: row.get("decided_by"),
        decided_at: row.get("decided_at"),
        note: row.get("note"),
        reservation_id: row
            .get::<Option<String>, _>("reservation_id")
            .and_then(|id| Uuid::parse_str(&id).ok()),
    })
}

async fn select_hold<'e, E>(ex: E, id: Uuid) -> Result<Option<HoldRecord>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(&format!(
        "SELECT {HOLD_COLUMNS}, (expires_at <= {NOW}) AS lapsed FROM holds WHERE id = ?1"
    ))
    .bind(id.to_string())
    .fetch_optional(ex)
    .await?;
    Ok(row.as_ref().and_then(hold_from_row))
}

/// Mark every hold whose time ran out as expired, and record each, as one
/// step. The time is read once, so the record and the transition agree.
async fn expire_holds(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let now: i64 = sqlx::query_scalar(&format!("SELECT {NOW}")).fetch_one(&mut *tx).await?;
    sqlx::query(
        "INSERT INTO decisions (request_id, tenant_id, key_id, kind, endpoint, model, code, reason, created_at)
         SELECT id, tenant_id, requested_by, 'hold', endpoint, model, ?2,
                CASE state WHEN 'pending' THEN ?3 ELSE ?4 END, ?1
           FROM holds WHERE state IN ('pending', 'approved') AND expires_at <= ?1",
    )
    .bind(now)
    .bind(holds::CODE_EXPIRED)
    .bind(holds::EXPIRED_UNDECIDED)
    .bind(holds::EXPIRED_UNUSED)
    .execute(&mut *tx)
    .await?;
    let expired = sqlx::query("UPDATE holds SET state = 'expired' WHERE state IN ('pending', 'approved') AND expires_at <= ?1")
        .bind(now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(expired.rows_affected())
}

/// Whole seconds, at least one: SQLite times here are Unix seconds.
fn lease_secs(lease: Duration) -> i64 {
    lease.as_secs().max(1) as i64
}

const MINUTE: &str = "(CAST(strftime('%s', 'now') AS INTEGER) / 60)";

/// Count this admission against the tenant's aligned minute, or refuse. See
/// the Postgres ledger's function of the same name.
async fn charge_rate(
    tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
    r: &NewReservation<'_>,
) -> Result<Option<LedgerRefusal>, sqlx::Error> {
    let (rpm, tpm, prompt) = (
        i64::from(r.limits.requests_per_minute),
        i64::from(r.limits.tokens_per_minute),
        i64::from(r.limits.prompt_tokens),
    );
    sqlx::query(&format!(
        "INSERT INTO rate_minutes (tenant_id, minute) VALUES (?1, {MINUTE}) ON CONFLICT DO NOTHING"
    ))
    .bind(r.tenant_id)
    .execute(&mut **tx)
    .await?;
    let counted: Option<i64> = sqlx::query_scalar(&format!(
        "UPDATE rate_minutes SET requests = requests + 1, tokens = tokens + ?2
          WHERE tenant_id = ?1 AND minute = {MINUTE}
            AND (?3 = 0 OR requests < ?3)
            AND (?4 = 0 OR tokens + ?2 <= ?4)
      RETURNING requests"
    ))
    .bind(r.tenant_id)
    .bind(prompt)
    .bind(rpm)
    .bind(tpm)
    .fetch_optional(&mut **tx)
    .await?;
    if counted.is_some() {
        return Ok(None);
    }
    let (requests, tokens): (i64, i64) = sqlx::query_as(&format!(
        "SELECT requests, tokens FROM rate_minutes WHERE tenant_id = ?1 AND minute = {MINUTE}"
    ))
    .bind(r.tenant_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(Some(if rpm > 0 && requests >= rpm {
        LedgerRefusal::RateLimited { done: requests as u32, limit: rpm as u32 }
    } else {
        LedgerRefusal::TokenRateLimited { done: tokens as u32, requested: prompt as u32, limit: tpm as u32 }
    }))
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
    // Released: nothing was executed, so an approval it used can be used
    // again. Lock order: reservation, hold, day row, rate counters.
    if c.outcome == Outcome::Released {
        sqlx::query("UPDATE holds SET state = 'approved', reservation_id = NULL WHERE reservation_id = ?1 AND state = 'consumed'")
            .bind(c.id.to_string())
            .execute(&mut *tx)
            .await?;
    }

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
    // Lock order, the same as admission's: reservation, then the day row
    // (above), then the rate counters (here). The reverse order deadlocks
    // against a concurrent admission of the same tenant.
    // Completion tokens are real usage: counted in the minute they settle.
    if c.completion_tokens > 0 {
        sqlx::query(&format!(
            "INSERT INTO rate_minutes (tenant_id, minute, tokens) VALUES (?1, {MINUTE}, ?2)
             ON CONFLICT (tenant_id, minute) DO UPDATE SET tokens = tokens + ?2"
        ))
        .bind(&tenant)
        .bind(i64::from(c.completion_tokens))
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
    live: Arc<DashMap<Uuid, ()>>,
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
                live.remove(&closing.id);
                pending.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

/// Renew the lease of every reservation this process holds, in one
/// statement, every third of the lease.
async fn run_renewer(pool: SqlitePool, live: Arc<DashMap<Uuid, ()>>, lease: Duration) {
    let mut ticker = tokio::time::interval((lease / 3).max(Duration::from_millis(200)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let ids: Vec<String> = live.iter().map(|e| e.key().to_string()).collect();
        if ids.is_empty() {
            continue;
        }
        let list = serde_json::to_string(&ids).unwrap_or_else(|_| "[]".into());
        if let Err(error) = sqlx::query(&format!(
            "UPDATE reservations SET lease_expires_at = {NOW} + ?2
              WHERE state = 'open' AND id IN (SELECT value FROM json_each(?1))"
        ))
        .bind(list)
        .bind(lease_secs(lease))
        .execute(&pool)
        .await
        {
            tracing::warn!(%error, reservations = ids.len(), "lease renewal failed; will retry");
        }
    }
}

async fn sweep(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    let swept = sqlx::query(&format!(
        "UPDATE reservations SET state = 'swept', delta_nano_usd = 0, closed_at = {NOW}
          WHERE state = 'open' AND lease_expires_at < {NOW}"
    ))
    .execute(pool)
    .await?;
    sqlx::query(&format!("DELETE FROM rate_minutes WHERE minute < {MINUTE} - 60"))
        .execute(pool)
        .await?;
    Ok(swept.rows_affected())
}

async fn insert_decision<'e, E>(ex: E, d: &Decision) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
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
    .execute(ex)
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

async fn run_sweeper(pool: SqlitePool, interval: Duration, decision_retention: Duration, upkeep: Arc<Upkeep>) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
    loop {
        ticker.tick().await;
        if let Err(error) = sqlx::query(&format!("DELETE FROM decisions WHERE created_at < {NOW} - ?1"))
            .bind(decision_retention.as_secs() as i64)
            .execute(&pool)
            .await
        {
            upkeep.sweep_failed();
            tracing::warn!(%error, "decision retention sweep failed; will retry");
        }
        if let Err(error) = expire_holds(&pool).await {
            upkeep.sweep_failed();
            tracing::warn!(%error, "hold expiry failed; will retry");
        }
        // Holds that can never change again go with the decisions about them.
        if let Err(error) = sqlx::query(&format!(
            "DELETE FROM holds WHERE state IN ('denied', 'expired', 'consumed') AND created_at < {NOW} - ?1"
        ))
        .bind(decision_retention.as_secs() as i64)
        .execute(&pool)
        .await
        {
            upkeep.sweep_failed();
            tracing::warn!(%error, "hold retention sweep failed; will retry");
        }
        match sweep(&pool).await {
            Ok(0) => {}
            Ok(n) => {
                upkeep.swept(n);
                tracing::warn!(
                    reservations = n,
                    "swept reservations whose lease lapsed; each keeps its full reservation as the bill"
                );
            }
            Err(error) => {
                upkeep.sweep_failed();
                tracing::warn!(%error, "ledger sweep failed; will retry");
            }
        }
    }
}

#[async_trait::async_trait]
impl Ledger for SqliteLedger {
    async fn try_reserve(&self, r: NewReservation<'_>) -> Result<u64, LedgerRefusal> {
        refuse_if_backlogged(self.pending.load(Ordering::Relaxed), self.max_pending_closings, &self.upkeep)?;
        // Deciding may be abandoned: running out drops the transaction before
        // its commit, which rolls it back and holds nothing. It keeps the
        // whole deadline, because under contention deciding is mostly queueing
        // for the tenant's day row, and cutting it short refuses requests that
        // would have been served.
        let started = Instant::now();
        let (tx, today) = match tokio::time::timeout(self.timeout, self.decide(&r)).await {
            Ok(Ok(Ok(decided))) => decided,
            Ok(Ok(Err(refusal))) => return Err(refusal),
            Ok(Err(error)) => return Err(unavailable("reserve", error)),
            Err(_) => return Err(unavailable("reserve", "timed out")),
        };
        let bucket = today.max(0) as u64;
        // The commit is the step that must not be cut short. It gets what
        // deciding left, but never less than a quarter of the deadline: an
        // overloaded ledger is slow to decide, and without the floor every
        // commit there would start with nothing left. So an admission can take
        // up to 1.25 times the deadline, and only under overload. One that
        // still overruns is refused like any timeout, but it is not abandoned:
        // if it lands, the reservation is released at once.
        let committing = self.timeout.saturating_sub(started.elapsed()).max(self.timeout / 4);
        match commit_within(tx.commit(), committing, self.release_if_late(&r, bucket)).await {
            CommitOutcome::Committed => {
                self.live.insert(r.id, ());
                Ok(bucket)
            }
            CommitOutcome::Failed(error) => Err(unavailable("reserve", error)),
            CommitOutcome::Late => Err(unavailable("reserve", "timed out committing")),
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

    fn enforces_limits(&self) -> bool {
        true
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

    fn upkeep(&self) -> UpkeepCounts {
        UpkeepCounts { pending_closings: self.pending.load(Ordering::Relaxed), ..self.upkeep.counts() }
    }

    async fn create_hold(&self, h: NewHold) -> Result<HoldRecord, LedgerRefusal> {
        bounded(self.timeout, "create hold", self.create_hold_tx(&h)).await?
    }

    async fn hold(&self, id: Uuid) -> Result<Option<HoldRecord>, LedgerRefusal> {
        bounded(self.timeout, "hold", select_hold(&self.pool, id)).await
    }

    async fn holds(&self, q: HoldQuery) -> Result<Vec<HoldRecord>, LedgerRefusal> {
        let sql = format!(
            "SELECT {HOLD_COLUMNS}, (expires_at <= {NOW}) AS lapsed FROM holds
              WHERE (?1 IS NULL OR tenant_id = ?1) AND (?2 IS NULL OR {HOLD_EFFECTIVE} = ?2)
           ORDER BY created_at DESC, id DESC
              LIMIT ?3"
        );
        let rows = sqlx::query(&sql)
            .bind(q.tenant_id.as_deref())
            .bind(q.state.map(HoldState::as_str))
            .bind(i64::from(q.limit))
            .fetch_all(&self.pool);
        let rows = bounded(self.timeout, "holds", rows).await?;
        Ok(rows.iter().filter_map(hold_from_row).collect())
    }

    async fn decide_hold(&self, d: HoldDecision) -> Result<HoldRecord, LedgerRefusal> {
        bounded(self.timeout, "decide hold", self.decide_hold_tx(&d)).await?
    }
}

/// Run one ledger operation under the deadline. A failure or a timeout is
/// `Unavailable`: the caller refuses rather than guesses.
async fn bounded<T>(
    timeout: Duration,
    op: &str,
    work: impl std::future::Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, LedgerRefusal> {
    match tokio::time::timeout(timeout, work).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(unavailable(op, error)),
        Err(_) => Err(unavailable(op, "timed out")),
    }
}

impl SqliteLedger {
    async fn create_hold_tx(&self, h: &NewHold) -> Result<Result<HoldRecord, LedgerRefusal>, sqlx::Error> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let pending: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM holds WHERE tenant_id = ?1 AND state = 'pending' AND expires_at > {NOW}"
        ))
        .bind(&h.tenant_id)
        .fetch_one(&mut *tx)
        .await?;
        if pending >= i64::from(h.max_pending) {
            return Ok(Err(LedgerRefusal::TooManyHolds { limit: h.max_pending }));
        }
        sqlx::query(&format!(
            "INSERT INTO holds (id, tenant_id, requested_by, endpoint, model, max_output_tokens, exposure_nano_usd,
                                reason, fingerprint, approval_valid_secs, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, {NOW}, {NOW} + ?11)"
        ))
        .bind(h.id.to_string())
        .bind(&h.tenant_id)
        .bind(&h.requested_by)
        .bind(&h.endpoint)
        .bind(&h.model)
        .bind(i64::from(h.max_output_tokens))
        .bind(to_i64(h.exposure_nano_usd))
        .bind(&h.reason)
        .bind(&h.fingerprint)
        .bind(lease_secs(h.approval_valid_for))
        .bind(lease_secs(h.expires_in))
        .execute(&mut *tx)
        .await?;
        let requested = Decision {
            request_id: h.id,
            tenant_id: h.tenant_id.clone(),
            key_id: h.requested_by.clone(),
            kind: DecisionKind::Hold,
            endpoint: h.endpoint.clone(),
            model: Some(h.model.clone()),
            code: holds::CODE_REQUESTED.to_string(),
            reason: String::new(),
        }
        .with_reason(&h.reason);
        insert_decision(&mut *tx, &requested).await?;
        let record = select_hold(&mut *tx, h.id)
            .await?
            .ok_or_else(|| sqlx::Error::RowNotFound)?;
        tx.commit().await?;
        Ok(Ok(record))
    }

    async fn decide_hold_tx(&self, d: &HoldDecision) -> Result<Result<HoldRecord, LedgerRefusal>, sqlx::Error> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let Some(current) = select_hold(&mut *tx, d.id).await? else {
            return Ok(Err(LedgerRefusal::Hold(HoldProblem::NotFound)));
        };
        let state = match holds::transition(&current, d) {
            Err(refusal) => return Ok(Err(refusal)),
            Ok(Transition::Unchanged) => return Ok(Ok(current)),
            Ok(Transition::To(state)) => state,
        };
        // An approval starts the approval's own clock; a denial keeps the
        // hold's expiry, which no longer matters.
        sqlx::query(&format!(
            "UPDATE holds SET state = ?2, decided_by = ?3, decided_at = {NOW}, note = ?4,
                    expires_at = CASE WHEN ?2 = 'approved' THEN {NOW} + approval_valid_secs ELSE expires_at END
              WHERE id = ?1"
        ))
        .bind(d.id.to_string())
        .bind(state.as_str())
        .bind(d.decided_by())
        .bind((!d.note.is_empty()).then_some(d.note.as_str()))
        .execute(&mut *tx)
        .await?;
        let decided = Decision {
            request_id: d.id,
            tenant_id: current.tenant_id.clone(),
            key_id: d.approver_key.clone(),
            kind: DecisionKind::Hold,
            endpoint: holds::DECIDE_ENDPOINT.to_string(),
            model: Some(current.model.clone()),
            code: d.code().to_string(),
            reason: String::new(),
        }
        .with_reason(&d.reason());
        insert_decision(&mut *tx, &decided).await?;
        let record = select_hold(&mut *tx, d.id)
            .await?
            .ok_or_else(|| sqlx::Error::RowNotFound)?;
        tx.commit().await?;
        Ok(Ok(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::ledger::{IdempotencyClaim, Outcome, SharedLimits};

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
        SqliteLedger::connect(options(file)).await.unwrap()
    }

    #[tokio::test]
    async fn a_connection_left_inside_a_transaction_is_closed_not_reused() {
        // What a deadline can leave behind: a transaction begun and
        // acknowledged, with nothing left to own it, because the future that
        // would have received it was dropped first. Forgetting the handle
        // makes the same state on purpose. The connection then goes back to
        // the pool holding SQLite's write lock. Reused, it stopped the whole
        // ledger: every admission, closing and sweep after it failed.
        let file = TempFile::new();
        let ledger = open(&file).await;
        {
            let mut conn = ledger.pool().acquire().await.unwrap();
            let tx = sqlx::Connection::begin_with(&mut *conn, "BEGIN IMMEDIATE").await.unwrap();
            std::mem::forget(tx);
        }
        // Released asynchronously; let the pool take it back.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let bucket = ledger.try_reserve(reservation("t", 100, 0)).await.expect("the ledger still admits");
        ledger.close(closing(Uuid::new_v4(), "t", bucket, 0, Outcome::Released));
        ledger.flush().await;
        assert_eq!(ledger.upkeep().stuck_connections_closed, 1);
        // Every connection the pool now holds can start a transaction.
        let mut held = Vec::new();
        for n in 0..4 {
            let mut conn = ledger.pool().acquire().await.unwrap();
            let begun = sqlx::Connection::begin(&mut *conn).await.map(drop);
            assert!(begun.is_ok(), "connection {n}: {begun:?}");
            held.push(conn);
        }
    }

    #[tokio::test]
    async fn admissions_are_refused_while_settlements_back_up() {
        // Closings carry money and are never dropped, so when the writer
        // falls behind, the bound is at the door: past the limit, admissions
        // are refused at once, without touching the database.
        let file = TempFile::new();
        let ledger = SqliteLedger::connect(SqliteOptions { max_pending_closings: 3, ..options(&file) })
            .await
            .unwrap();
        // Another process holds the write lock: no closing can be applied.
        let outside = SqlitePool::connect_with(SqliteConnectOptions::new().filename(&file.0)).await.unwrap();
        let lock = outside.begin_with("BEGIN IMMEDIATE").await.unwrap();
        for _ in 0..3 {
            ledger.close(closing(Uuid::new_v4(), "t", 0, 0, Outcome::Released));
        }

        let started = Instant::now();
        let refused = ledger.try_reserve(reservation("t", 100, 0)).await;
        assert!(
            matches!(refused, Err(LedgerRefusal::Unavailable(ref m)) if m.contains("settlements are waiting")),
            "{refused:?}"
        );
        assert!(started.elapsed() < Duration::from_millis(500), "refused at the door, not after waiting on the lock");
        let upkeep = ledger.upkeep();
        assert_eq!((upkeep.pending_closings, upkeep.backlog_refusals), (3, 1));

        lock.rollback().await.unwrap();
        ledger.flush().await;
        assert_eq!(ledger.upkeep().pending_closings, 0);
        ledger.try_reserve(reservation("t", 100, 0)).await.expect("admitted once the writer caught up");
    }

    fn options(file: &TempFile) -> SqliteOptions {
        SqliteOptions {
            path: file.0.clone(),
            timeout: Duration::from_secs(5),
            lease: Duration::from_secs(60),
            sweep_interval: Duration::from_secs(3_600),
            idempotency_retention: Duration::from_secs(86_400),
            sealer: Some(Arc::new(ResponseSealer::from_keys(KEYS).unwrap())),
            decision_retention: Duration::from_secs(30 * 86_400),
            max_pending_closings: 10_000,
        }
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
            hold: None,
            limits: crate::governance::ledger::SharedLimits::default(),
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
            completion_tokens: 0,
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
        // The dead process renewed nothing: its lease lapses.
        sqlx::query(&format!("UPDATE reservations SET lease_expires_at = {NOW} - 1 WHERE id = ?1"))
            .bind(id.to_string())
            .execute(reopened.pool())
            .await
            .unwrap();
        // The background sweeper also runs at boot and may get there first;
        // what matters is the end state, whoever swept it.
        assert!(reopened.sweep().await.unwrap() <= 1);
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
                "INSERT INTO reservations (id, tenant_id, day, reserved_nano_usd, prompt_nano_usd, created_at, lease_expires_at)
                 VALUES (?1, 't', ?2, 1000, 100, {NOW}, {NOW} + 3600)"
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
            "INSERT INTO reservations (id, tenant_id, day, reserved_nano_usd, prompt_nano_usd, created_at, lease_expires_at)
             VALUES (?1, 't', ?2, 1000, 100, {NOW}, {NOW} + 3600)"
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

    // -----------------------------------------------------------------------
    // Leases and shared limits
    // -----------------------------------------------------------------------

    async fn open_with_lease(file: &TempFile, lease: Duration) -> Arc<SqliteLedger> {
        SqliteLedger::connect(SqliteOptions {
            path: file.0.clone(),
            timeout: Duration::from_secs(5),
            lease,
            sweep_interval: Duration::from_secs(3_600),
            idempotency_retention: Duration::from_secs(86_400),
            sealer: None,
            decision_retention: Duration::from_secs(86_400),
            max_pending_closings: 10_000,
        })
        .await
        .unwrap()
    }

    fn limited<'a>(tenant: &'a str, limits: SharedLimits) -> NewReservation<'a> {
        NewReservation { limits, ..reservation(tenant, 100, 0) }
    }

    async fn state_of(ledger: &SqliteLedger, id: Uuid) -> String {
        sqlx::query_scalar("SELECT state FROM reservations WHERE id = ?1")
            .bind(id.to_string())
            .fetch_one(ledger.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_held_reservation_is_never_swept_while_its_holder_lives() {
        // A 1 s lease, held for three: the holder keeps renewing it, so the
        // sweeper never takes a reservation that is still being served.
        let file = TempFile::new();
        let ledger = open_with_lease(&file, Duration::from_secs(1)).await;
        let r = reservation("t", 1_000, 0);
        let bucket = ledger.try_reserve(r.clone()).await.unwrap();
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert_eq!(ledger.sweep().await.unwrap(), 0, "a live lease was swept");
        }
        assert_eq!(state_of(&ledger, r.id).await, "open");
        ledger.close(closing(r.id, "t", bucket, -700, Outcome::Settled));
        ledger.flush().await;
        assert_eq!(state_of(&ledger, r.id).await, "settled");
        assert_eq!(spent(&ledger, "t").await, 300, "billed its real usage, not the reservation");
    }

    #[tokio::test]
    async fn concurrency_is_one_cap_across_processes() {
        let file = TempFile::new();
        let (a, b) = (open(&file).await, open(&file).await);
        let caps = SharedLimits { max_concurrent: 3, ..Default::default() };
        let tasks: Vec<_> = (0..12)
            .map(|i| {
                let ledger = if i % 2 == 0 { Arc::clone(&a) } else { Arc::clone(&b) };
                tokio::spawn(async move { ledger.try_reserve(limited("t", caps)).await })
            })
            .collect();
        let results: Vec<_> = futures_util::future::join_all(tasks).await.into_iter().map(Result::unwrap).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 3);
        assert!(results
            .iter()
            .filter_map(|r| r.as_ref().err())
            .all(|e| *e == LedgerRefusal::ConcurrencyLimited { limit: 3 }));
    }

    #[tokio::test]
    async fn requests_per_minute_is_exact_across_processes_and_refusals_are_free() {
        let file = TempFile::new();
        let (a, b) = (open(&file).await, open(&file).await);
        let rate = SharedLimits { requests_per_minute: 5, ..Default::default() };
        // Far from a minute boundary, so all twenty land in one window.
        wait_for_mid_minute().await;
        let tasks: Vec<_> = (0..20)
            .map(|i| {
                let ledger = if i % 2 == 0 { Arc::clone(&a) } else { Arc::clone(&b) };
                tokio::spawn(async move { ledger.try_reserve(limited("t", rate)).await })
            })
            .collect();
        let results: Vec<_> = futures_util::future::join_all(tasks).await.into_iter().map(Result::unwrap).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 5);
        let counted: i64 = sqlx::query_scalar(&format!(
            "SELECT requests FROM rate_minutes WHERE tenant_id = 't' AND minute = {MINUTE}"
        ))
        .fetch_one(a.pool())
        .await
        .unwrap();
        assert_eq!(counted, 5, "the fifteen refusals consumed nothing");
    }

    #[tokio::test]
    async fn completion_tokens_count_against_the_minute_they_settle_in() {
        let file = TempFile::new();
        let ledger = open(&file).await;
        let tpm = |prompt| SharedLimits { tokens_per_minute: 100, prompt_tokens: prompt, ..Default::default() };
        wait_for_mid_minute().await;
        let first = limited("t", tpm(30));
        let bucket = ledger.try_reserve(first.clone()).await.unwrap();
        let mut done = closing(first.id, "t", bucket, 0, Outcome::Settled);
        done.completion_tokens = 60;
        ledger.close(done);
        ledger.flush().await;
        assert_eq!(
            ledger.try_reserve(limited("t", tpm(20))).await.unwrap_err(),
            LedgerRefusal::TokenRateLimited { done: 90, requested: 20, limit: 100 },
            "30 prompt + 60 completion leaves room for 10, not 20"
        );
        ledger.try_reserve(limited("t", tpm(10))).await.expect("exactly the room left");
    }

    #[tokio::test]
    async fn the_rate_window_is_an_aligned_minute() {
        // Fixed, aligned windows: a full minute does not limit the next one.
        // That is the documented boundary burst: a tenant can spend its
        // limit at 12:00:59 and again at 12:01:00.
        let file = TempFile::new();
        let ledger = open(&file).await;
        let rate = SharedLimits { requests_per_minute: 2, ..Default::default() };
        wait_for_mid_minute().await;
        sqlx::query(&format!("INSERT INTO rate_minutes (tenant_id, minute, requests) VALUES ('t', {MINUTE} - 1, 2)"))
            .execute(ledger.pool())
            .await
            .unwrap();
        ledger.try_reserve(limited("t", rate)).await.expect("last minute's full window is over");
        ledger.try_reserve(limited("t", rate)).await.expect("two in this minute");
        assert_eq!(
            ledger.try_reserve(limited("t", rate)).await.unwrap_err(),
            LedgerRefusal::RateLimited { done: 2, limit: 2 }
        );
    }

    /// Wait until at least 15 s remain in the current UTC minute, so a test
    /// that counts within one window cannot straddle two.
    async fn wait_for_mid_minute() {
        loop {
            let second = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                % 60;
            if second < 45 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn new_hold(tenant: &str, fingerprint: &[u8]) -> NewHold {
        NewHold {
            id: Uuid::new_v4(),
            tenant_id: tenant.into(),
            requested_by: "ak_app".into(),
            endpoint: "stream".into(),
            model: "m".into(),
            max_output_tokens: 10,
            exposure_nano_usd: 1_000,
            reason: "model `m` requires approval".into(),
            fingerprint: fingerprint.to_vec(),
            expires_in: Duration::from_secs(900),
            approval_valid_for: Duration::from_secs(300),
            max_pending: 20,
        }
    }

    fn verdict(id: Uuid, verdict: holds::Verdict) -> HoldDecision {
        HoldDecision {
            id,
            approver_tenant: "ops".into(),
            approver_key: "ak_approver".into(),
            verdict,
            note: String::new(),
        }
    }

    fn claim(id: Uuid, fingerprint: &[u8]) -> HoldClaim {
        HoldClaim {
            id,
            fingerprints: vec![fingerprint.to_vec()],
            model: "m".into(),
            exposure_nano_usd: 1_000,
            key_id: "ak_app".into(),
        }
    }

    async fn codes_for(ledger: &SqliteLedger, id: Uuid) -> Vec<(String, String)> {
        sqlx::query_as("SELECT code, reason FROM decisions WHERE request_id = ?1 ORDER BY id")
            .bind(id.to_string())
            .fetch_all(ledger.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_approval_is_used_once_and_not_by_a_refused_admission() {
        let file = TempFile::new();
        let ledger = open(&file).await;
        let hold = ledger.create_hold(new_hold("t", b"fp")).await.unwrap();
        assert_eq!(hold.state, HoldState::Pending);
        let early = NewReservation { hold: Some(claim(hold.id, b"fp")), ..reservation("t", 1_000, 0) };
        assert_eq!(ledger.try_reserve(early).await, Err(LedgerRefusal::Hold(HoldProblem::Pending)));
        ledger.decide_hold(verdict(hold.id, holds::Verdict::Approve)).await.unwrap();

        // Refused for budget: rolled back, approval and all.
        let poor = NewReservation { hold: Some(claim(hold.id, b"fp")), ..reservation("t", 1_000, 500) };
        assert!(matches!(ledger.try_reserve(poor).await, Err(LedgerRefusal::BudgetWouldBeExceeded { .. })));
        assert_eq!(ledger.hold(hold.id).await.unwrap().unwrap().state, HoldState::Approved);
        // Another request presenting it: refused, approval untouched.
        let other = NewReservation { hold: Some(claim(hold.id, b"other")), ..reservation("t", 1_000, 0) };
        assert_eq!(ledger.try_reserve(other).await, Err(LedgerRefusal::Hold(HoldProblem::Mismatch)));

        let used = NewReservation { hold: Some(claim(hold.id, b"fp")), ..reservation("t", 1_000, 0) };
        let used_id = used.id;
        ledger.try_reserve(used).await.unwrap();
        let after = ledger.hold(hold.id).await.unwrap().unwrap();
        assert_eq!(after.state, HoldState::Consumed);
        assert_eq!(after.reservation_id, Some(used_id));
        let again = NewReservation { hold: Some(claim(hold.id, b"fp")), ..reservation("t", 1_000, 0) };
        assert_eq!(ledger.try_reserve(again).await, Err(LedgerRefusal::Hold(HoldProblem::Consumed)));
        // Another tenant cannot even see it.
        let stranger = NewReservation { hold: Some(claim(hold.id, b"fp")), ..reservation("u", 1_000, 0) };
        assert_eq!(ledger.try_reserve(stranger).await, Err(LedgerRefusal::Hold(HoldProblem::NotFound)));

        let codes: Vec<String> = codes_for(&ledger, hold.id).await.into_iter().map(|(c, _)| c).collect();
        assert_eq!(codes, ["hold_requested", "hold_approved", "hold_consumed"]);
    }

    #[tokio::test]
    async fn a_released_request_gives_its_approval_back_and_a_settled_one_does_not() {
        let file = TempFile::new();
        let ledger = open(&file).await;
        let hold = ledger.create_hold(new_hold("t", b"fp")).await.unwrap();
        ledger.decide_hold(verdict(hold.id, holds::Verdict::Approve)).await.unwrap();

        let first = NewReservation { hold: Some(claim(hold.id, b"fp")), ..reservation("t", 1_000, 0) };
        let bucket = ledger.try_reserve(first.clone()).await.unwrap();
        ledger.close(closing(first.id, "t", bucket, -1_000, Outcome::Released));
        ledger.flush().await;
        assert_eq!(ledger.hold(hold.id).await.unwrap().unwrap().state, HoldState::Approved);

        let second = NewReservation { hold: Some(claim(hold.id, b"fp")), ..reservation("t", 1_000, 0) };
        let bucket = ledger.try_reserve(second.clone()).await.unwrap();
        ledger.close(closing(second.id, "t", bucket, 0, Outcome::Settled));
        ledger.flush().await;
        let after = ledger.hold(hold.id).await.unwrap().unwrap();
        assert_eq!(after.state, HoldState::Consumed, "executed: it stays used");
        assert_eq!(after.reservation_id, Some(second.id));
    }

    #[tokio::test]
    async fn lapsed_holds_are_expired_and_recorded_with_why() {
        let file = TempFile::new();
        let ledger = open(&file).await;
        let waiting = ledger.create_hold(new_hold("t", b"a")).await.unwrap();
        let approved = ledger.create_hold(new_hold("t", b"b")).await.unwrap();
        ledger.decide_hold(verdict(approved.id, holds::Verdict::Approve)).await.unwrap();
        let denied = ledger.create_hold(new_hold("t", b"c")).await.unwrap();
        ledger.decide_hold(verdict(denied.id, holds::Verdict::Deny)).await.unwrap();
        sqlx::query(&format!("UPDATE holds SET expires_at = {NOW} - 1"))
            .execute(ledger.pool())
            .await
            .unwrap();

        // Expired to every reader at once, before any sweep runs.
        assert_eq!(ledger.hold(waiting.id).await.unwrap().unwrap().state, HoldState::Expired);
        // The background sweeper's first tick fires at boot and may run
        // between the backdating above and this call, expiring some or all
        // of the holds itself. Between the two, each lapsed hold is expired
        // exactly once, whoever gets there first.
        assert!(ledger.expire_holds().await.unwrap() <= 2, "the denial is final and stays a denial");
        assert_eq!(ledger.hold(denied.id).await.unwrap().unwrap().state, HoldState::Denied);
        assert_eq!(ledger.hold(approved.id).await.unwrap().unwrap().state, HoldState::Expired);
        let expiries = |codes: Vec<(String, String)>| -> Vec<String> {
            codes.into_iter().filter(|(c, _)| c == "hold_expired").map(|(_, r)| r).collect()
        };
        assert_eq!(expiries(codes_for(&ledger, waiting.id).await), vec![holds::EXPIRED_UNDECIDED]);
        assert_eq!(expiries(codes_for(&ledger, approved.id).await), vec![holds::EXPIRED_UNUSED]);
        assert!(expiries(codes_for(&ledger, denied.id).await).is_empty(), "a denial never expires");
        assert_eq!(ledger.expire_holds().await.unwrap(), 0, "and only once");
        let late = ledger.decide_hold(verdict(waiting.id, holds::Verdict::Approve)).await;
        assert_eq!(late, Err(LedgerRefusal::HoldNotDecidable { state: HoldState::Expired }));
    }

    #[tokio::test]
    async fn the_pending_limit_holds_across_processes_sharing_the_file() {
        let file = TempFile::new();
        let (a, b) = (open(&file).await, open(&file).await);
        let limited = |fp: u8| NewHold { max_pending: 3, ..new_hold("t", &[fp]) };
        let attempts = futures_util::future::join_all(
            (0..10u8).map(|i| if i % 2 == 0 { a.create_hold(limited(i)) } else { b.create_hold(limited(i)) }),
        )
        .await;
        assert_eq!(attempts.iter().filter(|r| r.is_ok()).count(), 3);
        assert!(attempts.iter().filter_map(|r| r.as_ref().err()).all(|e| *e == LedgerRefusal::TooManyHolds { limit: 3 }));
        let listed = a
            .holds(HoldQuery { tenant_id: Some("t".into()), state: Some(HoldState::Pending), limit: 100 })
            .await
            .unwrap();
        assert_eq!(listed.len(), 3);
    }
}
