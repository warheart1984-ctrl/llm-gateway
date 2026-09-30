//! The shared, durable ledger on Postgres.
//!
//! Every money operation is one transaction around one conditional statement,
//! so the database, not any gateway process, decides:
//!
//! * **Admission** charges the tenant-day row with
//!   `UPDATE … WHERE spent < budget AND spent + amount <= budget`. The row lock
//!   serializes admissions for a tenant-day across every replica: two
//!   gateways cannot both see the same headroom.
//! * **Closing** is `UPDATE reservations … WHERE id = $1 AND state = 'open'`.
//!   A second closing for the same reservation matches no row and changes
//!   nothing, even from another process.
//! * **Idempotency** keys are claimed in the admission transaction, under a
//!   unique index, so a key and its charge commit together or not at all.
//!
//! Closings are queued to one writer task, because the gateway closes
//! reservations from `Drop`, which cannot wait on the network. The writer
//! applies them in order and retries until each is durable. A closing lost to
//! a crash leaves its reservation `open`; the sweeper closes such rows as
//! `swept` after `sweep_after`, billing the full reservation, because after a
//! crash the real usage is unknowable.

use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use super::{Closing, Ledger, LedgerRefusal, NewReservation};

/// Days since the Unix epoch, by the database clock. One definition, used by
/// every statement, so "today" means the same thing on every replica.
const TODAY: &str = "(floor(extract(epoch from now()) / 86400))::bigint";

#[derive(Debug, Clone)]
pub struct PostgresOptions {
    /// Connection URL. Comes from the environment; never logged.
    pub url: String,
    /// Schema the tables live in. Created if missing.
    pub schema: String,
    pub max_connections: u32,
    /// Bound on every admission and snapshot, including waiting for a
    /// connection. A slow database refuses requests rather than hanging them.
    pub timeout: Duration,
    /// An `open` reservation older than this is treated as orphaned by a
    /// crash. Must comfortably exceed the longest request.
    pub sweep_after: Duration,
    pub sweep_interval: Duration,
    pub idempotency_retention: Duration,
}

enum WriterMsg {
    Close(Closing),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug)]
pub struct PostgresLedger {
    pool: PgPool,
    writer: mpsc::UnboundedSender<WriterMsg>,
    pending: Arc<AtomicU64>,
    timeout: Duration,
    retention: Duration,
    sweeper: JoinHandle<()>,
}

impl std::fmt::Debug for WriterMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterMsg::Close(c) => f.debug_tuple("Close").field(&c.id).finish(),
            WriterMsg::Flush(_) => f.write_str("Flush"),
        }
    }
}

impl Drop for PostgresLedger {
    fn drop(&mut self) {
        self.sweeper.abort();
    }
}

/// Schema names are interpolated into DDL, so only plain identifiers pass.
fn valid_schema(schema: &str) -> bool {
    !schema.is_empty()
        && schema.len() <= 63
        && schema.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !schema.as_bytes()[0].is_ascii_digit()
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
        .and_then(|e| e.code())
        .is_some_and(|code| code == "23505")
}

impl PostgresLedger {
    /// Connect, create the schema, run migrations, and start the writer and
    /// the sweeper. Errors describe the failure without echoing the URL.
    pub async fn connect(opts: PostgresOptions) -> Result<Arc<Self>, String> {
        if !valid_schema(&opts.schema) {
            return Err(format!(
                "ledger schema `{}` must be lowercase letters, digits and underscores",
                opts.schema
            ));
        }
        let connect = PgConnectOptions::from_str(&opts.url)
            .map_err(|_| "the ledger database URL is not a valid postgres:// URL".to_string())?
            .options([("search_path", opts.schema.as_str())]);
        // Connection setup (TCP, TLS, auth) gets its own generous bound; the
        // per-request deadline is enforced around each whole operation
        // instead, so a slow handshake cannot stop the gateway starting. A
        // few connections are kept warm so no request pays for setup.
        let max = opts.max_connections.max(1);
        let pool = PgPoolOptions::new()
            .max_connections(max)
            .min_connections(max.min(2))
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(connect)
            .await
            .map_err(|e| format!("cannot connect to the ledger database: {e}"))?;
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", opts.schema))
            .execute(&pool)
            .await
            .map_err(|e| format!("cannot create ledger schema: {e}"))?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| format!("ledger migration failed: {e}"))?;

        let (writer, inbox) = mpsc::unbounded_channel();
        let pending = Arc::new(AtomicU64::new(0));
        tokio::spawn(run_writer(pool.clone(), inbox, Arc::clone(&pending)));
        let sweeper = tokio::spawn(run_sweeper(pool.clone(), opts.sweep_after, opts.sweep_interval));

        Ok(Arc::new(Self {
            pool,
            writer,
            pending,
            timeout: opts.timeout,
            retention: opts.idempotency_retention,
            sweeper,
        }))
    }

    /// The connection pool, for operational queries and tests.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Closings accepted but not yet durable.
    pub fn pending_closings(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }

    /// Close every reservation left `open` for longer than `older_than` as
    /// `swept`, keeping its full reservation as the bill. Returns how many.
    pub async fn sweep(&self, older_than: Duration) -> Result<u64, String> {
        sweep(&self.pool, older_than).await.map_err(|e| e.to_string())
    }

    async fn reserve_tx(&self, r: &NewReservation<'_>) -> Result<Result<u64, LedgerRefusal>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!(
            "SET LOCAL statement_timeout = {}",
            self.timeout.as_millis().max(1)
        ))
        .execute(&mut *tx)
        .await?;
        let today: i64 = sqlx::query_scalar(&format!("SELECT {TODAY}")).fetch_one(&mut *tx).await?;

        if let Some(claim) = &r.idempotency {
            let held = sqlx::query(
                "SELECT id, state, fingerprint, reserved_nano_usd, delta_nano_usd, response,
                        (closed_at IS NOT NULL AND closed_at < now() - make_interval(secs => $3)) AS expired
                   FROM reservations
                  WHERE tenant_id = $1 AND idempotency_key = $2
                    FOR UPDATE",
            )
            .bind(r.tenant_id)
            .bind(claim.key)
            .bind(self.retention.as_secs_f64())
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(row) = held {
                let id: uuid::Uuid = row.get("id");
                let expired: bool = row.get("expired");
                if !expired {
                    let fingerprint: Option<Vec<u8>> = row.get("fingerprint");
                    if fingerprint.as_deref() != Some(&claim.fingerprint[..]) {
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
                                response: row.get("response"),
                            }));
                        }
                    }
                }
                // Released (nothing executed) or expired: free the key so this
                // attempt can take it.
                sqlx::query("UPDATE reservations SET idempotency_key = NULL WHERE id = $1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
            }
        }

        sqlx::query("INSERT INTO spend_days (tenant_id, day) VALUES ($1, $2) ON CONFLICT DO NOTHING")
            .bind(r.tenant_id)
            .bind(today)
            .execute(&mut *tx)
            .await?;
        let amount = to_i64(r.amount_nano_usd);
        let budget = to_i64(r.budget_nano_usd);
        // The whole budget rule in one statement. `spent < budget` as well as
        // `spent + amount <= budget`: the short form admits a zero-cost
        // request when a tenant has spent exactly its budget.
        let charged: Option<i64> = sqlx::query_scalar(
            "UPDATE spend_days SET spent_nano_usd = spent_nano_usd + $3
              WHERE tenant_id = $1 AND day = $2
                AND ($4 = 0 OR (spent_nano_usd < $4 AND spent_nano_usd + $3 <= $4))
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
                sqlx::query_scalar("SELECT spent_nano_usd FROM spend_days WHERE tenant_id = $1 AND day = $2")
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

        let inserted = sqlx::query(
            "INSERT INTO reservations
                 (id, tenant_id, day, reserved_nano_usd, prompt_nano_usd, idempotency_key, fingerprint)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(r.id)
        .bind(r.tenant_id)
        .bind(today)
        .bind(amount)
        .bind(to_i64(r.prompt_nano_usd))
        .bind(r.idempotency.as_ref().map(|c| c.key))
        .bind(r.idempotency.as_ref().map(|c| c.fingerprint.to_vec()))
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_) => {}
            // Two first attempts with one key raced; the other one committed
            // its claim first. Dropping `tx` rolls this charge back.
            Err(e) if is_unique_violation(&e) => {
                return Ok(Err(LedgerRefusal::InProgress { original: None }));
            }
            Err(e) => return Err(e),
        }
        tx.commit().await?;
        Ok(Ok(today as u64))
    }
}

async fn apply_close(pool: &PgPool, c: &Closing) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let delta = clamp_delta(c.delta_nano_usd);
    let closed = sqlx::query(
        "UPDATE reservations
            SET state = $2, delta_nano_usd = $3, response = $4, closed_at = now()
          WHERE id = $1 AND state = 'open'
      RETURNING tenant_id, day",
    )
    .bind(c.id)
    .bind(c.outcome.as_str())
    .bind(delta)
    .bind(c.response.as_deref())
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = closed else {
        // Already closed (a duplicate, or swept). Nothing to apply.
        return tx.commit().await;
    };
    let tenant: String = row.get("tenant_id");
    let day: i64 = row.get("day");
    let today: i64 = sqlx::query_scalar(&format!("SELECT {TODAY}")).fetch_one(&mut *tx).await?;

    // The rollover rule, identical to the memory backend: a refund reaches
    // only the day it was reserved in, and is dropped once that day has
    // closed; an overage is real spend and is charged to today.
    if delta < 0 && day == today {
        sqlx::query(
            "UPDATE spend_days SET spent_nano_usd = GREATEST(spent_nano_usd + $3, 0)
              WHERE tenant_id = $1 AND day = $2",
        )
        .bind(&tenant)
        .bind(day)
        .bind(delta)
        .execute(&mut *tx)
        .await?;
    } else if delta > 0 {
        sqlx::query("INSERT INTO spend_days (tenant_id, day) VALUES ($1, $2) ON CONFLICT DO NOTHING")
            .bind(&tenant)
            .bind(today)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE spend_days SET spent_nano_usd = spent_nano_usd + $3 WHERE tenant_id = $1 AND day = $2")
            .bind(&tenant)
            .bind(today)
            .bind(delta)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await
}

/// Applies closings one at a time, in the order they were made, retrying each
/// until it is durable. Order matters only within a reservation, and each
/// reservation is closed exactly once, so a strict queue is simple and safe.
async fn run_writer(pool: PgPool, mut inbox: mpsc::UnboundedReceiver<WriterMsg>, pending: Arc<AtomicU64>) {
    while let Some(msg) = inbox.recv().await {
        match msg {
            WriterMsg::Flush(done) => {
                let _ = done.send(());
            }
            WriterMsg::Close(closing) => {
                let mut backoff = Duration::from_millis(100);
                loop {
                    match apply_close(&pool, &closing).await {
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

async fn sweep(pool: &PgPool, older_than: Duration) -> Result<u64, sqlx::Error> {
    let swept = sqlx::query(
        "UPDATE reservations SET state = 'swept', delta_nano_usd = 0, closed_at = now()
          WHERE state = 'open' AND created_at < now() - make_interval(secs => $1)",
    )
    .bind(older_than.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(swept.rows_affected())
}

async fn run_sweeper(pool: PgPool, sweep_after: Duration, interval: Duration) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
    loop {
        ticker.tick().await;
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
impl Ledger for PostgresLedger {
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
                 (SELECT spent_nano_usd FROM spend_days WHERE tenant_id = $1 AND day = {TODAY}), 0)"
        );
        let query = sqlx::query_as::<_, (i64, i64)>(&sql)
        .bind(tenant_id)
        .fetch_one(&self.pool);
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
        let ping = sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&self.pool);
        matches!(tokio::time::timeout(self.timeout, ping).await, Ok(Ok(1)))
    }

    fn backend(&self) -> &'static str {
        "postgres"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plain_schema_names_reach_ddl() {
        assert!(valid_schema("public"));
        assert!(valid_schema("gw_test_01"));
        assert!(!valid_schema(""));
        assert!(!valid_schema("Public"));
        assert!(!valid_schema("a;drop table x"));
        assert!(!valid_schema("\"quoted\""));
        assert!(!valid_schema("1starts_with_digit"));
    }
}
