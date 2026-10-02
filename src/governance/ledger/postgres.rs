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
//! * **Rate and concurrency limits** are enforced in the same admission
//!   transaction: requests and tokens per aligned UTC minute with the same
//!   conditional-update pattern, and concurrency as the tenant's count of
//!   open reservations with a live lease. Every replica sees one set of books.
//!
//! Closings are queued to one writer task, because the gateway closes
//! reservations from `Drop`, which cannot wait on the network. The writer
//! applies them in order and retries until each is durable.
//!
//! **Leases.** Every reservation carries `lease_expires_at`. This process
//! renews the leases of every reservation it still holds, in one statement,
//! every third of the lease, until each reservation's closing is durable. A
//! crashed process renews nothing, so its leases lapse; the sweeper closes
//! reservations whose lease has lapsed as `swept`, billing the full
//! reservation, because after a crash the real usage is unknowable. A stream
//! that simply runs long keeps its lease and is never swept while live.

use std::{
    future::Future,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use dashmap::DashMap;
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use super::{
    Closing, CommitOutcome, Ledger, LedgerRefusal, NewReservation, Outcome, Upkeep, UpkeepCounts, commit_within,
    refuse_if_backlogged,
    release_clean,
    decisions::{DECISION_QUEUE, Decision, DecisionKind, DecisionQuery, DecisionRecord},
    holds::{self, HoldClaim, HoldDecision, HoldProblem, HoldQuery, HoldRecord, HoldState, NewHold, Transition},
    sealed::{ResponseSealer, binding},
};

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
    /// How long a reservation's lease lasts without renewal. The holder
    /// renews every third of it; after a crash it lapses within this.
    pub lease: Duration,
    pub sweep_interval: Duration,
    pub idempotency_retention: Duration,
    /// Seals answers kept for idempotent replay. Without it no answer is
    /// stored at all, and a repeated completion gets `duplicate_request`
    /// rather than a replay: nothing leaves the process in the clear.
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

#[derive(Debug)]
pub struct PostgresLedger {
    pool: PgPool,
    writer: mpsc::UnboundedSender<WriterMsg>,
    pending: Arc<AtomicU64>,
    timeout: Duration,
    retention: Duration,
    sealer: Option<Arc<ResponseSealer>>,
    sweeper: JoinHandle<()>,
    renewer: JoinHandle<()>,
    lease: Duration,
    /// Reservations this process holds, whose leases it renews.
    live: Arc<DashMap<uuid::Uuid, ()>>,
    decision_queue: mpsc::Sender<Decision>,
    decisions_dropped: Arc<AtomicU64>,
    upkeep: Arc<Upkeep>,
    max_pending_closings: u64,
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
        self.renewer.abort();
    }
}

/// Schema names are interpolated into DDL, so only plain identifiers pass.
fn valid_schema(schema: &str) -> bool {
    !schema.is_empty()
        && schema.len() <= 63
        && schema.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !schema.as_bytes()[0].is_ascii_digit()
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
        let upkeep = Arc::new(Upkeep::default());
        let pool = PgPoolOptions::new()
            .max_connections(max)
            .min_connections(max.min(2))
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
            .map_err(|e| format!("cannot connect to the ledger database: {e}"))?;
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", opts.schema))
            .execute(&pool)
            .await
            .map_err(|e| format!("cannot create ledger schema: {e}"))?;
        sqlx::migrate!("./migrations/postgres")
            .run(&pool)
            .await
            .map_err(|e| format!("ledger migration failed: {e}"))?;

        let (writer, inbox) = mpsc::unbounded_channel();
        let pending = Arc::new(AtomicU64::new(0));
        let live: Arc<DashMap<uuid::Uuid, ()>> = Arc::default();
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
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Closings accepted but not yet durable.
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

    /// A stored answer, if it opens for this exact row. Anything that does
    /// not verify is withheld: the caller gets `duplicate_request`.
    fn open_response(&self, tenant_id: &str, id: uuid::Uuid, stored: Option<String>) -> Option<String> {
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
    ) -> Result<Result<(sqlx::Transaction<'static, sqlx::Postgres>, i64), LedgerRefusal>, sqlx::Error> {
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
                // Released (nothing executed) or expired: free the key so this
                // attempt can take it.
                sqlx::query("UPDATE reservations SET idempotency_key = NULL WHERE id = $1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        // After the key, so a repeat of an executed request is answered as a
        // repeat; before the day row, keeping one lock order everywhere:
        // reservation, hold, day row, rate counters. A refusal anywhere below
        // rolls the consumption back with the charge.
        if let Some(claim) = &r.hold
            && let Some(problem) = consume_hold(&mut tx, r, claim).await?
        {
            return Ok(Err(LedgerRefusal::Hold(problem)));
        }

        sqlx::query("INSERT INTO spend_days (tenant_id, day) VALUES ($1, $2) ON CONFLICT DO NOTHING")
            .bind(r.tenant_id)
            .bind(today)
            .execute(&mut *tx)
            .await?;
        // Lock the tenant's day row before anything is counted: from here to
        // commit, this tenant's admissions on every replica go one at a time,
        // so the concurrency count and the rate counters cannot be raced.
        sqlx::query("SELECT spent_nano_usd FROM spend_days WHERE tenant_id = $1 AND day = $2 FOR UPDATE")
            .bind(r.tenant_id)
            .bind(today)
            .fetch_one(&mut *tx)
            .await?;
        if r.limits.max_concurrent > 0 {
            let live: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM reservations
                  WHERE tenant_id = $1 AND state = 'open' AND lease_expires_at > now()",
            )
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
                 (id, tenant_id, day, reserved_nano_usd, prompt_nano_usd, idempotency_key, fingerprint, lease_expires_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, now() + make_interval(secs => $8))",
        )
        .bind(r.id)
        .bind(r.tenant_id)
        .bind(today)
        .bind(amount)
        .bind(to_i64(r.prompt_nano_usd))
        .bind(r.idempotency.as_ref().map(|c| c.key))
        .bind(r.idempotency.as_ref().map(|c| c.fingerprint.clone()))
        .bind(self.lease.as_secs_f64())
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
        Ok(Ok((tx, today)))
    }
}

/// Mark an approved hold used by this reservation, or say why it cannot be.
/// The row lock makes two requests presenting one approval go one at a time,
/// on every replica: the second sees it consumed.
async fn consume_hold(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    r: &NewReservation<'_>,
    claim: &HoldClaim,
) -> Result<Option<HoldProblem>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT state, fingerprint, model, endpoint, exposure_nano_usd, (expires_at <= now()) AS lapsed
           FROM holds WHERE id = $1 AND tenant_id = $2
            FOR UPDATE",
    )
    .bind(claim.id)
    .bind(r.tenant_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Ok(Some(HoldProblem::NotFound));
    };
    let state = HoldState::parse(&row.get::<String, _>("state"))
        .unwrap_or(HoldState::Expired)
        .effective(row.get("lapsed"));
    let fingerprint: Vec<u8> = row.get("fingerprint");
    let model: String = row.get("model");
    let approved: i64 = row.get("exposure_nano_usd");
    if let Err(problem) = claim.check(state, &fingerprint, &model, approved.max(0) as u64) {
        return Ok(Some(problem));
    }
    sqlx::query("UPDATE holds SET state = 'consumed', reservation_id = $2 WHERE id = $1")
        .bind(claim.id)
        .bind(r.id)
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
    reason, state, extract(epoch from created_at)::bigint AS created_at,
    extract(epoch from expires_at)::bigint AS expires_at, decided_by,
    extract(epoch from decided_at)::bigint AS decided_at, note, reservation_id,
    (expires_at <= now()) AS lapsed";

/// A hold's state as callers see it, in SQL.
const HOLD_EFFECTIVE: &str =
    "(CASE WHEN state IN ('pending', 'approved') AND expires_at <= now() THEN 'expired' ELSE state END)";

fn hold_from_row(row: &sqlx::postgres::PgRow) -> Option<HoldRecord> {
    Some(HoldRecord {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        requested_by: row.get("requested_by"),
        endpoint: row.get("endpoint"),
        model: row.get("model"),
        max_output_tokens: row.get::<i64, _>("max_output_tokens").clamp(0, i64::from(u32::MAX)) as u32,
        exposure_nano_usd: row.get::<i64, _>("exposure_nano_usd").max(0) as u64,
        reason: row.get("reason"),
        state: HoldState::parse(&row.get::<String, _>("state"))?.effective(row.get("lapsed")),
        created_at: row.get("created_at"),
        expires_at: row.get("expires_at"),
        decided_by: row.get("decided_by"),
        decided_at: row.get("decided_at"),
        note: row.get("note"),
        reservation_id: row.get("reservation_id"),
    })
}

async fn select_hold<'e, E>(ex: E, id: uuid::Uuid, for_update: bool) -> Result<Option<HoldRecord>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let lock = if for_update { " FOR UPDATE" } else { "" };
    let row = sqlx::query(&format!("SELECT {HOLD_COLUMNS} FROM holds WHERE id = $1{lock}"))
        .bind(id)
        .fetch_optional(ex)
        .await?;
    Ok(row.as_ref().and_then(hold_from_row))
}

/// Mark every hold whose time ran out as expired, and record each, in one
/// statement.
async fn expire_holds(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let expired = sqlx::query(
        "WITH lapsed AS (
             SELECT id, state FROM holds
              WHERE state IN ('pending', 'approved') AND expires_at <= now()
                FOR UPDATE SKIP LOCKED
         ), marked AS (
             UPDATE holds SET state = 'expired' FROM lapsed WHERE holds.id = lapsed.id
          RETURNING holds.id, holds.tenant_id, holds.requested_by, holds.endpoint, holds.model, lapsed.state AS was
         )
         INSERT INTO decisions (request_id, tenant_id, key_id, kind, endpoint, model, code, reason)
         SELECT id, tenant_id, requested_by, 'hold', endpoint, model, $1,
                CASE was WHEN 'pending' THEN $2 ELSE $3 END
           FROM marked",
    )
    .bind(holds::CODE_EXPIRED)
    .bind(holds::EXPIRED_UNDECIDED)
    .bind(holds::EXPIRED_UNUSED)
    .execute(pool)
    .await?;
    Ok(expired.rows_affected())
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

impl PostgresLedger {
    async fn begin_bounded(&self) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!("SET LOCAL statement_timeout = {}", self.timeout.as_millis().max(1)))
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }

    async fn create_hold_tx(&self, h: &NewHold) -> Result<Result<HoldRecord, LedgerRefusal>, sqlx::Error> {
        let mut tx = self.begin_bounded().await?;
        // One tenant's hold requests go one at a time, on every replica, so
        // the pending count cannot be raced past its limit.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('holds:' || $1, 0))")
            .bind(&h.tenant_id)
            .execute(&mut *tx)
            .await?;
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM holds WHERE tenant_id = $1 AND state = 'pending' AND expires_at > now()",
        )
        .bind(&h.tenant_id)
        .fetch_one(&mut *tx)
        .await?;
        if pending >= i64::from(h.max_pending) {
            return Ok(Err(LedgerRefusal::TooManyHolds { limit: h.max_pending }));
        }
        sqlx::query(
            "INSERT INTO holds (id, tenant_id, requested_by, endpoint, model, max_output_tokens, exposure_nano_usd,
                                reason, fingerprint, approval_valid_secs, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now() + make_interval(secs => $11))",
        )
        .bind(h.id)
        .bind(&h.tenant_id)
        .bind(&h.requested_by)
        .bind(&h.endpoint)
        .bind(&h.model)
        .bind(i64::from(h.max_output_tokens))
        .bind(to_i64(h.exposure_nano_usd))
        .bind(&h.reason)
        .bind(&h.fingerprint)
        .bind(h.approval_valid_for.as_secs().max(1) as i64)
        .bind(h.expires_in.as_secs_f64().max(1.0))
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
        let record = select_hold(&mut *tx, h.id, false)
            .await?
            .ok_or(sqlx::Error::RowNotFound)?;
        tx.commit().await?;
        Ok(Ok(record))
    }

    async fn decide_hold_tx(&self, d: &HoldDecision) -> Result<Result<HoldRecord, LedgerRefusal>, sqlx::Error> {
        let mut tx = self.begin_bounded().await?;
        let Some(current) = select_hold(&mut *tx, d.id, true).await? else {
            return Ok(Err(LedgerRefusal::Hold(HoldProblem::NotFound)));
        };
        let state = match holds::transition(&current, d) {
            Err(refusal) => return Ok(Err(refusal)),
            Ok(Transition::Unchanged) => return Ok(Ok(current)),
            Ok(Transition::To(state)) => state,
        };
        // An approval starts the approval's own clock; a denial keeps the
        // hold's expiry, which no longer matters.
        sqlx::query(
            "UPDATE holds SET state = $2, decided_by = $3, decided_at = now(), note = $4,
                    expires_at = CASE WHEN $2 = 'approved'
                                      THEN now() + make_interval(secs => approval_valid_secs)
                                      ELSE expires_at END
              WHERE id = $1",
        )
        .bind(d.id)
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
        let record = select_hold(&mut *tx, d.id, false)
            .await?
            .ok_or(sqlx::Error::RowNotFound)?;
        tx.commit().await?;
        Ok(Ok(record))
    }
}

/// Minutes since the Unix epoch, by the database clock: the rate window.
const MINUTE: &str = "(floor(extract(epoch from now()) / 60))::bigint";

/// Count this admission against the tenant's aligned minute, or refuse. The
/// whole check is one conditional update, so it cannot be raced; a refusal
/// rolls back with the transaction and consumes nothing.
async fn charge_rate(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    r: &NewReservation<'_>,
) -> Result<Option<LedgerRefusal>, sqlx::Error> {
    let (rpm, tpm, prompt) = (
        i64::from(r.limits.requests_per_minute),
        i64::from(r.limits.tokens_per_minute),
        i64::from(r.limits.prompt_tokens),
    );
    sqlx::query(&format!(
        "INSERT INTO rate_minutes (tenant_id, minute) VALUES ($1, {MINUTE}) ON CONFLICT DO NOTHING"
    ))
    .bind(r.tenant_id)
    .execute(&mut **tx)
    .await?;
    let counted: Option<i64> = sqlx::query_scalar(&format!(
        "UPDATE rate_minutes SET requests = requests + 1, tokens = tokens + $2
          WHERE tenant_id = $1 AND minute = {MINUTE}
            AND ($3 = 0 OR requests < $3)
            AND ($4 = 0 OR tokens + $2 <= $4)
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
        "SELECT requests, tokens FROM rate_minutes WHERE tenant_id = $1 AND minute = {MINUTE}"
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

async fn apply_close(pool: &PgPool, c: &Closing, sealed_response: Option<&str>) -> Result<(), sqlx::Error> {
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
    .bind(sealed_response)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = closed else {
        // Already closed (a duplicate, or swept). Nothing to apply.
        return tx.commit().await;
    };
    let tenant: String = row.get("tenant_id");
    let day: i64 = row.get("day");
    let today: i64 = sqlx::query_scalar(&format!("SELECT {TODAY}")).fetch_one(&mut *tx).await?;
    // Released: nothing was executed, so an approval it used can be used
    // again. Lock order, as in admission: reservation, hold, day row, rate
    // counters.
    if c.outcome == Outcome::Released {
        sqlx::query("UPDATE holds SET state = 'approved', reservation_id = NULL WHERE reservation_id = $1 AND state = 'consumed'")
            .bind(c.id)
            .execute(&mut *tx)
            .await?;
    }

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
    // Lock order, the same as admission's: reservation, then the day row
    // (above), then the rate counters (here). The reverse order deadlocks
    // against a concurrent admission of the same tenant.
    // Completion tokens are real usage: counted in the minute they settle.
    if c.completion_tokens > 0 {
        sqlx::query(&format!(
            "INSERT INTO rate_minutes (tenant_id, minute, tokens) VALUES ($1, {MINUTE}, $2)
             ON CONFLICT (tenant_id, minute) DO UPDATE SET tokens = rate_minutes.tokens + $2"
        ))
        .bind(&tenant)
        .bind(i64::from(c.completion_tokens))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}

/// Applies closings one at a time, in the order they were made, retrying each
/// until it is durable. Order matters only within a reservation, and each
/// reservation is closed exactly once, so a strict queue is simple and safe.
async fn run_writer(
    pool: PgPool,
    mut inbox: mpsc::UnboundedReceiver<WriterMsg>,
    pending: Arc<AtomicU64>,
    sealer: Option<Arc<ResponseSealer>>,
    live: Arc<DashMap<uuid::Uuid, ()>>,
) {
    while let Some(msg) = inbox.recv().await {
        match msg {
            WriterMsg::Flush(done) => {
                let _ = done.send(());
            }
            WriterMsg::Close(closing) => {
                // Sealed once, before any retry, so a retried write stores
                // the same bytes. No sealer: the answer is not stored.
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
                // Renewal stops only now that the closing is durable: a
                // backlog must not let a finished reservation's lease lapse
                // and be swept before its real settlement lands.
                live.remove(&closing.id);
                pending.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

/// Renew the lease of every reservation this process holds, in one
/// statement, every third of the lease.
async fn run_renewer(pool: PgPool, live: Arc<DashMap<uuid::Uuid, ()>>, lease: Duration) {
    let mut ticker = tokio::time::interval((lease / 3).max(Duration::from_millis(200)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let ids: Vec<uuid::Uuid> = live.iter().map(|e| *e.key()).collect();
        if ids.is_empty() {
            continue;
        }
        if let Err(error) = sqlx::query(
            "UPDATE reservations SET lease_expires_at = now() + make_interval(secs => $2)
              WHERE id = ANY($1) AND state = 'open'",
        )
        .bind(&ids)
        .bind(lease.as_secs_f64())
        .execute(&pool)
        .await
        {
            tracing::warn!(%error, reservations = ids.len(), "lease renewal failed; will retry");
        }
    }
}

async fn sweep(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let swept = sqlx::query(
        "UPDATE reservations SET state = 'swept', delta_nano_usd = 0, closed_at = now()
          WHERE state = 'open' AND lease_expires_at < now()",
    )
    .execute(pool)
    .await?;
    // Rate windows older than an hour can never matter again.
    sqlx::query(&format!("DELETE FROM rate_minutes WHERE minute < {MINUTE} - 60"))
        .execute(pool)
        .await?;
    Ok(swept.rows_affected())
}

async fn insert_decision<'e, E>(ex: E, d: &Decision) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(
        "INSERT INTO decisions (request_id, tenant_id, key_id, kind, endpoint, model, code, reason)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(d.request_id)
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
async fn run_decision_writer(pool: PgPool, mut inbox: mpsc::Receiver<Decision>, dropped: Arc<AtomicU64>) {
    while let Some(decision) = inbox.recv().await {
        if let Err(error) = insert_decision(&pool, &decision).await {
            dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, "a decision record was dropped");
        }
    }
}

async fn run_sweeper(pool: PgPool, interval: Duration, decision_retention: Duration, upkeep: Arc<Upkeep>) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
    loop {
        ticker.tick().await;
        if let Err(error) = sqlx::query("DELETE FROM decisions WHERE created_at < now() - make_interval(secs => $1)")
            .bind(decision_retention.as_secs_f64())
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
        if let Err(error) = sqlx::query(
            "DELETE FROM holds WHERE state IN ('denied', 'expired', 'consumed')
                AND created_at < now() - make_interval(secs => $1)",
        )
        .bind(decision_retention.as_secs_f64())
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
impl Ledger for PostgresLedger {
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
            "SELECT request_id, tenant_id, key_id, kind, endpoint, model, code, reason,
                    extract(epoch from created_at)::bigint AS at
               FROM decisions
              WHERE ($1::text IS NULL OR tenant_id = $1) AND created_at >= to_timestamp($2)
           ORDER BY created_at DESC, id DESC
              LIMIT $3",
        )
        .bind(q.tenant_id.as_deref())
        .bind(q.since as f64)
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
                        request_id: row.get("request_id"),
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

    async fn hold(&self, id: uuid::Uuid) -> Result<Option<HoldRecord>, LedgerRefusal> {
        bounded(self.timeout, "hold", select_hold(&self.pool, id, false)).await
    }

    async fn holds(&self, q: HoldQuery) -> Result<Vec<HoldRecord>, LedgerRefusal> {
        let sql = format!(
            "SELECT {HOLD_COLUMNS} FROM holds
              WHERE ($1::text IS NULL OR tenant_id = $1) AND ($2::text IS NULL OR {HOLD_EFFECTIVE} = $2)
           ORDER BY created_at DESC, id DESC
              LIMIT $3"
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
