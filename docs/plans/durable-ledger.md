# Plan: durable spend ledger (Postgres)

**Branch:** `durable-ledger`, stacked on `hardening-pass` (PR #1). Rebase onto
`master` once #1 merges.
**Builder:** OpenCode. **Debugger/reviewer:** Claude Code, after handoff.
**Goal:** a daily budget that survives restarts and holds across replicas,
without changing any billing rule that exists today.

Read this whole file before writing code. Sections 2 and 3 are the
contract. Everything else is guidance.

---

## 1. Why, and what is out of scope

Today every budget lives in `SpendLedger` in
`src/governance/limits.rs`, in process memory. A restart resets daily
spend, and N replicas allow N times the budget. See "State and restarts" in
`README.md`.

**In scope:** the spend ledger only: reservations, settlements and daily
spend per tenant.

**Out of scope (do not touch):**
- RPM/TPM windows and concurrency counters. They stay in memory and
  per-replica. Say so in the README.
- Provider failover, new providers, and any change to the SSE wire format.
- The billing rules. The Postgres backend must bill exactly what the memory
  backend bills. The conformance suite in Phase 3 enforces this.

---

## 2. Invariants the new backend must preserve

These are already tested against the memory backend. The same tests must
pass against Postgres.

1. **Atomic check-and-reserve.** A request is refused if `spent >= budget`
   (`BudgetExhausted`), or if `amount > budget - spent`
   (`BudgetWouldBeExceeded`). `budget == 0` means no ceiling, but spend is
   still recorded.
   - ⚠️ The SQL condition is `spent < budget AND spent + amount <= budget`,
     **not** just `spent + amount <= budget`. The short form wrongly admits a
     zero-cost request when `spent == budget`.
2. **The first closing call decides the bill.** `settle`, `abandon`,
   `release`, `commit_reserved` and `Drop` are each applied at most once per
   reservation. Every later call is a ledger no-op.
3. **Rollover rule.** A correction is aimed at the reservation's own day
   (`bucket`), and applied like this:
   - The reservation's day is still today: apply the correction as is.
   - The day has rolled over and the correction is a refund
     (`delta < 0`): **drop it**.
   - The day has rolled over and the correction is an overage
     (`delta > 0`): **charge it to today**.

   See `SpendLedger::correct`. Keep this rule identical in both backends,
   even though Postgres could technically go back and correct the old day.
4. **Clamp at zero.** Spend never goes negative.
5. **Cost tracking off** (`cost_tracking_enabled = false`) means nothing is
   written to the ledger at all.

Existing tests that must stay green **unchanged in intent**:
- In `src/governance/limits.rs`: every test in `mod tests`, in particular
  `the_first_closing_call_decides_the_bill_in_every_order`,
  `concurrent_admit_and_close_leave_the_ledger_exact`,
  `concurrent_reservations_near_the_limit_admit_exactly_what_fits`,
  `a_refund_after_rollover_never_reaches_the_new_day`, and
  `stale_refunds_racing_new_reservations_cannot_erase_todays_spend`.
- In `tests/integration.rs`: all of them.

You may change how these tests call the API, for example making them
`async` or adding `flush()`. You may not weaken an assertion.

---

## 3. Design

### 3.1 The seam: a `Ledger` trait

Extract the ledger behind a trait in a new file,
`src/governance/ledger/mod.rs`:

```rust
#[async_trait::async_trait]
pub trait Ledger: Send + Sync + std::fmt::Debug {
    /// Atomic check-and-reserve. Returns the day bucket charged.
    async fn try_reserve(&self, r: NewReservation<'_>) -> Result<u64, LedgerError>;

    /// Close a reservation. MUST be synchronous: it is called from `Drop`.
    /// The memory backend applies it immediately. The Postgres backend
    /// enqueues it for the settlement writer (3.3).
    fn close(&self, c: Closing);

    /// (bucket, spent) for today.
    async fn snapshot(&self, tenant_id: &str, now: SystemTime) -> Result<(u64, u64), LedgerError>;

    /// Wait until every `close` accepted so far is durable. Used by tests
    /// and by graceful shutdown. A no-op for the memory backend.
    async fn flush(&self);
}

pub struct NewReservation<'a> {
    pub id: uuid::Uuid,   // the request id; the idempotency key
    pub tenant_id: &'a str,
    pub now: SystemTime,
    pub amount_nano_usd: u64,
    pub prompt_nano_usd: u64,
    pub budget_nano_usd: u64,
}

pub struct Closing {
    pub id: uuid::Uuid,
    pub tenant_id: Arc<str>,
    pub bucket: u64,
    pub delta_nano_usd: i128, // relative to the reservation, as today
    pub outcome: Outcome,     // Settled | Abandoned | Released | Committed
    pub at: SystemTime,
}

pub enum LedgerError {
    Budget(LimitError),        // BudgetExhausted / BudgetWouldBeExceeded
    Unavailable(String),       // backend down or timed out
}
```

- `MemoryLedger`: move today's `SpendLedger` logic here, keyed by tenant
  (a `DashMap<String, SpendLedger>`). Behavior must be byte-for-byte what
  it is now.
- `PostgresLedger`: see 3.2 to 3.4.
- `TenantState.spend` goes away. `LimitEngine` holds an `Arc<dyn Ledger>`.
- `Reservation` gains an `id: Uuid` and keeps an `Arc<dyn Ledger>`. Its
  private `correct()` becomes `ledger.close(Closing { .. })`. The
  `close()`/`settled` flag stays in `Reservation`: that flag is the
  in-process guard, and the DB state column (3.2) is the cross-process one.
- `LimitEngine::admit` takes a `reservation_id: Uuid`. `chat.rs` passes the
  request id. Note that `Uuid::parse_str(request_id)` can fail, because
  clients choose `x-request-id`: use a fresh v4 id when it does, **and
  also** when the id was already used, since a client can resend the same
  id. Simplest rule: always generate a fresh v4 id for the reservation and
  log the request id next to it.
- `LimitEngine::snapshot` becomes `async` and returns
  `Result<BudgetSnapshot, LedgerError>`. Update `system::get_usage`.

### 3.2 Postgres schema

Embedded migrations in `migrations/`, run at boot with
`sqlx::migrate!()`.

```sql
CREATE TABLE spend_days (
    tenant_id       TEXT    NOT NULL,
    day             BIGINT  NOT NULL,          -- budget_bucket(): days since epoch, UTC
    spent_nano_usd  BIGINT  NOT NULL DEFAULT 0 CHECK (spent_nano_usd >= 0),
    PRIMARY KEY (tenant_id, day)
);

CREATE TABLE reservations (
    id                 UUID        PRIMARY KEY,
    tenant_id          TEXT        NOT NULL,
    day                BIGINT      NOT NULL,
    reserved_nano_usd  BIGINT      NOT NULL,
    prompt_nano_usd    BIGINT      NOT NULL,
    state              TEXT        NOT NULL DEFAULT 'open'
        CHECK (state IN ('open','settled','abandoned','released','committed','swept')),
    delta_nano_usd     BIGINT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at          TIMESTAMPTZ
);
CREATE INDEX reservations_open ON reservations (created_at) WHERE state = 'open';
```

Nano-USD fits in `BIGINT`: `i64::MAX` is about 9.2 billion USD. Convert with
`i64::try_from`, never with `as`, and turn overflow into an error.

**try_reserve**: one transaction.

```sql
INSERT INTO spend_days (tenant_id, day) VALUES ($1, $2) ON CONFLICT DO NOTHING;
UPDATE spend_days
   SET spent_nano_usd = spent_nano_usd + $3
 WHERE tenant_id = $1 AND day = $2
   AND ($4 = 0 OR (spent_nano_usd < $4 AND spent_nano_usd + $3 <= $4))
RETURNING spent_nano_usd;
-- no row returned: SELECT spent_nano_usd, then build BudgetExhausted or
-- BudgetWouldBeExceeded exactly as the memory backend would; ROLLBACK
INSERT INTO reservations (id, tenant_id, day, reserved_nano_usd, prompt_nano_usd)
VALUES ($5, $1, $2, $3, $6);
```

The row lock taken by the `UPDATE` serializes concurrent admissions for a
tenant-day, across replicas too. No advisory locks are needed. Use the
default READ COMMITTED isolation.

**close**: one transaction per closing, applied by the writer.

```sql
UPDATE reservations
   SET state = $2, delta_nano_usd = $3, closed_at = now()
 WHERE id = $1 AND state = 'open'
RETURNING tenant_id, day;
-- no row: already closed (duplicate or swept). COMMIT and stop. This is the
-- idempotency guarantee, and it holds across processes.
-- Otherwise apply the correction with invariant 3's rule, where today =
-- budget_bucket(closing.at):
--   day == today            -> UPDATE spend_days SET spent = GREATEST(spent + delta, 0) ...
--   day <  today, delta > 0 -> upsert today's row and add delta
--   day <  today, delta < 0 -> nothing
```

### 3.3 The settlement writer (why `close` can be sync)

`close()` pushes a `Closing` onto an **unbounded**
`tokio::sync::mpsc` channel and returns. It must never block, and it must
never drop a settlement.

One background task drains the channel. It applies each closing in its own
transaction. On a transient error it retries with capped exponential
backoff (100 ms doubling to 5 s, forever), keeping order. `flush()` sends a
oneshot marker through the same channel and awaits it.

Graceful shutdown in `main.rs`: after the stream drain, call
`ledger.flush()` with a timeout of `shutdown_grace_ms`. Any closing still
queued at that point is lost. Its reservation stays `open` and the sweeper
handles it (3.4).

Metrics to add:
- `gw_ledger_pending_closings` (gauge)
- `gw_ledger_errors_total{op="reserve|close|snapshot"}` (counter, where
  `op` is a fixed set)

### 3.4 Crash recovery: the sweeper

If a process dies, its `open` reservations never close. At boot, and then
every `ledger.sweep_interval_secs`, run:

```sql
UPDATE reservations SET state = 'swept', delta_nano_usd = 0, closed_at = now()
 WHERE state = 'open' AND created_at < now() - make_interval(secs => $1);
```

A swept reservation **keeps its full reservation as the bill**
(`delta = 0`). Usage is unknowable after a crash, and this is the same
choice passthrough makes. The threshold, `ledger.sweep_after_secs`
(default 3600), must be comfortably longer than the longest stream. Say so
in the config comment.

### 3.5 When the database is unavailable

Config `ledger.on_unavailable = "deny" | "allow"`, default `"deny"`.

- **`deny`**: `admit` returns a new
  `LimitError::LedgerUnavailable`. It maps to **503** with
  `error.code = "ledger_unavailable"`, `retryable: true`, and a
  `retry-after: 5` header.
- **`allow`**: admit without a reservation row, log at `warn`, and count
  `gw_ledger_errors_total{op="reserve"}`. The later `close()` for that
  reservation must be a no-op, so mark the `Reservation` untracked.

`try_reserve` gets a statement timeout (`ledger.timeout_ms`, default 250),
so a slow database cannot hang admissions.

`/v1/usage` returns 503 `ledger_unavailable` when its snapshot fails.
`/health/ready` reports `"ledger unreachable"` as an issue. This takes the
replica out of the load balancer, which is the correct behavior for `deny`.

### 3.6 Config

```toml
[ledger]
backend = "memory"                     # memory | postgres
url_env = "LLM_GATEWAY_DATABASE_URL"   # the variable's name; the URL itself stays in env
max_connections = 16
timeout_ms = 250
on_unavailable = "deny"
sweep_after_secs = 3600
sweep_interval_secs = 300
```

Every field is `#[serde(default)]`, so existing config files keep working.
The config struct is `deny_unknown_fields`, like its siblings. Add the new
settings to `redacted_settings`: backend name only, **never** the URL.

### 3.7 Dependency

```toml
sqlx = { version = "0.8", default-features = false, features = [
  "runtime-tokio", "tls-rustls", "postgres", "uuid", "macros", "migrate",
] }
```

⚠️ Use `sqlx::query` / `query_as` (runtime-checked), **not** the
`query!` macros. The macros need a live `DATABASE_URL` or `.sqlx` offline
data at compile time, and that would break `cargo build` for everyone else
and in CI. Commit the updated `Cargo.lock`. Check that
`cargo +1.88 check --locked --all-targets` still passes, and if sqlx needs a
newer Rust, stop and write that down in the notes (section 5).

---

## 4. Phases

Commit at the end of each phase, only with the full gate green:

```bash
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
```

### Phase 1: extract the trait, memory backend only (no sqlx yet)
- `Ledger` trait, `MemoryLedger`, `LimitEngine` holding an
  `Arc<dyn Ledger>`, and `Reservation` carrying an id plus
  `ledger.close()`.
- `snapshot` becomes async, and its callers are updated.
- **Done when:** every existing test passes, with only mechanical call-site
  changes. **No behavior change.** This phase is a pure refactor.

### Phase 2: Postgres backend
- Migrations, `PostgresLedger`, the settlement writer, the sweeper, the
  config, and bootstrap wiring (`backend = "postgres"` builds the pool, runs
  migrations, and starts the writer and sweeper).
- The 503 for `LedgerUnavailable`, the readiness issue, the metrics, and
  `flush()` on shutdown.
- **Done when:** the gateway boots against a local Postgres and a streamed
  request leaves one `settled` reservation row and the right
  `spend_days` value.

### Phase 3: conformance suite, run against both backends
- A new file, `tests/ledger_conformance.rs`, with one generic test body per
  invariant in section 2. Each runs against `MemoryLedger` always, and
  against `PostgresLedger` when `LLM_GATEWAY_TEST_DATABASE_URL` is set.
  With the variable unset, the Postgres cases print `skipped` and pass.
- Give each test its own tenant id, `format!("t-{}", Uuid::new_v4())`,
  so parallel tests never share rows. Never truncate tables.
- Port these properties, calling `flush()` before any ledger assertion:
  - every ordering of closing calls;
  - the randomized concurrent ledger property;
  - admissions racing at the budget limit;
  - both rollover tests.
- Postgres-only tests:
  - **Cross-process idempotence:** two `PostgresLedger` instances on the
    same database close the same reservation id, and it is billed once.
  - **Replica budget:** two instances admit concurrently against one
    tenant-day budget, and the total admitted fits the budget exactly. This
    is the headline claim of the whole plan.
  - **Restart:** reserve and settle, drop the ledger, build a new one on
    the same database, and `snapshot` still shows the spend.
  - **Sweeper:** an `open` row older than the threshold becomes `swept`,
    and its full reservation stays billed.
  - **Unavailable:** point at a closed port, and `deny` gives a 503 while
    `allow` admits. Drive this through the router, like `tests/integration.rs`.
- **Done when:** the suite is green against both backends locally.

### Phase 4: CI, demo, docs
- **CI:** on the Linux test job, add a `postgres:16` service container and
  set `LLM_GATEWAY_TEST_DATABASE_URL`. Windows runners cannot run service
  containers, so Windows keeps running memory-only.
- **Demo:** add a `postgres` service to `demo/compose.yaml` and switch the
  demo to `backend = "postgres"`. Add a step to `demo/client.sh` that
  restarts the gateway container and checks that `shoestring` is still
  refused (402) afterward. That proves the durability claim end to end.
- **README:** rewrite "State and restarts" to cover both backends. Spell out
  what is now durable (spend, reservations) and what is still per-process
  (RPM/TPM, concurrency). Document `on_unavailable` and the sweeper.

---

## 5. Handoff to Claude Code

When you stop, whether finished or stuck, leave:

1. Every phase you completed committed on `durable-ledger`, one commit per
   phase, with a clear message.
2. Anything half-done committed separately as `WIP: ...`, and never mixed
   into a phase commit.
3. `docs/plans/durable-ledger-NOTES.md` containing:
   - what is done, per phase;
   - **every deviation from this plan, and why**;
   - failing tests, each with its name, the exact command, and its output
     or the first 30 lines of it;
   - anything you suspect but did not prove, such as a race or a flaky
     test;
   - how you ran Postgres locally (image, port, URL) so it can be
     reproduced.
4. Do not weaken, delete, or `#[ignore]` a test to get green. If a test
   blocks you, leave it failing and write it up in the notes. A failing
   test with a clear note is more useful than a green suite that lies.
5. Do not push to `master`, and do not merge PR #1.

---

## 6. Traps (read before Phase 2)

- **`Drop` cannot `.await`.** Anything a stream's `Drop` calls must be sync.
  That is why `close()` enqueues work instead of running it.
- **Unbounded channel.** A bounded channel whose `try_send` fails in `Drop`
  loses money silently. The memory cost of a backlog is the right trade.
- **Order within a reservation.** Its `close` is enqueued exactly once
  (the in-process `settled` flag guarantees that), so there is nothing to
  reorder.
- **Clock.** "Today" for a closing is `budget_bucket(closing.at)`, taken
  when `close()` is called, **not** when the writer applies it. Otherwise a
  backlog that crosses midnight changes which day gets charged.
- **Test isolation.** One tenant id per test. Do not rely on test order or
  on empty tables.
- **Two limits for concurrency.** `max_concurrent_streams_global` and the
  per-tenant cap are still per-process, and that is by design. Do not try
  to make them distributed in this plan.
- **Secrets.** The database URL comes from env through `url_env`. It must
  never appear in logs, in `redacted_settings`, or in an error message
  returned to a client. The log-redaction integration test covers only
  tenant keys, so extend it to cover the database URL too.
