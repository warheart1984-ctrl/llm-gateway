//! Load harness: what the gateway adds, where it starts refusing, and whether
//! its ledger settles every reservation exactly, under concurrent load.
//!
//! ```text
//! cargo bench --bench load
//! ```
//!
//! A fake upstream streams answers at a fixed pace (first token after
//! `LOAD_TTFT_MS`, then `LOAD_TOKENS` tokens `LOAD_INTERVAL_MS` apart). At
//! each concurrency level, `C` clients loop on streaming requests for
//! `LOAD_SECONDS`: first straight at the upstream, then through a gateway on
//! each ledger in `LOAD_LEDGERS`. Clients are spread over `LOAD_TENANTS`
//! tenants. Per level and ledger it reports:
//!
//! * **added TTFT**: time to the first token through the gateway minus the
//!   same percentile straight from the upstream, at the same concurrency;
//! * **ok/s**: completed streams per second, i.e. reservations made and
//!   settled per second;
//! * **refused**: answers that were not a 200, by status. The gateway runs
//!   with the shipped `max_concurrent_streams_global` (512) and ledger
//!   deadline (`LOAD_LEDGER_TIMEOUT_MS`, default the shipped 1,000 ms), so a
//!   level past the cap shows 429s and a ledger that cannot keep up shows
//!   503s. A refused client waits 50 ms before its next attempt;
//! * **reserved / billed**: the totals the clients were told, from each
//!   response's `x-reserved-nano-usd` header and its `end` event;
//! * **settle lag**: from the last response finishing to the ledger holding
//!   no open reservation;
//! * **reconciled**: after every level, per tenant, three independent records
//!   must agree, or the run fails:
//!   1. what the clients saw: admitted streams, reserved and billed totals;
//!   2. the ledger's `reservations` rows: one per admitted stream, none left
//!      `open` or `swept`, each bill `reserved + delta`;
//!   3. the tenant's total: `spend_days` in the database, and
//!      `spent_nano_usd` from `GET /v1/usage`, with no stream in flight.
//!
//!   The memory ledger has no tables, so it checks (1) against `/v1/usage`.
//!   Totals are cumulative across levels, since the ledger is too;
//! * **late released / swept / sweep failures**: the ledger's upkeep
//!   counters from `/metrics`, cumulative: admissions whose commit landed
//!   after their deadline and were released at no charge; reservations the
//!   sweeper billed in full; and sweeper steps that failed. A released row
//!   is one no client was admitted on, so it must bill nothing;
//! * **KiB/stream**: peak heap through the gateway minus peak heap straight
//!   to the upstream, divided by the most streams the gateway held open at
//!   once. Counted by this binary's allocator, so heap only.
//!
//! `LOAD_LEDGERS` is a list of `memory`, `sqlite` and `postgres` (default
//! `memory,sqlite`, plus `postgres` when `LLM_GATEWAY_TEST_DATABASE_URL` is
//! set). Postgres gets a fresh schema per run, named in the output; SQLite a
//! fresh file in a temp directory.
//!
//! What this is not: production numbers. Upstream, gateway, clients and the
//! database share one machine and its CPU, over loopback, against an
//! upstream that never slows down. Read the results as the gateway's own
//! overhead and ceilings on the machine that produced them.
//!
//! Other knobs: `LOAD_LEVELS` (default `50,200,500,1000`), `LOAD_FRAMING`
//! (`normalized` or `passthrough`), `LOAD_GLOBAL_CAP`. Each level is appended
//! to `target/load-report.jsonl`. Under `cargo test` (no `--bench` flag) it
//! runs one tiny level on the memory and SQLite ledgers as a smoke check,
//! reconciliation included.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeMap,
    io::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{Router, body::Body, http::header, response::Response, routing::post};
use futures_util::StreamExt as _;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Heap accounting
// ---------------------------------------------------------------------------

/// The system allocator, counting live bytes and their high-water mark.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Start a new high-water mark from the current live heap; returns it.
fn reset_peak() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

const DATABASE_URL_ENV: &str = "LLM_GATEWAY_TEST_DATABASE_URL";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LedgerKind {
    Memory,
    Sqlite,
    Postgres,
}

impl LedgerKind {
    fn name(self) -> &'static str {
        match self {
            LedgerKind::Memory => "memory",
            LedgerKind::Sqlite => "sqlite",
            LedgerKind::Postgres => "postgres",
        }
    }
}

#[derive(Clone, Debug)]
struct Plan {
    levels: Vec<usize>,
    seconds: u64,
    ttft_ms: u64,
    tokens: u32,
    interval_ms: u64,
    global_cap: usize,
    framing: String,
    tenants: usize,
    ledgers: Vec<LedgerKind>,
    ledger_timeout_ms: u64,
}

impl Plan {
    fn from_env(smoke: bool) -> Self {
        let num = |name: &str, default: u64| {
            std::env::var(name).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default)
        };
        let framing = std::env::var("LOAD_FRAMING").unwrap_or_else(|_| "normalized".into());
        assert!(
            framing == "normalized" || framing == "passthrough",
            "LOAD_FRAMING must be `normalized` or `passthrough`"
        );
        let database = std::env::var(DATABASE_URL_ENV).is_ok_and(|v| !v.trim().is_empty());
        let ledgers = match std::env::var("LOAD_LEDGERS") {
            Ok(list) => list
                .split(',')
                .map(|l| match l.trim() {
                    "memory" => LedgerKind::Memory,
                    "sqlite" => LedgerKind::Sqlite,
                    "postgres" => {
                        assert!(database, "LOAD_LEDGERS names postgres, but {DATABASE_URL_ENV} is not set");
                        LedgerKind::Postgres
                    }
                    other => panic!("LOAD_LEDGERS: unknown ledger `{other}`"),
                })
                .collect(),
            Err(_) if smoke => vec![LedgerKind::Memory, LedgerKind::Sqlite],
            Err(_) if database => vec![LedgerKind::Memory, LedgerKind::Sqlite, LedgerKind::Postgres],
            Err(_) => {
                println!("load: postgres skipped; set {DATABASE_URL_ENV} to include it");
                vec![LedgerKind::Memory, LedgerKind::Sqlite]
            }
        };
        if smoke {
            return Self {
                levels: vec![4],
                seconds: 1,
                ttft_ms: 5,
                tokens: 4,
                interval_ms: 5,
                global_cap: 512,
                framing,
                tenants: 2,
                ledgers,
                ledger_timeout_ms: 5_000,
            };
        }
        let levels = std::env::var("LOAD_LEVELS")
            .ok()
            .map(|v| v.split(',').filter_map(|l| l.trim().parse().ok()).collect::<Vec<usize>>())
            .filter(|l| !l.is_empty());
        Self {
            levels: levels.unwrap_or_else(|| vec![50, 200, 500, 1000]),
            seconds: num("LOAD_SECONDS", 10),
            ttft_ms: num("LOAD_TTFT_MS", 50),
            tokens: num("LOAD_TOKENS", 32) as u32,
            interval_ms: num("LOAD_INTERVAL_MS", 20),
            global_cap: num("LOAD_GLOBAL_CAP", 512) as usize,
            framing,
            tenants: num("LOAD_TENANTS", 10).max(1) as usize,
            ledgers,
            ledger_timeout_ms: num("LOAD_LEDGER_TIMEOUT_MS", 1_000),
        }
    }
}

// ---------------------------------------------------------------------------
// Fake upstream
// ---------------------------------------------------------------------------

/// Token text. Appears in no other frame either side sends (ids are hex,
/// field names have no `zq`), so finding it marks the first token.
const TOKEN: &str = "zq";

fn upstream_app(plan: &Plan) -> Router {
    let (ttft, tokens, interval) = (
        Duration::from_millis(plan.ttft_ms),
        plan.tokens,
        Duration::from_millis(plan.interval_ms),
    );
    Router::new().route(
        "/v1/chat/completions",
        post(move |_body: axum::body::Bytes| async move {
            let frames = futures_util::stream::unfold(0u32, move |i| async move {
                let frame = if i == 0 {
                    tokio::time::sleep(ttft).await;
                    chunk()
                } else if i < tokens {
                    tokio::time::sleep(interval).await;
                    chunk()
                } else if i == tokens {
                    let done = json!({
                        "id": "load", "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }],
                        "usage": { "prompt_tokens": 8, "completion_tokens": tokens, "total_tokens": 8 + tokens }
                    });
                    format!("data: {done}\n\ndata: [DONE]\n\n")
                } else {
                    return None;
                };
                Some((Ok::<_, std::io::Error>(frame), i + 1))
            });
            let mut resp = Response::new(Body::from_stream(frames));
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
            resp
        }),
    )
}

fn chunk() -> String {
    let frame = json!({ "id": "load", "model": "load-model",
                        "choices": [{ "index": 0, "delta": { "content": TOKEN } }] });
    format!("data: {frame}\n\n")
}

/// Serve `app` on its own runtime and thread, so its scheduling does not
/// compete with the clients' inside one executor.
fn serve_on_own_runtime<F, Fut>(name: &str, threads: usize, app: F) -> SocketAddr
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Router>,
{
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async move {
                let app = app().await;
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
                tx.send(listener.local_addr().unwrap()).unwrap();
                axum::serve(listener, app).await.expect("serve");
            });
        })
        .expect("spawn server thread");
    rx.recv().expect("server address: the server thread failed to start; see its panic above")
}

// ---------------------------------------------------------------------------
// Gateway
// ---------------------------------------------------------------------------

const ADMIN_KEY: &str = "load-admin-key";

fn key_of(tenant: usize) -> String {
    format!("load-key-{tenant}")
}

/// Where a gateway's ledger keeps its rows, for reconciliation.
#[derive(Clone)]
enum Store {
    /// Process memory: only `/v1/usage` can see it.
    Memory,
    Sqlite(PathBuf),
    Postgres { url: String, schema: String },
}

struct GatewayUnderTest {
    kind: LedgerKind,
    addr: SocketAddr,
    store: Store,
}

fn boot_gateway(upstream: SocketAddr, plan: &Plan, kind: LedgerKind, threads: usize) -> GatewayUnderTest {
    let dir = std::env::temp_dir().join(format!("llm-gateway-load-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("models.yaml"),
        format!(
            "schema_version: 1\nmodels:\n  load/chat:\n    provider: nvidia\n    endpoint: http://{upstream}/v1/chat/completions\n    \
             upstream_model: load-model\n    cost:\n      input_per_mtok_usd: 1.0\n      output_per_mtok_usd: 1.0\n"
        ),
    )
    .unwrap();
    // Tenants whose own limits never bind, so any refusal is the
    // gateway-wide cap or the ledger.
    let mut tenants = String::from("tenants:\n");
    for t in 0..plan.tenants {
        // The first tenant also holds the key that reads `/metrics`.
        let admin = if t == 0 {
            format!("      - key_id: ak_load_admin\n        key: {ADMIN_KEY}\n        scopes: [admin]\n")
        } else {
            String::new()
        };
        tenants.push_str(&format!(
            "  - tenant_id: load-{t}\n    enabled: true\n    credentials:\n      - key_id: ak_load_{t}\n        key: {}\n        \
             scopes: [chat:stream]\n{admin}    allowed_models: [\"*\"]\n    limits:\n      requests_per_minute: 100000000\n      \
             tokens_per_minute: 4000000000\n      max_concurrent_streams: 1000000\n      max_output_tokens: 4096\n      \
             daily_budget_nano_usd: 1000000000000000\n",
            key_of(t)
        ));
    }
    std::fs::write(dir.join("tenants.yaml"), tenants).unwrap();

    let mut ledger = llm_gateway::config::LedgerConfig {
        timeout_ms: plan.ledger_timeout_ms,
        // Never pick up a developer's sealing keys from the environment.
        response_keys_env: "LLM_GATEWAY_LOAD_NO_RESPONSE_KEYS".into(),
        fingerprint_keys_env: "LLM_GATEWAY_LOAD_NO_FINGERPRINT_KEYS".into(),
        ..Default::default()
    };
    let store = match kind {
        LedgerKind::Memory => {
            ledger.backend = llm_gateway::config::LedgerBackend::Memory;
            Store::Memory
        }
        LedgerKind::Sqlite => {
            let path = dir.join("ledger.sqlite3");
            ledger.backend = llm_gateway::config::LedgerBackend::Sqlite;
            ledger.sqlite_path = path.clone();
            Store::Sqlite(path)
        }
        LedgerKind::Postgres => {
            let schema = format!("load_{}", uuid::Uuid::new_v4().simple());
            ledger.backend = llm_gateway::config::LedgerBackend::Postgres;
            ledger.url_env = DATABASE_URL_ENV.into();
            ledger.schema = schema.clone();
            Store::Postgres { url: std::env::var(DATABASE_URL_ENV).unwrap(), schema }
        }
    };
    let settings = llm_gateway::config::Settings {
        server: llm_gateway::config::ServerConfig {
            bind_addr: "127.0.0.1".into(),
            port: 0,
            max_concurrent_streams_global: plan.global_cap,
            ..Default::default()
        },
        registry: llm_gateway::config::RegistryConfig {
            models_path: dir.join("models.yaml"),
            tenants_path: dir.join("tenants.yaml"),
            hot_reload: false,
            reload_interval_ms: 60_000,
        },
        ledger,
        ..Default::default()
    };
    let addr = serve_on_own_runtime(&format!("gateway-{}", kind.name()), threads, move || async move {
        let state = llm_gateway::bootstrap::build(settings).await.expect("build gateway");
        llm_gateway::api::router(state)
    });
    GatewayUnderTest { kind, addr, store }
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

/// What the clients were told, per tenant.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct Money {
    admitted: u64,
    reserved: u64,
    billed: u64,
}

impl std::ops::AddAssign for Money {
    fn add_assign(&mut self, o: Self) {
        self.admitted += o.admitted;
        self.reserved += o.reserved;
        self.billed += o.billed;
    }
}

#[derive(Default)]
struct Tally {
    ttft_us: Vec<u64>,
    ok: u64,
    refused: BTreeMap<u16, u64>,
    broken: u64,
    /// Per tenant. A broken stream that was admitted still counts here: it
    /// holds a reservation the ledger must settle.
    money: BTreeMap<usize, Money>,
    /// Admitted streams whose bill the client never learned.
    unbilled: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum Target {
    Upstream,
    Gateway,
}

struct Run {
    tally: Tally,
    wall: Duration,
    peak_heap: usize,
    max_open: usize,
}

async fn run_level(
    client: &reqwest::Client,
    target: Target,
    url: &str,
    plan: &Plan,
    concurrency: usize,
) -> Run {
    let tally = Arc::new(Mutex::new(Tally::default()));
    let open = Arc::new(AtomicUsize::new(0));
    let max_open = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let body = match target {
        Target::Upstream => json!({ "model": "load-model", "stream": true,
                                    "messages": [{ "role": "user", "content": "hi" }], "max_tokens": 64 }),
        Target::Gateway => json!({ "model": "load/chat", "stream": true, "framing": plan.framing,
                                   "messages": [{ "role": "user", "content": "hi" }],
                                   "params": { "max_tokens": 64 } }),
    };
    let body = bytes::Bytes::from(serde_json::to_vec(&body).unwrap());
    let passthrough = plan.framing == "passthrough";

    let baseline = reset_peak();
    let started = Instant::now();
    let workers: Vec<_> = (0..concurrency)
        .map(|worker| {
            let tenant = worker % plan.tenants;
            let ctx = Worker {
                client: client.clone(),
                url: url.to_string(),
                body: body.clone(),
                key: key_of(tenant),
                tenant,
                passthrough,
                tally: Arc::clone(&tally),
                open: Arc::clone(&open),
                max_open: Arc::clone(&max_open),
            };
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    ctx.one_request().await;
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_secs(plan.seconds)).await;
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }
    let wall = started.elapsed();
    let peak_heap = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
    let tally = Arc::try_unwrap(tally).ok().unwrap().into_inner().unwrap();
    Run { tally, wall, peak_heap, max_open: max_open.load(Ordering::Relaxed) }
}

struct Worker {
    client: reqwest::Client,
    url: String,
    body: bytes::Bytes,
    key: String,
    tenant: usize,
    passthrough: bool,
    tally: Arc<Mutex<Tally>>,
    open: Arc<AtomicUsize>,
    max_open: Arc<AtomicUsize>,
}

impl Worker {
    async fn one_request(&self) {
        let sent = Instant::now();
        let reply = self
            .client
            .post(&self.url)
            .header("x-api-key", &self.key)
            .header(header::CONTENT_TYPE, "application/json")
            .body(self.body.clone())
            .send()
            .await;
        let reply = match reply {
            Ok(r) => r,
            Err(_) => {
                self.tally.lock().unwrap().broken += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
                return;
            }
        };
        let status = reply.status().as_u16();
        if status != 200 {
            let _ = reply.bytes().await;
            *self.tally.lock().unwrap().refused.entry(status).or_default() += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
            return;
        }
        // Absent straight from the upstream; present on every admitted
        // gateway stream.
        let reserved: Option<u64> = reply
            .headers()
            .get("x-reserved-nano-usd")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        let now_open = self.open.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_open.fetch_max(now_open, Ordering::Relaxed);

        let mut events = Events::default();
        let mut stream = reply.bytes_stream();
        let mut failed = false;
        while let Some(piece) = stream.next().await {
            let Ok(piece) = piece else {
                failed = true;
                break;
            };
            events.feed(&piece, sent);
        }
        self.open.fetch_sub(1, Ordering::Relaxed);

        let mut t = self.tally.lock().unwrap();
        let clean = !failed && !events.error && events.done && events.first.is_some();
        if clean {
            t.ok += 1;
            t.ttft_us.push(events.first.unwrap().as_micros() as u64);
        } else {
            t.broken += 1;
        }
        if let Some(reserved) = reserved {
            // Passthrough is billed the reservation; normalized says what it
            // billed in its `end` event.
            let billed = if self.passthrough { Some(reserved) } else { events.cost };
            let money = t.money.entry(self.tenant).or_default();
            money.admitted += 1;
            money.reserved += reserved;
            match billed {
                Some(b) => money.billed += b,
                None => t.unbilled += 1,
            }
        }
    }
}

/// An SSE reader that keeps only the event in progress, so a long stream
/// costs the client a few hundred bytes, not its whole body.
#[derive(Default)]
struct Events {
    pending: String,
    first: Option<Duration>,
    done: bool,
    error: bool,
    /// `cost_nano_usd` from the gateway's `end` event.
    cost: Option<u64>,
}

impl Events {
    fn feed(&mut self, piece: &[u8], sent: Instant) {
        self.pending.push_str(&String::from_utf8_lossy(piece));
        while let Some(end) = self.pending.find("\n\n") {
            let event: String = self.pending.drain(..end + 2).collect();
            if self.first.is_none() && event.contains(TOKEN) {
                self.first = Some(sent.elapsed());
            }
            if event.contains("[DONE]") {
                self.done = true;
            }
            if event.starts_with("event: error") || event.contains("\nevent: error") {
                self.error = true;
            }
            if event.starts_with("event: end") || event.contains("\nevent: end") {
                self.cost = event
                    .lines()
                    .find_map(|l| l.strip_prefix("data:"))
                    .and_then(|d| serde_json::from_str::<Value>(d.trim()).ok())
                    .and_then(|v| v["cost_nano_usd"].as_u64());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

/// One tenant's reservations as the ledger holds them.
#[derive(Default, Debug)]
struct LedgerRows {
    /// Rows by state.
    states: BTreeMap<String, u64>,
    reserved: u64,
    /// `reserved + delta` over every row.
    billed: u64,
    /// Rows released without reaching a client: an admission whose commit
    /// landed after its deadline. Each must bill exactly nothing.
    released: u64,
    released_reserved: u64,
    released_billed: u64,
    /// The tenant's `spend_days`, all days.
    spend_days: u64,
}

async fn ledger_rows(store: &Store) -> Option<BTreeMap<String, LedgerRows>> {
    // Same SQL for both databases; casts keep Postgres' SUM from widening to
    // NUMERIC.
    let rows_sql = |t: &str| {
        format!(
            "SELECT tenant_id, state, COUNT(*), CAST(COALESCE(SUM(reserved_nano_usd), 0) AS BIGINT),
                    CAST(COALESCE(SUM(reserved_nano_usd + COALESCE(delta_nano_usd, 0)), 0) AS BIGINT)
               FROM {t} GROUP BY tenant_id, state"
        )
    };
    let days_sql =
        |t: &str| format!("SELECT tenant_id, CAST(COALESCE(SUM(spent_nano_usd), 0) AS BIGINT) FROM {t} GROUP BY tenant_id");
    type Row = (String, String, i64, i64, i64);
    let (rows, days): (Vec<Row>, Vec<(String, i64)>) = match store {
        Store::Memory => return None,
        Store::Sqlite(path) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(path).read_only(true))
                .await
                .expect("open the SQLite ledger for reading");
            let rows = sqlx::query_as(&rows_sql("reservations")).fetch_all(&pool).await.unwrap();
            let days = sqlx::query_as(&days_sql("spend_days")).fetch_all(&pool).await.unwrap();
            pool.close().await;
            (rows, days)
        }
        Store::Postgres { url, schema } => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await
                .expect("connect to the Postgres ledger");
            let rows = sqlx::query_as(&rows_sql(&format!("\"{schema}\".reservations")))
                .fetch_all(&pool)
                .await
                .unwrap();
            let days = sqlx::query_as(&days_sql(&format!("\"{schema}\".spend_days")))
                .fetch_all(&pool)
                .await
                .unwrap();
            pool.close().await;
            (rows, days)
        }
    };
    let mut out: BTreeMap<String, LedgerRows> = BTreeMap::new();
    for (tenant, state, count, reserved, billed) in rows {
        let t = out.entry(tenant).or_default();
        if state == "released" {
            t.released += count as u64;
            t.released_reserved += reserved as u64;
            t.released_billed += billed as u64;
        }
        *t.states.entry(state).or_default() += count as u64;
        t.reserved += reserved as u64;
        t.billed += billed as u64;
    }
    for (tenant, spent) in days {
        out.entry(tenant).or_default().spend_days = spent as u64;
    }
    Some(out)
}

/// Wait for the ledger to close every reservation, then check that the
/// clients, the rows and the totals agree. Returns the settle lag and every
/// disagreement found.
async fn reconcile(
    client: &reqwest::Client,
    gw: &GatewayUnderTest,
    plan: &Plan,
    seen: &BTreeMap<usize, Money>,
) -> (Duration, Vec<String>) {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(60);
    // Settled: nothing open in the database (or, for memory, nothing in
    // flight) and the totals have stopped moving.
    let mut last: Option<Vec<(u64, u64)>> = None;
    loop {
        let usage = usage_of_all(client, gw, plan).await;
        let open_rows = match ledger_rows(&gw.store).await {
            Some(rows) => rows.values().map(|r| r.states.get("open").copied().unwrap_or(0)).sum(),
            None => 0,
        };
        let in_flight: u64 = usage.iter().map(|u| u.1).sum();
        if open_rows == 0 && in_flight == 0 && last.as_ref() == Some(&usage) {
            break;
        }
        if Instant::now() > deadline {
            return (started.elapsed(), vec![format!("never settled: {open_rows} open rows, {in_flight} in flight")]);
        }
        last = Some(usage);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // The last check found it settled twice in a row, 50 ms apart.
    let lag = started.elapsed().saturating_sub(Duration::from_millis(50));

    let mut problems = Vec::new();
    let usage = usage_of_all(client, gw, plan).await;
    let rows = ledger_rows(&gw.store).await;
    for (tenant, &(api_spent, _)) in usage.iter().enumerate() {
        let name = format!("load-{tenant}");
        let seen = seen.get(&tenant).copied().unwrap_or_default();
        if api_spent != seen.billed {
            problems.push(format!("{name}: /v1/usage spent {api_spent}, clients were billed {}", seen.billed));
        }
        let Some(rows) = &rows else { continue };
        let empty = LedgerRows::default();
        let r = rows.get(&name).unwrap_or(&empty);
        // A client is told nothing about a released row: its request was
        // refused. Every other row is one admitted stream.
        let admitted_rows = r.states.values().sum::<u64>() - r.released;
        if admitted_rows != seen.admitted {
            problems.push(format!("{name}: {admitted_rows} reservation rows, {} admitted streams", seen.admitted));
        }
        if r.released_billed != 0 {
            problems.push(format!("{name}: {} released rows bill {}", r.released, r.released_billed));
        }
        for bad in ["open", "swept"] {
            if let Some(n) = r.states.get(bad).filter(|n| **n > 0) {
                problems.push(format!("{name}: {n} reservations left `{bad}`"));
            }
        }
        if r.reserved - r.released_reserved != seen.reserved {
            problems.push(format!(
                "{name}: rows reserved {}, clients were told {}",
                r.reserved - r.released_reserved,
                seen.reserved
            ));
        }
        if r.billed != seen.billed {
            problems.push(format!("{name}: rows bill {}, clients were billed {}", r.billed, seen.billed));
        }
        if r.spend_days != r.billed {
            problems.push(format!("{name}: spend_days {} but rows bill {}", r.spend_days, r.billed));
        }
    }
    (lag, problems)
}

/// The ledger's upkeep counters, cumulative: `(late commits released,
/// reservations swept, sweep failures)`.
async fn upkeep_of(client: &reqwest::Client, gw: &GatewayUnderTest) -> (u64, u64, u64) {
    let text = client
        .get(format!("http://{}/metrics", gw.addr))
        .header("x-api-key", ADMIN_KEY)
        .send()
        .await
        .expect("metrics")
        .text()
        .await
        .unwrap_or_default();
    let read = |name: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.trim().parse().ok())
            .unwrap_or(0)
    };
    (
        read("gw_ledger_late_commits_released_total"),
        read("gw_ledger_swept_reservations_total"),
        read("gw_ledger_sweep_failures_total"),
    )
}

/// `(spent, in flight)` for every tenant, from `GET /v1/usage`.
async fn usage_of_all(client: &reqwest::Client, gw: &GatewayUnderTest, plan: &Plan) -> Vec<(u64, u64)> {
    let mut out = Vec::with_capacity(plan.tenants);
    for tenant in 0..plan.tenants {
        let usage: Value = client
            .get(format!("http://{}/v1/usage", gw.addr))
            .header("x-api-key", key_of(tenant))
            .send()
            .await
            .expect("usage")
            .json()
            .await
            .expect("usage json");
        out.push((
            usage["spent_nano_usd"].as_u64().unwrap_or(u64::MAX),
            usage["streams_in_flight"].as_u64().unwrap_or(u64::MAX),
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

fn pct(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1] as f64 / 1000.0
}

fn usd(nano: u64) -> String {
    format!("${:.4}", nano as f64 / 1e9)
}

fn main() {
    let smoke = !std::env::args().any(|a| a == "--bench");
    let plan = Plan::from_env(smoke);
    // The gateway logs one summary line per stream; format it, discard it,
    // so its cost is in the numbers.
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .with_writer(std::io::sink)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let share = (cores / 3).max(1);
    let upstream = serve_on_own_runtime("upstream", share, {
        let plan = plan.clone();
        move || async move { upstream_app(&plan) }
    });
    // One gateway per ledger, each on its own runtime. Only one is driven at
    // a time; the others sit idle.
    let gateways: Vec<GatewayUnderTest> =
        plan.ledgers.iter().map(|&kind| boot_gateway(upstream, &plan, kind, share)).collect();

    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(share).enable_all().build().unwrap();
    let failures = rt.block_on(async {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(4096)
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();
        let upstream_url = format!("http://{upstream}/v1/chat/completions");

        // Wait for every gateway to accept, then warm each path.
        let warm = Plan { seconds: 1, ..plan.clone() };
        run_level(&client, Target::Upstream, &upstream_url, &warm, 8).await;
        // Client-side totals per gateway and tenant, cumulative like the ledger.
        let mut seen: Vec<BTreeMap<usize, Money>> = vec![BTreeMap::new(); gateways.len()];
        for (g, gw) in gateways.iter().enumerate() {
            for _ in 0..200 {
                if client.get(format!("http://{}/health/live", gw.addr)).send().await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let run = run_level(&client, Target::Gateway, &gateway_url(gw), &warm, 8).await;
            for (t, m) in run.tally.money {
                *seen[g].entry(t).or_default() += m;
            }
        }

        println!(
            "load: {} framing, {}s per level, {} tenants, upstream TTFT {} ms + {} tokens every {} ms, gateway cap {}, \
             ledger deadline {} ms, {} cores ({} threads each for upstream, each gateway, clients)",
            plan.framing,
            plan.seconds,
            plan.tenants,
            plan.ttft_ms,
            plan.tokens,
            plan.interval_ms,
            plan.global_cap,
            plan.ledger_timeout_ms,
            cores,
            share
        );
        for gw in &gateways {
            if let Store::Postgres { schema, .. } = &gw.store {
                println!("load: postgres ledger in schema `{schema}`");
            }
        }
        println!(
            "| ledger | streams | ok/s | refused | broken | added TTFT p50 / p99 (ms) | reserved | billed | settle lag | reconciled | late released / swept / sweep failures | heap KiB/stream |"
        );
        println!("|---|---:|---:|---|---:|---|---:|---:|---:|---|---|---:|");
        let mut failures = Vec::new();
        for &level in &plan.levels {
            let direct = run_level(&client, Target::Upstream, &upstream_url, &plan, level).await;
            for (g, gw) in gateways.iter().enumerate() {
                let via = run_level(&client, Target::Gateway, &gateway_url(gw), &plan, level).await;
                let mut level_money = Money::default();
                for (&t, &m) in &via.tally.money {
                    *seen[g].entry(t).or_default() += m;
                    level_money += m;
                }
                let (lag, mut problems) = reconcile(&client, gw, &plan, &seen[g]).await;
                if via.tally.unbilled > 0 {
                    problems.push(format!("{} admitted streams ended without an `end` event", via.tally.unbilled));
                }
                let upkeep = upkeep_of(&client, gw).await;
                report(&plan, level, gw, &direct, &via, level_money, lag, upkeep, &problems, smoke);
                failures.extend(problems.into_iter().map(|p| format!("{} at {level} streams: {p}", gw.kind.name())));
            }
        }
        failures
    });
    if !failures.is_empty() {
        eprintln!("\nreconciliation failed:");
        for f in &failures {
            eprintln!("  {f}");
        }
        std::process::exit(1);
    }
}

fn gateway_url(gw: &GatewayUnderTest) -> String {
    format!("http://{}/v1/chat/stream", gw.addr)
}

#[allow(clippy::too_many_arguments)]
fn report(
    plan: &Plan,
    level: usize,
    gw: &GatewayUnderTest,
    direct: &Run,
    via: &Run,
    money: Money,
    lag: Duration,
    (late, swept, sweep_failures): (u64, u64, u64),
    problems: &[String],
    smoke: bool,
) {
    let mut d_ttft = direct.tally.ttft_us.clone();
    let mut g_ttft = via.tally.ttft_us.clone();
    d_ttft.sort_unstable();
    g_ttft.sort_unstable();
    let (d50, d99, g50, g99) = (pct(&d_ttft, 50.0), pct(&d_ttft, 99.0), pct(&g_ttft, 50.0), pct(&g_ttft, 99.0));
    let ok_per_s = via.tally.ok as f64 / via.wall.as_secs_f64();
    let refused: Vec<String> = via.tally.refused.iter().map(|(s, n)| format!("{n}×{s}")).collect();
    let refused = if refused.is_empty() { "0".to_string() } else { refused.join(", ") };
    let kib_per_stream = if via.max_open > 0 {
        via.peak_heap.saturating_sub(direct.peak_heap) as f64 / via.max_open as f64 / 1024.0
    } else {
        f64::NAN
    };
    let verdict = if problems.is_empty() { "✓".to_string() } else { format!("✗ {} problems", problems.len()) };
    println!(
        "| {} | {level} | {ok_per_s:.0} | {refused} | {} | {:.1} / {:.1} | {} | {} | {} ms | {verdict} | {late} / {swept} / {sweep_failures} | {kib_per_stream:.1} |",
        gw.kind.name(),
        via.tally.broken + direct.tally.broken,
        g50 - d50,
        g99 - d99,
        usd(money.reserved),
        usd(money.billed),
        lag.as_millis(),
    );
    if smoke {
        assert!(via.tally.ok > 0 && direct.tally.ok > 0, "the smoke run completed no stream");
        return;
    }
    let line = json!({
        "at": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        "ledger": gw.kind.name(),
        "framing": plan.framing,
        "streams": level,
        "tenants": plan.tenants,
        "seconds": plan.seconds,
        "upstream": { "ttft_ms": plan.ttft_ms, "tokens": plan.tokens, "interval_ms": plan.interval_ms },
        "global_cap": plan.global_cap,
        "ledger_timeout_ms": plan.ledger_timeout_ms,
        "ok_per_s": ok_per_s,
        "ok": via.tally.ok,
        "admitted": money.admitted,
        "reserved_nano_usd": money.reserved,
        "billed_nano_usd": money.billed,
        "refused": via.tally.refused.iter().map(|(s, n)| (s.to_string(), json!(n))).collect::<serde_json::Map<String, Value>>(),
        "broken": { "direct": direct.tally.broken, "gateway": via.tally.broken },
        "ttft_ms": { "direct_p50": d50, "direct_p99": d99, "gateway_p50": g50, "gateway_p99": g99 },
        "settle_lag_ms": lag.as_millis() as u64,
        "reconciled": problems.is_empty(),
        "upkeep": { "late_commits_released": late, "swept": swept, "sweep_failures": sweep_failures },
        "problems": problems,
        "max_open_via_gateway": via.max_open,
        "heap_kib_per_open_stream": kib_per_stream,
    });
    let report = Path::new(env!("CARGO_MANIFEST_DIR")).join("target").join("load-report.jsonl");
    let _ = std::fs::create_dir_all(report.parent().unwrap());
    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&report) {
        let _ = writeln!(file, "{line}");
    }
}
