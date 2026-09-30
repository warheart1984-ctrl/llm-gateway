//! Boundary pressure suite.
//!
//! The claim under test: **the gateway prevents an unauthorised or
//! financially inadmissible request from reaching the provider.** Each test
//! applies one kind of pressure (a forged key, a burst at the budget edge, a
//! dead or hung provider, a restart) and checks the claim where it matters:
//! at the provider. The mock upstream counts every request it receives, so
//! "refused" means the provider provably never saw it, not merely that the
//! gateway returned an error code.
//!
//! Tests named `gap_*` are different on purpose. They pin behaviour where the
//! boundary does NOT hold today (in-memory state, no replay detection) by
//! asserting what actually happens. They pass because they describe the gap
//! honestly, and they are meant to fail the day the gap is closed, so the fix
//! has to flip them rather than silently change the claim.
//!
//! The claim -> test -> result table is `docs/boundary-pressure-tests.md`.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};

// ---------------------------------------------------------------------------
// Mock provider
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Provider {
    /// A normal streamed answer: three tokens, usage 12 prompt / 3 completion.
    Healthy,
    /// `Healthy`, but each request waits for a permit before answering, so a
    /// test can hold admitted requests open at the provider.
    Gated,
    /// HTTP 429 before any stream.
    Refusing,
    /// Accepts the request and never answers.
    Hung,
    /// Starts streaming, then the connection dies mid-answer.
    DiesMidStream,
    /// Sends one token, waits for a gate permit, then finishes normally
    /// with the usual usage: a stream that runs as long as the test likes.
    HeldMidStream,
}

#[derive(Clone)]
struct ProviderState {
    behaviour: Provider,
    calls: Arc<AtomicU64>,
    gate: Arc<tokio::sync::Semaphore>,
    last_body: Arc<std::sync::Mutex<Option<Value>>>,
}

struct MockProvider {
    addr: SocketAddr,
    state: ProviderState,
}

impl MockProvider {
    async fn start(behaviour: Provider) -> Self {
        let state = ProviderState {
            behaviour,
            calls: Arc::new(AtomicU64::new(0)),
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
            last_body: Arc::new(std::sync::Mutex::new(None)),
        };
        let app = Router::new()
            .route("/v1/chat/completions", post(answer))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind provider");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, state }
    }

    /// Requests that reached the provider. The evidence for every claim.
    fn calls(&self) -> u64 {
        self.state.calls.load(Ordering::SeqCst)
    }

    fn last_body(&self) -> Value {
        self.state.last_body.lock().unwrap().clone().expect("the provider saw a request")
    }

    fn open_gate(&self, permits: usize) {
        self.state.gate.add_permits(permits);
    }
}

fn sse(payload: Value) -> String {
    format!("data: {payload}\n\n")
}

fn token(text: &str) -> String {
    sse(json!({ "id": "p1", "model": "mock-model",
                "choices": [{ "index": 0, "delta": { "content": text } }] }))
}

/// Frames are paced, so what was sent before a failure is actually flushed
/// onto the wire, as with a real provider; an error ready at the same instant
/// as the headers would reset the connection before any of it was written.
fn sse_body(frames: Vec<Result<String, std::io::Error>>) -> Response {
    let paced = futures_util::stream::unfold(frames.into_iter(), |mut frames| async move {
        let next = frames.next()?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        Some((next, frames))
    });
    let mut response = Response::new(Body::from_stream(paced));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
    response
}

async fn answer(State(state): State<ProviderState>, Json(body): Json<Value>) -> Response {
    state.calls.fetch_add(1, Ordering::SeqCst);
    let streaming = body["stream"] != json!(false);
    *state.last_body.lock().unwrap() = Some(body);
    match state.behaviour {
        Provider::Refusing => {
            return (StatusCode::TOO_MANY_REQUESTS, Json(json!({ "error": { "message": "slow down" } })))
                .into_response();
        }
        Provider::Hung => std::future::pending::<()>().await,
        Provider::Gated => state.gate.acquire().await.expect("gate").forget(),
        Provider::Healthy | Provider::DiesMidStream | Provider::HeldMidStream => {}
    }
    if !streaming {
        return completion_answer(state.behaviour);
    }
    if state.behaviour == Provider::HeldMidStream {
        let gate = Arc::clone(&state.gate);
        let frames = futures_util::stream::unfold(0u8, move |step| {
            let gate = Arc::clone(&gate);
            async move {
                let frame = match step {
                    0 => token("Hello"),
                    1 => {
                        gate.acquire().await.expect("gate").forget();
                        token(", world")
                    }
                    2 => sse(json!({ "id": "p1", "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] })),
                    3 => sse(json!({ "id": "p1", "choices": [],
                                    "usage": { "prompt_tokens": 12, "completion_tokens": 3, "total_tokens": 15 } })),
                    4 => "data: [DONE]\n\n".to_string(),
                    _ => return None,
                };
                Some((Ok::<_, std::io::Error>(frame), step + 1))
            }
        });
        let mut response = Response::new(Body::from_stream(frames));
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
        return response;
    }
    if state.behaviour == Provider::DiesMidStream {
        return sse_body(vec![
            Ok(token("partial ")),
            Ok(token("answer ")),
            Err(std::io::Error::other("provider connection lost")),
        ]);
    }
    sse_body(vec![
        Ok(token("Hello")),
        Ok(token(", ")),
        Ok(token("world")),
        Ok(sse(json!({ "id": "p1", "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }))),
        Ok(sse(json!({ "id": "p1", "choices": [],
                       "usage": { "prompt_tokens": 12, "completion_tokens": 3, "total_tokens": 15 } }))),
        Ok("data: [DONE]\n\n".to_string()),
    ])
}

/// The non-streaming answer: one JSON document with the same usage as the
/// streamed one, or, for `DiesMidStream`, half a document and a dead socket.
fn completion_answer(behaviour: Provider) -> Response {
    let whole = json!({
        "id": "p1", "model": "mock-model",
        "choices": [{ "index": 0, "finish_reason": "stop",
                      "message": { "role": "assistant", "content": "Hello, world" } }],
        "usage": { "prompt_tokens": 12, "completion_tokens": 3, "total_tokens": 15 }
    });
    if behaviour == Provider::DiesMidStream {
        let text = whole.to_string();
        let mut response = sse_body(vec![
            Ok(text[..text.len() / 2].to_string()),
            Err(std::io::Error::other("provider connection lost")),
        ]);
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));
        return response;
    }
    Json(whole).into_response()
}

// ---------------------------------------------------------------------------
// Deployment: config on disk, and gateway processes booted from it
// ---------------------------------------------------------------------------

/// Pricing on every mock model: $2/MTok in, $8/MTok out, i.e. 2,000 and
/// 8,000 nano-USD per token. A healthy answer (12 prompt + 3 completion
/// tokens) settles at exactly 48,000 nano-USD.
const HEALTHY_ANSWER_COST: u64 = 48_000;

fn models_yaml(provider: SocketAddr) -> String {
    let model = |id: &str, endpoint: &str, max_tokens: Option<u32>| {
        let defaults = max_tokens
            .map(|m| format!("    default_params:\n      max_tokens: {m}\n"))
            .unwrap_or_default();
        format!(
            "  {id}:\n    provider: nvidia\n    endpoint: {endpoint}\n    upstream_model: mock-model\n{defaults}    cost:\n      input_per_mtok_usd: 2.0\n      output_per_mtok_usd: 8.0\n"
        )
    };
    let live = format!("http://{provider}/v1/chat/completions");
    format!(
        "schema_version: 1\nmodels:\n{}{}{}{}{}",
        model("mock/chat", &live, Some(256)),
        model("mock/other", &live, Some(256)),
        model("mock/forbidden", &live, Some(256)),
        model("mock/uncapped", &live, None),
        // Nothing listens on the discard port: an unreachable provider.
        model("mock/down", "http://127.0.0.1:9/v1/chat/completions", Some(256)),
    )
}

/// The tenant roster. `include_rotating_key` is how a test revokes a key.
fn tenants_yaml(include_rotating_key: bool) -> String {
    let rotating = if include_rotating_key {
        "      - key_id: ak_rotating\n        key: key-rotating\n        scopes: [chat:stream]\n"
    } else {
        ""
    };
    let roomy = "      requests_per_minute: 10000\n      tokens_per_minute: 100000000\n      max_concurrent_streams: 256\n      max_output_tokens: 4096\n";
    format!(
        r#"tenants:
  - tenant_id: acme
    enabled: true
    credentials:
      - key_id: ak_admin
        key: key-admin
        scopes: [chat:stream, models:read, admin]
      - key_id: ak_app
        key: key-app
        scopes: [chat:stream]
      - key_id: ak_readonly
        key: key-readonly
        scopes: [models:read]
      - key_id: ak_switched_off
        key: key-switched-off
        scopes: [chat:stream]
        enabled: false
{rotating}    allowed_models: ["mock/*"]
    denied_models: ["mock/forbidden"]
    limits:
{roomy}      daily_budget_nano_usd: 100000000000
  - tenant_id: narrow
    enabled: true
    credentials:
      - key_id: ak_narrow
        key: key-narrow
        scopes: [chat:stream]
    allowed_models: ["mock/chat"]
    limits:
{roomy}      daily_budget_nano_usd: 100000000000
  - tenant_id: tight
    enabled: true
    credentials:
      - key_id: ak_tight
        key: key-tight
        scopes: [chat:stream]
    allowed_models: ["mock/*"]
    limits:
{roomy}      # Room for three standard reservations (~1.03M nano-USD each), never four.
      daily_budget_nano_usd: 3500000
  - tenant_id: oneshot
    enabled: true
    credentials:
      - key_id: ak_oneshot
        key: key-oneshot
        scopes: [chat:stream]
    allowed_models: ["mock/*"]
    limits:
{roomy}      # One small request (reserves 46,000, settles at 48,000) fits; then
      # 12,000 remain, less than the next reservation.
      daily_budget_nano_usd: 60000
  - tenant_id: bystander
    enabled: true
    credentials:
      - key_id: ak_bystander
        key: key-bystander
        scopes: [chat:stream]
    allowed_models: ["mock/*"]
    limits:
{roomy}      daily_budget_nano_usd: 100000000000
  - tenant_id: dormant
    enabled: false
    credentials:
      - key_id: ak_dormant
        key: key-dormant
        scopes: [chat:stream]
    allowed_models: ["mock/*"]
  - tenant_id: capped
    enabled: true
    credentials:
      - key_id: ak_capped
        key: key-capped
        scopes: [chat:stream]
    allowed_models: ["mock/*"]
    limits:
      requests_per_minute: 10000
      tokens_per_minute: 100000000
      max_concurrent_streams: 3
      max_output_tokens: 4096
      daily_budget_nano_usd: 100000000000
  - tenant_id: paced
    enabled: true
    credentials:
      - key_id: ak_paced
        key: key-paced
        scopes: [chat:stream]
    allowed_models: ["mock/*"]
    limits:
      requests_per_minute: 6
      tokens_per_minute: 100000000
      max_concurrent_streams: 256
      max_output_tokens: 4096
      daily_budget_nano_usd: 100000000000
  - tenant_id: strict
    enabled: true
    require_idempotency_key: true
    credentials:
      - key_id: ak_strict
        key: key-strict
        scopes: [chat:stream]
    allowed_models: ["mock/*"]
    limits:
{roomy}      daily_budget_nano_usd: 100000000000
  # HOLD: `mock/other` always waits for approval, and so does anything whose
  # worst case is above 1,000,000 nano-USD (a standard request, ~1.03M).
  - tenant_id: guarded
    enabled: true
    credentials:
      - key_id: ak_guarded
        key: key-guarded
        scopes: [chat:stream]
      # Can approve holds, but never its own.
      - key_id: ak_guarded_lead
        key: key-guarded-lead
        scopes: [chat:stream, approve:holds]
    allowed_models: ["mock/*"]
    limits:
{roomy}      daily_budget_nano_usd: 100000000000
    holds:
      models: ["mock/other"]
      above_nano_usd: 1000000
  # Holds that lapse after three seconds, approvals after one.
  - tenant_id: hasty
    enabled: true
    credentials:
      - key_id: ak_hasty
        key: key-hasty
        scopes: [chat:stream]
    allowed_models: ["mock/*"]
    limits:
{roomy}      daily_budget_nano_usd: 100000000000
    holds:
      models: ["mock/*"]
      expiry_secs: 3
      approval_valid_secs: 1
  # At most two holds waiting at once.
  - tenant_id: queued
    enabled: true
    credentials:
      - key_id: ak_queued
        key: key-queued
        scopes: [chat:stream]
    allowed_models: ["mock/*"]
    limits:
{roomy}      daily_budget_nano_usd: 100000000000
    holds:
      models: ["mock/*"]
      max_pending: 2
  # Everything is held, and the budget is too small for a standard request.
  - tenant_id: thrifty
    enabled: true
    credentials:
      - key_id: ak_thrifty
        key: key-thrifty
        scopes: [chat:stream]
    allowed_models: ["mock/*"]
    limits:
{roomy}      daily_budget_nano_usd: 60000
    holds:
      models: ["mock/*"]
  # The approvers: a key that can decide holds and do nothing else.
  - tenant_id: ops
    enabled: true
    credentials:
      - key_id: ak_approver
        key: key-approver
        scopes: [approve:holds]
    allowed_models: ["mock/chat"]
"#
    )
}

/// Which ledger a deployment's gateways use.
#[derive(Clone)]
enum LedgerChoice {
    /// Each gateway process has its own, in memory.
    Memory,
    /// Every gateway process shares one Postgres ledger: a schema of its own
    /// per deployment, so parallel tests never share rows. `sealed` says
    /// whether stored answers are encrypted (and therefore stored at all).
    Shared { url: String, schema: String, sealed: bool },
}

/// The test database, or `None` to skip. CI sets
/// `LLM_GATEWAY_REQUIRE_TEST_DATABASE`, which turns a missing database into a
/// failure rather than a silent pass.
fn test_database_url() -> Option<String> {
    match std::env::var("LLM_GATEWAY_TEST_DATABASE_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            let required = std::env::var("LLM_GATEWAY_REQUIRE_TEST_DATABASE").is_ok_and(|v| !v.is_empty());
            assert!(!required, "LLM_GATEWAY_TEST_DATABASE_URL is required but not set");
            eprintln!("skipped: set LLM_GATEWAY_TEST_DATABASE_URL to run the shared-ledger tests");
            None
        }
    }
}

/// Test-only answer-sealing key: 32 bytes, base64.
const TEST_RESPONSE_KEYS: &str = "test-1:AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

async fn postgres_ledger(
    url: &str,
    schema: &str,
    sealed: bool,
) -> Result<Arc<llm_gateway::governance::ledger::PostgresLedger>, String> {
    let sealer = sealed.then(|| {
        Arc::new(llm_gateway::governance::ledger::sealed::ResponseSealer::from_keys(TEST_RESPONSE_KEYS).unwrap())
    });
    llm_gateway::governance::ledger::PostgresLedger::connect(
        llm_gateway::governance::ledger::postgres::PostgresOptions {
            url: url.to_string(),
            schema: schema.to_string(),
            max_connections: 8,
            // A loaded machine's deadline: see `local_ledger`.
            timeout: Duration::from_millis(5000),
            lease: Duration::from_secs(60),
            sweep_interval: Duration::from_secs(3_600),
            idempotency_retention: Duration::from_secs(86_400),
            sealer,
            decision_retention: Duration::from_secs(30 * 86_400),
        },
    )
    .await
}

/// Config files on disk, shared by every gateway process booted from them,
/// the way replicas share a config volume.
struct Deployment {
    dir: Arc<tempdir::TempDir>,
    ledger: LedgerChoice,
}

impl Deployment {
    fn new(provider: &MockProvider) -> Self {
        captured_logs::install();
        let dir = Arc::new(tempdir::TempDir::new("llm-gateway-pressure"));
        std::fs::write(dir.path().join("models.yaml"), models_yaml(provider.addr)).unwrap();
        std::fs::write(dir.path().join("tenants.yaml"), tenants_yaml(true)).unwrap();
        Self { dir, ledger: LedgerChoice::Memory }
    }

    /// A deployment whose gateways share one Postgres ledger at `url`.
    fn shared(provider: &MockProvider, url: &str) -> Self {
        Self::shared_with(provider, url, true)
    }

    fn shared_with(provider: &MockProvider, url: &str, sealed: bool) -> Self {
        let schema = format!("pressure_{}", uuid::Uuid::new_v4().simple());
        Self {
            ledger: LedgerChoice::Shared { url: url.to_string(), schema, sealed },
            ..Self::new(provider)
        }
    }

    /// Direct access to the shared ledger's tables, for inspecting what is
    /// actually at rest.
    async fn ledger_pool(&self) -> sqlx::PgPool {
        let LedgerChoice::Shared { url, schema, .. } = &self.ledger else {
            panic!("not a shared-ledger deployment");
        };
        postgres_ledger(url, schema, false).await.expect("connect ledger").pool().clone()
    }

    fn rewrite_tenants(&self, yaml: &str) {
        std::fs::write(self.dir.path().join("tenants.yaml"), yaml).unwrap();
    }

    /// Boot one gateway process. All governance state lives in the process,
    /// so booting again from the same files is exactly a restart, and two
    /// boots are exactly two replicas.
    async fn boot(&self) -> Gateway {
        self.boot_with(|_| {}).await
    }

    async fn boot_with(&self, tweak: impl FnOnce(&mut llm_gateway::config::Settings)) -> Gateway {
        self.try_boot_with(tweak).await.expect("boot gateway")
    }

    /// Boot, or report why the gateway refused to start.
    async fn try_boot_with(
        &self,
        tweak: impl FnOnce(&mut llm_gateway::config::Settings),
    ) -> Result<Gateway, String> {
        let mut settings = llm_gateway::config::Settings {
            server: llm_gateway::config::ServerConfig {
                bind_addr: "127.0.0.1".into(),
                port: 0,
                metrics_enabled: true,
                ..Default::default()
            },
            registry: llm_gateway::config::RegistryConfig {
                models_path: self.dir.path().join("models.yaml"),
                tenants_path: self.dir.path().join("tenants.yaml"),
                hot_reload: false,
                reload_interval_ms: 60_000,
            },
            auth: llm_gateway::config::AuthConfig {
                allow_anonymous: false,
                ..Default::default()
            },
            // Explicitly the in-memory ledger: the default is a SQLite file,
            // and parallel tests must not share one.
            ledger: llm_gateway::config::LedgerConfig {
                backend: llm_gateway::config::LedgerBackend::Memory,
                ..Default::default()
            },
            ..Default::default()
        };
        tweak(&mut settings);
        let state = match &self.ledger {
            LedgerChoice::Memory => llm_gateway::bootstrap::build(settings).await.map_err(|e| e.to_string())?,
            // A fresh ledger client per boot, exactly as a separate process
            // would have: nothing is shared but the database.
            LedgerChoice::Shared { url, schema, sealed } => {
                let ledger = postgres_ledger(url, schema, *sealed).await?;
                llm_gateway::bootstrap::build_with_ledger(settings, ledger)
                    .await
                    .map_err(|e| e.to_string())?
            }
        };
        let app = llm_gateway::api::router(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind gateway");
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        Ok(Gateway { addr, task, _config: Arc::clone(&self.dir) })
    }

    /// Settings for a gateway on its own SQLite ledger file in this
    /// deployment's directory, optionally as one of `split` replicas.
    fn local_ledger(&self, file: &str, split: Option<u32>) -> impl FnOnce(&mut llm_gateway::config::Settings) {
        let path = self.dir.path().join(file);
        move |s| {
            s.ledger.backend = llm_gateway::config::LedgerBackend::Sqlite;
            s.ledger.sqlite_path = path;
            // The suite runs dozens of gateways in parallel on one disk, each
            // syncing every commit. These tests assert admission outcomes, not
            // latency, so they give the ledger a loaded machine's deadline.
            s.ledger.timeout_ms = 5_000;
            // No sealing key in these tests: answers are simply not stored.
            s.ledger.response_keys_env = "LLM_GATEWAY_PRESSURE_NO_KEYS".into();
            if let Some(replicas) = split {
                s.ledger.quota_split_replicas = replicas;
            }
        }
    }
}

struct Gateway {
    addr: SocketAddr,
    task: JoinHandle<()>,
    /// A running process keeps its config files, as on a real host.
    _config: Arc<tempdir::TempDir>,
}

impl Drop for Gateway {
    /// Dropping the gateway kills the process: the listener stops, and its
    /// in-memory state goes with it.
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: String,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("not JSON ({e}): {}", self.body))
    }

    fn code(&self) -> String {
        self.json()["error"]["code"].as_str().unwrap_or_default().to_string()
    }
}

impl Gateway {
    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Reply {
        let response = request.send().await.expect("gateway reachable");
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.text().await.unwrap_or_default();
        Reply { status, headers, body }
    }

    async fn chat(&self, key: Option<&str>, body: Value) -> Reply {
        let mut request = reqwest::Client::new().post(self.url("/v1/chat/stream")).json(&body);
        if let Some(key) = key {
            request = request.header("x-api-key", key);
        }
        self.send(request).await
    }

    /// The second door: `POST /v1/chat/complete`.
    async fn complete(&self, key: Option<&str>, body: Value) -> Reply {
        let mut request = reqwest::Client::new().post(self.url("/v1/chat/complete")).json(&body);
        if let Some(key) = key {
            request = request.header("x-api-key", key);
        }
        self.send(request).await
    }

    async fn get(&self, path: &str, key: &str) -> Reply {
        self.send(reqwest::Client::new().get(self.url(path)).header("x-api-key", key)).await
    }

    async fn spent(&self, key: &str) -> u64 {
        let reply = self.get("/v1/usage", key).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        reply.json()["spent_nano_usd"].as_u64().unwrap()
    }

    async fn in_flight(&self, key: &str) -> u64 {
        self.get("/v1/usage", key).await.json()["streams_in_flight"].as_u64().unwrap()
    }
}

fn standard_request() -> Value {
    json!({
        "model": "mock/chat",
        "messages": [{ "role": "user", "content": "hi" }],
        "params": { "max_tokens": 128 }
    })
}

/// Reserves 3 x 2,000 + 5 x 8,000 = 46,000 nano-USD, settles at 48,000.
fn small_request() -> Value {
    json!({
        "model": "mock/chat",
        "messages": [{ "role": "user", "content": "hi" }],
        "params": { "max_tokens": 5 }
    })
}

/// Wait until `condition` holds, or fail the test with `what`.
async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ===========================================================================
// 1. Identity: who is asking?
// ===========================================================================

#[tokio::test]
async fn identity_no_credential_never_reaches_the_provider() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let reply = gw.chat(None, standard_request()).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_eq!(reply.code(), "unauthorized");
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn identity_a_forged_key_never_reaches_the_provider() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    for forged in ["key-admin-", "KEY-ADMIN", "key-admi", "", " "] {
        let reply = gw.chat(Some(forged), standard_request()).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "x-api-key {forged:?}");
    }
    for bearer in ["Bearer forged", "Bearer ", "Basic a2V5LWFkbWlu", "key-admin"] {
        let request = reqwest::Client::new()
            .post(gw.url("/v1/chat/stream"))
            .header("authorization", bearer)
            .json(&standard_request());
        assert_eq!(gw.send(request).await.status, StatusCode::UNAUTHORIZED, "authorization {bearer:?}");
    }
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn identity_a_switched_off_credential_never_reaches_the_provider() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let reply = gw.chat(Some("key-switched-off"), standard_request()).await;
    assert!(
        matches!(reply.status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN),
        "{} {}",
        reply.status,
        reply.body
    );
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn identity_a_revoked_key_stops_at_the_next_request() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    let gw = deployment.boot().await;

    assert_eq!(gw.chat(Some("key-rotating"), standard_request()).await.status, StatusCode::OK);
    assert_eq!(provider.calls(), 1);

    deployment.rewrite_tenants(&tenants_yaml(false));
    let reload = gw
        .send(reqwest::Client::new().post(gw.url("/v1/admin/registry/reload")).header("x-api-key", "key-admin"))
        .await;
    assert_eq!(reload.status, StatusCode::OK, "{}", reload.body);

    let reply = gw.chat(Some("key-rotating"), standard_request()).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_eq!(provider.calls(), 1, "the revoked key's request never reached the provider");
}

#[tokio::test]
async fn identity_a_disabled_tenant_never_reaches_the_provider() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let reply = gw.chat(Some("key-dormant"), standard_request()).await;
    assert!(
        matches!(reply.status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN),
        "{} {}",
        reply.status,
        reply.body
    );
    assert_eq!(provider.calls(), 0);
}

// ===========================================================================
// 2. Authority: are they allowed?
// ===========================================================================

#[tokio::test]
async fn authority_a_key_without_the_spend_scope_never_reaches_the_provider() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let reply = gw.chat(Some("key-readonly"), standard_request()).await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn authority_operator_actions_need_the_admin_scope() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let reload = |key: &'static str| {
        reqwest::Client::new().post(gw.url("/v1/admin/registry/reload")).header("x-api-key", key)
    };
    assert_eq!(gw.send(reload("key-app")).await.status, StatusCode::FORBIDDEN);
    assert_eq!(gw.get("/metrics", "key-app").await.status, StatusCode::FORBIDDEN);
    let admin = gw.send(reload("key-admin")).await;
    assert_eq!(admin.status, StatusCode::OK, "{}", admin.body);
    assert_eq!(gw.get("/metrics", "key-admin").await.status, StatusCode::OK);
}

#[tokio::test]
async fn authority_a_tenant_sees_only_its_own_spend() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    assert_eq!(gw.chat(Some("key-bystander"), standard_request()).await.status, StatusCode::OK);
    // There is no way to name another tenant: the query is ignored, and the
    // answer is always the caller's own ledger.
    let reply = gw.get("/v1/usage?tenant=bystander&tenant_id=bystander", "key-app").await;
    assert_eq!(reply.json()["tenant_id"], "acme");
    assert_eq!(reply.json()["spent_nano_usd"], 0);
}

// ===========================================================================
// 3. Model access: which provider and model can they use?
// ===========================================================================

#[tokio::test]
async fn model_outside_the_allowlist_never_reaches_the_provider() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let mut body = standard_request();
    body["model"] = json!("mock/other");
    let reply = gw.chat(Some("key-narrow"), body).await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert_eq!(reply.code(), "model_not_allowed");
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn model_denied_under_a_wildcard_never_reaches_the_provider() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let mut body = standard_request();
    body["model"] = json!("mock/forbidden");
    let reply = gw.chat(Some("key-app"), body).await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN, "deny beats `mock/*`: {}", reply.body);
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn model_unknown_never_reaches_the_provider() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    for model in ["mock/nope", "other-vendor/gpt", "mock/*", "*"] {
        let mut body = standard_request();
        body["model"] = json!(model);
        let reply = gw.chat(Some("key-app"), body).await;
        assert!(reply.status.is_client_error(), "{model}: {} {}", reply.status, reply.body);
    }
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn model_and_transport_cannot_be_redirected_through_parameters() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let mut body = standard_request();
    body["params"] = json!({
        "max_tokens": 32,
        "model": "attacker/expensive-model",
        "stream": false,
    });
    assert_eq!(gw.chat(Some("key-app"), body).await.status, StatusCode::OK);
    let sent = provider.last_body();
    assert_eq!(sent["model"], "mock-model", "the registry decides the upstream model");
    assert_eq!(sent["stream"], true, "the gateway decides the transport");
}

// ===========================================================================
// 4. Budget and pre-flight cost
// ===========================================================================

#[tokio::test]
async fn budget_worst_case_exposure_over_budget_is_refused_up_front() {
    // The answer would actually cost 48,000 nano-USD, well inside the
    // budget. The worst case the caller asked for (4,000 output tokens,
    // 32M nano-USD) is not, so it is refused before execution.
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let mut body = standard_request();
    body["params"] = json!({ "max_tokens": 4000 });
    let reply = gw.chat(Some("key-tight"), body).await;
    assert_eq!(reply.status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(reply.code(), "budget_exhausted");
    assert_eq!(provider.calls(), 0);
    assert_eq!(gw.spent("key-tight").await, 0, "a refusal costs nothing");
}

#[tokio::test]
async fn budget_an_uncapped_request_is_refused() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let body = json!({ "model": "mock/uncapped", "messages": [{ "role": "user", "content": "hi" }] });
    let reply = gw.chat(Some("key-app"), body).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(reply.code(), "max_tokens_required");
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn budget_the_output_ceiling_is_what_the_provider_receives() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let mut body = standard_request();
    body["params"] = json!({ "max_tokens": 1_000_000 });
    assert_eq!(gw.chat(Some("key-app"), body).await.status, StatusCode::OK);
    assert_eq!(provider.last_body()["max_tokens"], 4096, "clamped to the tenant ceiling");
}

#[tokio::test]
async fn budget_a_concurrent_burst_admits_exactly_what_fits() {
    // Twenty simultaneous requests against a budget with room for three.
    // The provider holds every admitted request open, so all reservations
    // are live at once and none can settle early and free budget.
    let provider = MockProvider::start(Provider::Gated).await;
    let gw = Arc::new(Deployment::new(&provider).boot().await);

    let refused = Arc::new(AtomicU64::new(0));
    let burst: Vec<_> = (0..20)
        .map(|_| {
            let (gw, refused) = (Arc::clone(&gw), Arc::clone(&refused));
            tokio::spawn(async move {
                let reply = gw.chat(Some("key-tight"), standard_request()).await;
                if reply.status == StatusCode::PAYMENT_REQUIRED {
                    refused.fetch_add(1, Ordering::SeqCst);
                }
                reply.status
            })
        })
        .collect();

    eventually("every request admitted or refused", || {
        provider.calls() + refused.load(Ordering::SeqCst) == 20
    })
    .await;
    assert_eq!(provider.calls(), 3, "exactly three reservations fit");
    provider.open_gate(20);
    let statuses: Vec<StatusCode> = futures_util::future::join_all(burst)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert_eq!(statuses.iter().filter(|s| **s == StatusCode::OK).count(), 3);
    assert_eq!(refused.load(Ordering::SeqCst), 17);
}

#[tokio::test]
async fn budget_one_tenants_exhaustion_does_not_touch_another() {
    let provider = MockProvider::start(Provider::Gated).await;
    let gw = Arc::new(Deployment::new(&provider).boot().await);

    // Pin `tight` at its ceiling with three held reservations.
    let held: Vec<_> = (0..3)
        .map(|_| {
            let gw = Arc::clone(&gw);
            tokio::spawn(async move { gw.chat(Some("key-tight"), standard_request()).await.status })
        })
        .collect();
    eventually("tight at its ceiling", || provider.calls() == 3).await;
    assert_eq!(gw.chat(Some("key-tight"), standard_request()).await.status, StatusCode::PAYMENT_REQUIRED);

    let bystander = {
        let gw = Arc::clone(&gw);
        tokio::spawn(async move { gw.chat(Some("key-bystander"), standard_request()).await.status })
    };
    eventually("the bystander reaches the provider", || provider.calls() == 4).await;
    provider.open_gate(10);
    assert_eq!(bystander.await.unwrap(), StatusCode::OK);
    for h in held {
        assert_eq!(h.await.unwrap(), StatusCode::OK);
    }
}

// ===========================================================================
// 5. Failure: the boundary under a failing provider or client
// ===========================================================================

#[tokio::test]
async fn failure_an_unreachable_provider_costs_nothing_and_frees_the_slot() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let mut body = standard_request();
    body["model"] = json!("mock/down");
    let reply = gw.chat(Some("key-app"), body).await;
    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
    assert_eq!(gw.spent("key-app").await, 0);
    assert_eq!(gw.in_flight("key-app").await, 0);
}

#[tokio::test]
async fn failure_a_refusing_provider_costs_nothing() {
    let provider = MockProvider::start(Provider::Refusing).await;
    let gw = Deployment::new(&provider).boot().await;

    let reply = gw.chat(Some("key-app"), standard_request()).await;
    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
    assert_eq!(reply.headers["x-error-retryable"], "true");
    assert_eq!(gw.spent("key-app").await, 0);
    assert_eq!(gw.in_flight("key-app").await, 0);
}

#[tokio::test]
async fn failure_a_hung_provider_cannot_pin_budget_or_slots() {
    // The provider accepts the connection and never answers. The read
    // timeout, not the client's patience, bounds how long the reservation
    // and the concurrency slot are held.
    let provider = MockProvider::start(Provider::Hung).await;
    let gw = Deployment::new(&provider)
        .boot_with(|s| s.upstream.stream_read_timeout_ms = 300)
        .await;

    let started = std::time::Instant::now();
    let reply = tokio::time::timeout(Duration::from_secs(10), gw.chat(Some("key-app"), standard_request()))
        .await
        .expect("the gateway must give up on a hung provider");
    assert_eq!(reply.status, StatusCode::BAD_GATEWAY, "{}", reply.body);
    assert!(started.elapsed() < Duration::from_secs(5), "took {:?}", started.elapsed());
    assert_eq!(provider.calls(), 1);
    assert_eq!(gw.spent("key-app").await, 0, "no answer, no charge");
    assert_eq!(gw.in_flight("key-app").await, 0);
}

#[tokio::test]
async fn failure_a_provider_dying_mid_answer_bills_only_what_was_delivered() {
    let provider = MockProvider::start(Provider::DiesMidStream).await;
    let gw = Deployment::new(&provider).boot().await;

    let reply = gw.chat(Some("key-app"), standard_request()).await;
    assert_eq!(reply.status, StatusCode::OK, "headers were already sent");
    assert!(reply.body.contains("event: error"), "the failure is reported in-band: {}", reply.body);
    assert!(reply.body.contains("data: [DONE]"), "and the stream still terminates");
    let reserved: u64 = reply.headers["x-reserved-nano-usd"].to_str().unwrap().parse().unwrap();
    let spent = gw.spent("key-app").await;
    assert!(spent > 0 && spent < reserved, "delivered output billed, rest refunded: {spent} of {reserved}");
    assert_eq!(gw.in_flight("key-app").await, 0);
}

#[tokio::test]
async fn failure_a_disconnect_storm_leaks_no_slots() {
    let provider = MockProvider::start(Provider::Gated).await;
    let gw = Deployment::new(&provider).boot().await;

    let impatient = reqwest::Client::builder().timeout(Duration::from_millis(150)).build().unwrap();
    let storm: Vec<_> = (0..100)
        .map(|_| {
            impatient
                .post(gw.url("/v1/chat/stream"))
                .header("x-api-key", "key-app")
                .json(&standard_request())
                .send()
        })
        .collect();
    for result in futures_util::future::join_all(storm).await {
        assert!(result.is_err(), "every client gave up");
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(gw.in_flight("key-app").await, 0, "no slot outlives its client");
    provider.open_gate(1);
    assert_eq!(gw.chat(Some("key-app"), standard_request()).await.status, StatusCode::OK);
}

#[tokio::test]
async fn failure_oversized_input_is_refused_before_it_is_read() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let status_line = |head: String| async move {
        let mut sock = tokio::net::TcpStream::connect(gw.addr).await.unwrap();
        sock.write_all(head.as_bytes()).await.unwrap();
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut buf))
            .await
            .expect("answered without waiting for a body")
            .unwrap_or(0);
        String::from_utf8_lossy(&buf[..n]).to_string()
    };

    let body = status_line(format!(
        "POST /v1/chat/stream HTTP/1.1\r\nhost: x\r\nx-api-key: key-app\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
        64 * 1024 * 1024
    ))
    .await;
    assert!(body.starts_with("HTTP/1.1 413"), "{body:?}");

    let mut flood = "POST /v1/chat/stream HTTP/1.1\r\nhost: x\r\n".to_string();
    for i in 0..500 {
        flood.push_str(&format!("x-flood-{i}: {i}\r\n"));
    }
    flood.push_str("\r\n");
    let headers = status_line(flood).await;
    assert!(headers.starts_with("HTTP/1.1 431"), "{headers:?}");
    assert_eq!(provider.calls(), 0);
}

// ===========================================================================
// 6. Accountability: what was refused, and why?
// ===========================================================================

#[tokio::test]
async fn ledger_every_refusal_is_recorded_with_request_tenant_and_reason() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let mut body = standard_request();
    body["params"] = json!({ "max_tokens": 4000 });
    let refused = gw.chat(Some("key-tight"), body).await;
    assert_eq!(refused.status, StatusCode::PAYMENT_REQUIRED);
    let request_id = refused.json()["request_id"].as_str().unwrap().to_string();

    let logs = captured_logs::contents();
    let line = logs
        .lines()
        .find(|l| l.contains(&request_id))
        .unwrap_or_else(|| panic!("no log line for refused request {request_id}"));
    let entry: Value = serde_json::from_str(line).expect("structured log line");
    assert_eq!(entry["tenant"], "tight");
    assert_eq!(entry["stage"], "budget_exhausted");
    assert!(entry["reason"].as_str().unwrap().contains("nano-USD"), "{entry}");
    assert_eq!(entry["level"], "WARN");
}

/// The decision record for one request, as an admin reads it.
async fn decision_for(gw: &Gateway, request_id: &str) -> Option<Value> {
    let reply = gw.get("/v1/admin/decisions?limit=1000", "key-admin").await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    reply.json()["decisions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["request_id"] == request_id)
        .cloned()
}

/// A budget refusal from `tight`, returning its request id.
async fn refuse_on_budget(gw: &Gateway, prompt: &str) -> String {
    let mut body = standard_request();
    body["messages"] = json!([{ "role": "user", "content": prompt }]);
    body["params"] = json!({ "max_tokens": 4000 });
    let reply = gw.chat(Some("key-tight"), body).await;
    assert_eq!(reply.status, StatusCode::PAYMENT_REQUIRED);
    reply.json()["request_id"].as_str().unwrap().to_string()
}

async fn eventually_recorded(gw: &Gateway, request_id: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(record) = decision_for(gw, request_id).await {
            return record;
        }
        assert!(tokio::time::Instant::now() < deadline, "{request_id} was never recorded");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn ledger_a_refusal_is_on_the_decision_record() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    for gw in [
        deployment.boot().await,
        deployment.boot_with(deployment.local_ledger("decisions.sqlite3", None)).await,
    ] {
        let request_id = refuse_on_budget(&gw, "hi").await;
        let record = eventually_recorded(&gw, &request_id).await;
        assert_eq!(record["tenant_id"], "tight");
        assert_eq!(record["key_id"], "ak_tight");
        assert_eq!(record["kind"], "refused");
        assert_eq!(record["code"], "budget_exhausted");
        assert_eq!(record["endpoint"], "stream");
        assert_eq!(record["model"], "mock/chat");
        assert!(record["reason"].as_str().unwrap().contains("nano-USD"));
    }
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn ledger_anonymous_refusals_are_never_recorded() {
    // Persisting refusals of unauthenticated callers would let anyone write
    // to the database.
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let reply = gw.chat(Some("forged-key"), standard_request()).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    let request_id = reply.json()["request_id"].as_str().unwrap().to_string();
    // A later, authenticated refusal is recorded, so the record is working.
    let later = refuse_on_budget(&gw, "hi").await;
    eventually_recorded(&gw, &later).await;
    assert!(decision_for(&gw, &request_id).await.is_none(), "an anonymous refusal was persisted");
}

#[tokio::test]
async fn ledger_the_decision_record_holds_no_prompt_text() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let marker = format!("prompt-marker-{}", uuid::Uuid::new_v4().simple());

    let refused = refuse_on_budget(&gw, &marker).await;
    // A body the parser rejects while quoting the offending value.
    let malformed = gw
        .chat(Some("key-app"), json!({ "model": "mock/chat", "messages": marker, "params": { "max_tokens": 5 } }))
        .await;
    assert_eq!(malformed.status, StatusCode::BAD_REQUEST);
    let malformed_id = malformed.json()["request_id"].as_str().unwrap().to_string();

    eventually_recorded(&gw, &refused).await;
    let record = eventually_recorded(&gw, &malformed_id).await;
    assert_eq!(record["reason"], "request body is not valid JSON");
    let everything = gw.get("/v1/admin/decisions?limit=1000", "key-admin").await.body;
    assert!(!everything.contains(&marker), "prompt text reached the decision record");
}

#[tokio::test]
async fn ledger_decisions_survive_a_restart() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    let first = deployment.boot_with(deployment.local_ledger("decisions.sqlite3", None)).await;
    let request_id = refuse_on_budget(&first, "hi").await;
    eventually_recorded(&first, &request_id).await;
    drop(first);

    let restarted = deployment.boot_with(deployment.local_ledger("decisions.sqlite3", None)).await;
    assert!(decision_for(&restarted, &request_id).await.is_some(), "the record outlived the process");
}

#[tokio::test]
async fn ledger_an_operator_action_is_recorded() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let reload = gw
        .send(reqwest::Client::new().post(gw.url("/v1/admin/registry/reload")).header("x-api-key", "key-admin"))
        .await;
    assert_eq!(reload.status, StatusCode::OK);
    let all = gw.get("/v1/admin/decisions", "key-admin").await.json();
    let action = all["decisions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["code"] == "registry_reload")
        .cloned()
        .expect("the reload is on the record");
    assert_eq!(action["kind"], "admin_action");
    assert_eq!(action["key_id"], "ak_admin");
    assert!(gw.get("/metrics", "key-admin").await.body.contains("gw_decisions_dropped_total 0"));
    assert_eq!(gw.get("/v1/admin/decisions", "key-app").await.status, StatusCode::FORBIDDEN);
}

// ===========================================================================
// 7. The second door: POST /v1/chat/complete
// ===========================================================================
//
// The non-streaming endpoint shares the governance code with the stream
// endpoint. These tests check that it cannot be used to get around the
// boundary: every refusal still leaves the provider untouched, and both
// doors draw on one budget.

#[tokio::test]
async fn second_door_identity_and_authority_hold() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    for (key, expected) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some("key-admin-"), StatusCode::UNAUTHORIZED),
        (Some("key-switched-off"), StatusCode::UNAUTHORIZED),
        (Some("key-readonly"), StatusCode::FORBIDDEN),
        (Some("key-dormant"), StatusCode::FORBIDDEN),
    ] {
        let reply = gw.complete(key, standard_request()).await;
        assert_eq!(reply.status, expected, "{key:?}: {}", reply.body);
    }
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn second_door_model_access_holds() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    for (key, model) in [
        ("key-narrow", "mock/other"),
        ("key-app", "mock/forbidden"),
        ("key-app", "mock/nope"),
    ] {
        let mut body = standard_request();
        body["model"] = json!(model);
        let reply = gw.complete(Some(key), body).await;
        assert!(reply.status.is_client_error(), "{key} {model}: {}", reply.body);
    }
    assert_eq!(provider.calls(), 0);

    let mut body = standard_request();
    body["params"] = json!({ "max_tokens": 32, "model": "attacker/expensive-model", "stream": true });
    assert_eq!(gw.complete(Some("key-app"), body).await.status, StatusCode::OK);
    let sent = provider.last_body();
    assert_eq!(sent["model"], "mock-model");
    assert_eq!(sent["stream"], false, "the caller cannot turn a completion into a stream");
}

#[tokio::test]
async fn second_door_worst_case_exposure_is_refused_up_front() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let mut body = standard_request();
    body["params"] = json!({ "max_tokens": 4000 });
    let reply = gw.complete(Some("key-tight"), body).await;
    assert_eq!(reply.status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(provider.calls(), 0);
    let body = json!({ "model": "mock/uncapped", "messages": [{ "role": "user", "content": "hi" }] });
    assert_eq!(gw.complete(Some("key-app"), body).await.status, StatusCode::BAD_REQUEST);
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn second_door_both_doors_draw_on_one_budget() {
    // Twenty simultaneous requests, alternating between the two doors,
    // against a budget with room for three. If the doors kept separate
    // books, six would get through.
    let provider = MockProvider::start(Provider::Gated).await;
    let gw = Arc::new(Deployment::new(&provider).boot().await);

    let refused = Arc::new(AtomicU64::new(0));
    let burst: Vec<_> = (0..20)
        .map(|i| {
            let (gw, refused) = (Arc::clone(&gw), Arc::clone(&refused));
            tokio::spawn(async move {
                let reply = if i % 2 == 0 {
                    gw.chat(Some("key-tight"), standard_request()).await
                } else {
                    gw.complete(Some("key-tight"), standard_request()).await
                };
                if reply.status == StatusCode::PAYMENT_REQUIRED {
                    refused.fetch_add(1, Ordering::SeqCst);
                }
                reply.status
            })
        })
        .collect();

    eventually("every request admitted or refused", || {
        provider.calls() + refused.load(Ordering::SeqCst) == 20
    })
    .await;
    assert_eq!(provider.calls(), 3, "one budget across both doors");
    provider.open_gate(20);
    let statuses = futures_util::future::join_all(burst).await;
    assert_eq!(statuses.into_iter().filter(|s| *s.as_ref().unwrap() == StatusCode::OK).count(), 3);
}

#[tokio::test]
async fn second_door_a_hung_provider_cannot_pin_budget_or_slots() {
    let provider = MockProvider::start(Provider::Hung).await;
    let gw = Deployment::new(&provider)
        .boot_with(|s| s.upstream.stream_read_timeout_ms = 300)
        .await;

    let reply = tokio::time::timeout(Duration::from_secs(10), gw.complete(Some("key-app"), standard_request()))
        .await
        .expect("the gateway must give up on a hung provider");
    assert_eq!(reply.status, StatusCode::BAD_GATEWAY, "{}", reply.body);
    assert_eq!(gw.spent("key-app").await, 0, "no answer, no charge");
    assert_eq!(gw.in_flight("key-app").await, 0);
}

#[tokio::test]
async fn second_door_a_provider_dying_mid_answer_bills_the_prompt_only() {
    let provider = MockProvider::start(Provider::DiesMidStream).await;
    let gw = Deployment::new(&provider).boot().await;

    let reply = gw.complete(Some("key-app"), standard_request()).await;
    assert_eq!(reply.status, StatusCode::BAD_GATEWAY, "{}", reply.body);
    let spent = gw.spent("key-app").await;
    // Accepted, then the answer never arrived: the prompt was consumed, the
    // 128-token output reservation (1,024,000 nano-USD) is refunded.
    assert!(spent > 0 && spent < 128 * 8_000, "prompt only: {spent}");
    assert_eq!(gw.in_flight("key-app").await, 0);
}

#[tokio::test]
async fn second_door_a_disconnect_storm_leaks_no_slots_and_bills_each_reservation() {
    // A client that gives up on a completion is billed its reservation: the
    // provider will finish and bill the answer anyway, unseen.
    let provider = MockProvider::start(Provider::Gated).await;
    let gw = Deployment::new(&provider).boot().await;

    let impatient = reqwest::Client::builder().timeout(Duration::from_millis(150)).build().unwrap();
    let storm: Vec<_> = (0..20)
        .map(|_| {
            impatient
                .post(gw.url("/v1/chat/complete"))
                .header("x-api-key", "key-app")
                .json(&small_request())
                .send()
        })
        .collect();
    for result in futures_util::future::join_all(storm).await {
        assert!(result.is_err(), "every client gave up");
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(gw.in_flight("key-app").await, 0, "no slot outlives its client");
    // small_request reserves 3 x 2,000 + 5 x 8,000 = 46,000 nano-USD.
    assert_eq!(gw.spent("key-app").await, 20 * 46_000);
}

// ===========================================================================
// 8. Replays: one execution and one charge per Idempotency-Key
// ===========================================================================

fn with_key(request: reqwest::RequestBuilder, key: &str) -> reqwest::RequestBuilder {
    request.header("x-api-key", "key-app").header("idempotency-key", key)
}

#[tokio::test]
async fn replay_a_repeated_stream_is_recognised_not_executed() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let send = || gw.send(with_key(reqwest::Client::new().post(gw.url("/v1/chat/stream")), "order-1").json(&standard_request()));

    let first = send().await;
    assert_eq!(first.status, StatusCode::OK);
    let second = send().await;
    assert_eq!(second.status, StatusCode::CONFLICT, "{}", second.body);
    assert_eq!(second.code(), "duplicate_request");
    assert_eq!(second.json()["billed_nano_usd"], HEALTHY_ANSWER_COST);
    assert!(second.json()["original_request_id"].is_string());
    assert_eq!(provider.calls(), 1, "executed once");
    assert_eq!(gw.spent("key-app").await, HEALTHY_ANSWER_COST, "billed once");
}

#[tokio::test]
async fn replay_a_repeated_completion_is_served_from_the_ledger() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let send = || gw.send(with_key(reqwest::Client::new().post(gw.url("/v1/chat/complete")), "order-2").json(&standard_request()));

    let first = send().await;
    assert_eq!(first.status, StatusCode::OK);
    let replay = send().await;
    assert_eq!(replay.status, StatusCode::OK, "{}", replay.body);
    assert_eq!(replay.headers["idempotent-replayed"], "true");
    assert_eq!(replay.body, first.body, "the same answer, byte for byte");
    assert_eq!(provider.calls(), 1, "executed once");
    assert_eq!(gw.spent("key-app").await, HEALTHY_ANSWER_COST, "billed once");
}

#[tokio::test]
async fn replay_the_same_key_for_a_different_request_is_refused() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let send = |body: Value| gw.send(with_key(reqwest::Client::new().post(gw.url("/v1/chat/stream")), "order-3").json(&body));

    assert_eq!(send(standard_request()).await.status, StatusCode::OK);
    let mut other = standard_request();
    other["messages"] = json!([{ "role": "user", "content": "something else" }]);
    let reply = send(other).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(reply.code(), "idempotency_key_reused");
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn replay_a_retry_while_the_first_is_running_is_told_to_wait() {
    let provider = MockProvider::start(Provider::Gated).await;
    let gw = Arc::new(Deployment::new(&provider).boot().await);
    let send = |gw: Arc<Gateway>| async move {
        gw.send(with_key(reqwest::Client::new().post(gw.url("/v1/chat/stream")), "order-4").json(&standard_request()))
            .await
    };

    let first = tokio::spawn(send(Arc::clone(&gw)));
    eventually("the first attempt reaches the provider", || provider.calls() == 1).await;
    let retry = send(Arc::clone(&gw)).await;
    assert_eq!(retry.status, StatusCode::CONFLICT);
    assert_eq!(retry.code(), "request_in_progress");
    assert_eq!(retry.headers["retry-after"], "1");
    provider.open_gate(1);
    assert_eq!(first.await.unwrap().status, StatusCode::OK);
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn replay_a_refused_attempt_can_be_retried_under_its_key() {
    // Nothing was executed and nothing billed, so the key is free again: an
    // honest retry after a provider refusal must reach the provider.
    let provider = MockProvider::start(Provider::Refusing).await;
    let gw = Deployment::new(&provider).boot().await;
    let send = || gw.send(with_key(reqwest::Client::new().post(gw.url("/v1/chat/stream")), "order-5").json(&standard_request()));

    assert_eq!(send().await.status, StatusCode::BAD_GATEWAY);
    assert_eq!(send().await.status, StatusCode::BAD_GATEWAY);
    assert_eq!(provider.calls(), 2, "the retry was executed, not refused as a duplicate");
    assert_eq!(gw.spent("key-app").await, 0);
}

#[tokio::test]
async fn replay_without_a_key_cannot_be_recognised() {
    // A limit, not a gap: two identical requests with no key may be two
    // legitimate requests. Tenants whose clients retry require the key.
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    assert_eq!(gw.chat(Some("key-app"), standard_request()).await.status, StatusCode::OK);
    assert_eq!(gw.chat(Some("key-app"), standard_request()).await.status, StatusCode::OK);
    assert_eq!(provider.calls(), 2);
}

#[tokio::test]
async fn replay_a_tenant_can_require_the_key() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let reply = gw.chat(Some("key-strict"), standard_request()).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(reply.code(), "idempotency_key_required");
    assert_eq!(provider.calls(), 0);
    let keyed = reqwest::Client::new()
        .post(gw.url("/v1/chat/stream"))
        .header("x-api-key", "key-strict")
        .header("idempotency-key", "order-6")
        .json(&standard_request());
    assert_eq!(gw.send(keyed).await.status, StatusCode::OK);
}

// ===========================================================================
// 9. The shared ledger: restarts, replicas, and a dead database
// ===========================================================================
//
// Run against a real Postgres when LLM_GATEWAY_TEST_DATABASE_URL is set.

#[tokio::test]
async fn shared_ledger_a_restart_keeps_todays_spend() {
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &url);

    let first = deployment.boot().await;
    assert_eq!(first.chat(Some("key-oneshot"), small_request()).await.status, StatusCode::OK);
    eventually_async("the settlement is durable", || async { first.spent("key-oneshot").await == HEALTHY_ANSWER_COST }).await;
    drop(first); // crash

    let restarted = deployment.boot().await;
    assert_eq!(restarted.spent("key-oneshot").await, HEALTHY_ANSWER_COST, "the ledger outlived the process");
    assert_eq!(
        restarted.chat(Some("key-oneshot"), small_request()).await.status,
        StatusCode::PAYMENT_REQUIRED,
        "an exhausted tenant stays exhausted after a restart"
    );
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn shared_ledger_replicas_draw_on_one_budget() {
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &url);
    let (a, b) = (deployment.boot().await, deployment.boot().await);

    assert_eq!(a.chat(Some("key-oneshot"), small_request()).await.status, StatusCode::OK);
    eventually_async("the settlement is durable", || async { b.spent("key-oneshot").await == HEALTHY_ANSWER_COST }).await;
    assert_eq!(
        b.chat(Some("key-oneshot"), small_request()).await.status,
        StatusCode::PAYMENT_REQUIRED,
        "the second replica sees the first one's spend"
    );
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn shared_ledger_a_burst_across_replicas_admits_exactly_what_fits() {
    // Twenty simultaneous requests alternating between two gateway processes,
    // against a budget with room for three. Separate books would admit six.
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Gated).await;
    let deployment = Deployment::shared(&provider, &url);
    let replicas = [Arc::new(deployment.boot().await), Arc::new(deployment.boot().await)];

    // Every answer that comes back before the gate opens is recorded, so a
    // refusal of the wrong kind names itself instead of looking like a hang.
    let answered: Arc<std::sync::Mutex<Vec<(StatusCode, String)>>> = Arc::default();
    let burst: Vec<_> = (0..20)
        .map(|i| {
            let (gw, answered) = (Arc::clone(&replicas[i % 2]), Arc::clone(&answered));
            tokio::spawn(async move {
                let reply = gw.chat(Some("key-tight"), standard_request()).await;
                if reply.status != StatusCode::OK {
                    answered.lock().unwrap().push((reply.status, reply.body.clone()));
                }
                reply.status
            })
        })
        .collect();
    eventually("every request admitted or answered", || {
        provider.calls() as usize + answered.lock().unwrap().len() == 20
    })
    .await;
    let answered = answered.lock().unwrap().clone();
    assert!(
        answered.iter().all(|(status, _)| *status == StatusCode::PAYMENT_REQUIRED),
        "every refusal must be a budget refusal: {answered:#?}"
    );
    assert_eq!(provider.calls(), 3, "one budget across both replicas");
    provider.open_gate(20);
    let statuses = futures_util::future::join_all(burst).await;
    assert_eq!(statuses.into_iter().filter(|s| *s.as_ref().unwrap() == StatusCode::OK).count(), 3);
}

#[tokio::test]
async fn shared_ledger_concurrency_is_one_cap_across_replicas() {
    // `capped` allows 3 streams at once. Twenty simultaneous requests split
    // across two replicas: exactly 3 reach the provider, not 3 per replica.
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Gated).await;
    let deployment = Deployment::shared(&provider, &url);
    let replicas = [Arc::new(deployment.boot().await), Arc::new(deployment.boot().await)];

    let answered: Arc<std::sync::Mutex<Vec<(StatusCode, String)>>> = Arc::default();
    let burst: Vec<_> = (0..20)
        .map(|i| {
            let (gw, answered) = (Arc::clone(&replicas[i % 2]), Arc::clone(&answered));
            tokio::spawn(async move {
                let reply = gw.chat(Some("key-capped"), small_request()).await;
                if reply.status != StatusCode::OK {
                    answered.lock().unwrap().push((reply.status, reply.code()));
                }
            })
        })
        .collect();
    eventually("every request admitted or refused", || {
        provider.calls() as usize + answered.lock().unwrap().len() == 20
    })
    .await;
    assert_eq!(provider.calls(), 3, "one concurrency cap across both replicas");
    let refusals = answered.lock().unwrap().clone();
    assert!(
        refusals.iter().all(|(s, c)| *s == StatusCode::TOO_MANY_REQUESTS && c == "concurrency_limited"),
        "{refusals:?}"
    );
    provider.open_gate(20);
    futures_util::future::join_all(burst).await;
}

#[tokio::test]
async fn shared_ledger_settlements_racing_admissions_never_deadlock() {
    // Every settlement writes the tenant's rate counters while other
    // admissions for the same tenant hold its day row. Both must take the
    // locks in one order, or Postgres aborts one of them and a healthy
    // request gets a 503.
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &url);
    let replicas = [Arc::new(deployment.boot().await), Arc::new(deployment.boot().await)];
    let burst: Vec<_> = (0..40)
        .map(|i| {
            let gw = Arc::clone(&replicas[i % 2]);
            tokio::spawn(async move { gw.chat(Some("key-app"), small_request()).await })
        })
        .collect();
    let replies: Vec<Reply> = futures_util::future::join_all(burst).await.into_iter().map(Result::unwrap).collect();
    let failed: Vec<String> = replies
        .iter()
        .filter(|r| r.status != StatusCode::OK)
        .map(|r| format!("{} {}", r.status, r.body))
        .collect();
    assert!(failed.is_empty(), "{} of 40 failed: {failed:#?}", failed.len());
}

#[tokio::test]
async fn shared_ledger_the_rate_limit_is_one_window_across_replicas() {
    // `paced` allows 6 requests a minute, in total, across both replicas.
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &url);
    let (a, b) = (deployment.boot().await, deployment.boot().await);
    wait_for_mid_minute().await;
    let mut admitted = 0;
    for i in 0..14 {
        let gw = if i % 2 == 0 { &a } else { &b };
        let reply = gw.chat(Some("key-paced"), small_request()).await;
        match reply.status {
            StatusCode::OK => admitted += 1,
            StatusCode::TOO_MANY_REQUESTS => assert_eq!(reply.code(), "rate_limited"),
            other => panic!("unexpected {other}: {}", reply.body),
        }
    }
    assert_eq!(admitted, 6, "one minute's quota across both replicas, not one per replica");
    assert_eq!(provider.calls(), 6);
}

#[tokio::test]
async fn shared_ledger_a_repeated_key_is_recognised_across_replicas() {
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &url);
    let (a, b) = (deployment.boot().await, deployment.boot().await);
    async fn send(gw: &Gateway) -> Reply {
        gw.send(with_key(reqwest::Client::new().post(gw.url("/v1/chat/complete")), "order-7").json(&standard_request()))
            .await
    }

    let first = send(&a).await;
    assert_eq!(first.status, StatusCode::OK);
    // A client retrying on 409 `request_in_progress`, exactly as told to,
    // until the first attempt's settlement is durable.
    let mut replay = send(&b).await;
    for _ in 0..100 {
        if replay.status != StatusCode::CONFLICT {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        replay = send(&b).await;
    }
    assert_eq!(replay.status, StatusCode::OK, "{}", replay.body);
    assert_eq!(replay.headers["idempotent-replayed"], "true");
    assert_eq!(replay.body, first.body);
    assert_eq!(provider.calls(), 1, "executed once across both replicas");
    assert_eq!(b.spent("key-app").await, HEALTHY_ANSWER_COST, "billed once");
}

#[tokio::test]
async fn shared_ledger_a_dead_ledger_fails_closed() {
    // The gateway reaches Postgres through a relay the test can cut. With the
    // ledger gone, nothing may reach the provider: 503, not a free pass.
    let Some(url) = test_database_url() else { return };
    let relay = relay::Relay::start(&url).await;
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::shared(&provider, &relay.url).boot().await;

    assert_eq!(gw.chat(Some("key-app"), standard_request()).await.status, StatusCode::OK);
    assert_eq!(provider.calls(), 1);

    relay.cut();
    let reply = gw.chat(Some("key-app"), standard_request()).await;
    assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE, "{}", reply.body);
    assert_eq!(reply.code(), "ledger_unavailable");
    assert_eq!(reply.headers["retry-after"], "5");
    assert_eq!(provider.calls(), 1, "the provider saw nothing while the ledger was down");
    let ready = gw.send(reqwest::Client::new().get(gw.url("/health/ready"))).await;
    assert_eq!(ready.status, StatusCode::SERVICE_UNAVAILABLE, "pulled from the load balancer");
    assert!(ready.body.contains("ledger"), "{}", ready.body);
}

#[tokio::test]
async fn shared_ledger_an_operator_action_that_cannot_be_recorded_does_not_happen() {
    // The reload would revoke `key-rotating`. With the ledger unreachable the
    // action cannot be recorded, so it must not take effect at all.
    let Some(url) = test_database_url() else { return };
    let relay = relay::Relay::start(&url).await;
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &relay.url);
    let gw = deployment.boot().await;
    assert_eq!(gw.get("/v1/models", "key-rotating").await.status, StatusCode::OK);

    deployment.rewrite_tenants(&tenants_yaml(false));
    relay.cut();
    let reload = gw
        .send(reqwest::Client::new().post(gw.url("/v1/admin/registry/reload")).header("x-api-key", "key-admin"))
        .await;
    assert_eq!(reload.status, StatusCode::SERVICE_UNAVAILABLE, "{}", reload.body);
    assert_eq!(
        gw.get("/v1/models", "key-rotating").await.status,
        StatusCode::OK,
        "the unrecorded reload was not applied"
    );
}

#[tokio::test]
async fn shared_ledger_a_gateway_will_not_start_without_its_ledger() {
    // A configured ledger that cannot be reached is fatal at boot. Falling
    // back to memory would silently drop both guarantees.
    let Some(url) = test_database_url() else { return };
    let relay = relay::Relay::start(&url).await;
    relay.cut();
    let err = postgres_ledger(&relay.url, "pressure_unreachable", true).await.unwrap_err();
    assert!(!err.contains("gatewaytest"), "the error must not echo credentials: {err}");
}

// ---------------------------------------------------------------------------
// Answers at rest
// ---------------------------------------------------------------------------

async fn complete_with_key(gw: &Gateway, key: &str) -> Reply {
    gw.send(with_key(reqwest::Client::new().post(gw.url("/v1/chat/complete")), key).json(&standard_request()))
        .await
}

/// Replay `key` until the first attempt's settlement is durable, as a client
/// told `request_in_progress` would.
async fn settled_replay(gw: &Gateway, key: &str) -> Reply {
    let mut reply = complete_with_key(gw, key).await;
    for _ in 0..100 {
        if !(reply.status == StatusCode::CONFLICT && reply.code() == "request_in_progress") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        reply = complete_with_key(gw, key).await;
    }
    reply
}

async fn stored_answer(deployment: &Deployment, key: &str) -> Option<String> {
    sqlx::query_scalar("SELECT response FROM reservations WHERE idempotency_key = $1")
        .bind(key)
        .fetch_one(&deployment.ledger_pool().await)
        .await
        .unwrap()
}

#[tokio::test]
async fn shared_ledger_stored_answers_are_sealed() {
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &url);
    let gw = deployment.boot().await;

    assert_eq!(complete_with_key(&gw, "sealed-1").await.status, StatusCode::OK);
    let replay = settled_replay(&gw, "sealed-1").await;
    assert_eq!(replay.status, StatusCode::OK, "{}", replay.body);
    assert!(replay.body.contains("Hello, world"), "the replay is the real answer");

    let stored = stored_answer(&deployment, "sealed-1").await.expect("the answer is stored for replay");
    assert!(stored.starts_with("v1:test-1:"), "{stored}");
    assert!(!stored.contains("Hello"), "the answer must not be at rest in the clear: {stored}");
}

#[tokio::test]
async fn shared_ledger_without_a_key_stores_no_answer() {
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared_with(&provider, &url, false);
    let gw = deployment.boot().await;

    assert_eq!(complete_with_key(&gw, "unsealed-1").await.status, StatusCode::OK);
    let replay = settled_replay(&gw, "unsealed-1").await;
    assert_eq!(replay.status, StatusCode::CONFLICT, "{}", replay.body);
    assert_eq!(replay.code(), "duplicate_request", "recognised and billed once, but not replayed");
    assert_eq!(provider.calls(), 1);
    assert!(stored_answer(&deployment, "unsealed-1").await.is_none(), "no key, no stored answer");
}

#[tokio::test]
async fn shared_ledger_an_answer_moved_to_another_row_is_not_served() {
    // Someone with write access to the table copies one request's sealed
    // answer into another's row. The seal is bound to its row, so the copy
    // does not open, and the gateway refuses to replay rather than serve it.
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &url);
    let gw = deployment.boot().await;

    assert_eq!(complete_with_key(&gw, "moved-a").await.status, StatusCode::OK);
    assert_eq!(complete_with_key(&gw, "moved-b").await.status, StatusCode::OK);
    assert_eq!(settled_replay(&gw, "moved-a").await.status, StatusCode::OK);
    assert_eq!(settled_replay(&gw, "moved-b").await.status, StatusCode::OK);

    sqlx::query(
        "UPDATE reservations SET response = (SELECT response FROM reservations WHERE idempotency_key = 'moved-a')
          WHERE idempotency_key = 'moved-b'",
    )
    .execute(&deployment.ledger_pool().await)
    .await
    .unwrap();

    let replay = complete_with_key(&gw, "moved-b").await;
    assert_eq!(replay.status, StatusCode::CONFLICT, "{}", replay.body);
    assert_eq!(replay.code(), "duplicate_request");
    assert_eq!(provider.calls(), 2, "nothing was re-executed either");
}

// ===========================================================================
// 10. The local ledger (SQLite) and quota-split replicas
// ===========================================================================
//
// No database server needed, so these run everywhere, Windows included.

#[tokio::test]
async fn local_ledger_a_restart_keeps_todays_spend() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);

    let first = deployment.boot_with(deployment.local_ledger("ledger.sqlite3", None)).await;
    assert_eq!(first.chat(Some("key-oneshot"), small_request()).await.status, StatusCode::OK);
    eventually_async("the settlement is durable", || async { first.spent("key-oneshot").await == HEALTHY_ANSWER_COST }).await;
    drop(first); // crash

    let restarted = deployment.boot_with(deployment.local_ledger("ledger.sqlite3", None)).await;
    assert_eq!(restarted.spent("key-oneshot").await, HEALTHY_ANSWER_COST, "the ledger outlived the process");
    assert_eq!(
        restarted.chat(Some("key-oneshot"), small_request()).await.status,
        StatusCode::PAYMENT_REQUIRED,
        "an exhausted tenant stays exhausted after a restart"
    );
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn local_ledger_a_repeated_key_is_recognised_after_a_restart() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    async fn send(gw: &Gateway) -> Reply {
        gw.send(with_key(reqwest::Client::new().post(gw.url("/v1/chat/stream")), "local-1").json(&standard_request()))
            .await
    }

    let first = deployment.boot_with(deployment.local_ledger("ledger.sqlite3", None)).await;
    assert_eq!(send(&first).await.status, StatusCode::OK);
    eventually_async("the settlement is durable", || async { first.spent("key-app").await == HEALTHY_ANSWER_COST }).await;
    drop(first);

    let restarted = deployment.boot_with(deployment.local_ledger("ledger.sqlite3", None)).await;
    let replay = send(&restarted).await;
    assert_eq!(replay.status, StatusCode::CONFLICT, "{}", replay.body);
    assert_eq!(replay.code(), "duplicate_request");
    assert_eq!(provider.calls(), 1, "executed once across the restart");
}

#[tokio::test]
async fn quota_split_a_burst_across_replicas_admits_exactly_the_shares() {
    // Two replicas with separate SQLite files and no shared database. `tight`
    // has room for three standard requests; each replica's half-share has
    // room for one. Without the split, each replica would admit three: six.
    let provider = MockProvider::start(Provider::Gated).await;
    let deployment = Deployment::new(&provider);
    let replicas = [
        Arc::new(deployment.boot_with(deployment.local_ledger("replica-a.sqlite3", Some(2))).await),
        Arc::new(deployment.boot_with(deployment.local_ledger("replica-b.sqlite3", Some(2))).await),
    ];

    let refused = Arc::new(AtomicU64::new(0));
    let burst: Vec<_> = (0..20)
        .map(|i| {
            let (gw, refused) = (Arc::clone(&replicas[i % 2]), Arc::clone(&refused));
            tokio::spawn(async move {
                let reply = gw.chat(Some("key-tight"), standard_request()).await;
                if reply.status == StatusCode::PAYMENT_REQUIRED {
                    refused.fetch_add(1, Ordering::SeqCst);
                }
                reply.status
            })
        })
        .collect();
    eventually("every request admitted or refused", || {
        provider.calls() + refused.load(Ordering::SeqCst) == 20
    })
    .await;
    assert_eq!(provider.calls(), 2, "one share per replica, and the shares fit the budget");
    provider.open_gate(20);
    let statuses = futures_util::future::join_all(burst).await;
    assert_eq!(statuses.into_iter().filter(|s| *s.as_ref().unwrap() == StatusCode::OK).count(), 2);
}

#[tokio::test]
async fn quota_split_a_restarted_replica_keeps_its_spent_share() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);

    let replica = deployment.boot_with(deployment.local_ledger("replica-a.sqlite3", Some(2))).await;
    assert_eq!(replica.chat(Some("key-tight"), standard_request()).await.status, StatusCode::OK);
    eventually_async("the settlement is durable", || async { replica.spent("key-tight").await == HEALTHY_ANSWER_COST }).await;
    drop(replica);

    let restarted = deployment.boot_with(deployment.local_ledger("replica-a.sqlite3", Some(2))).await;
    let usage = restarted.get("/v1/usage", "key-tight").await.json();
    assert_eq!(usage["spent_nano_usd"], HEALTHY_ANSWER_COST, "the share's spend survived");
    assert_eq!(usage["daily_budget_nano_usd"], 3_500_000 / 2, "this replica enforces half");
    assert_eq!(usage["budget_scope"]["replica_share"]["replicas"], 2);
    assert_eq!(usage["budget_scope"]["replica_share"]["tenant_budget_nano_usd"], 3_500_000);
}

#[tokio::test]
async fn quota_split_refuses_to_boot_where_it_would_fail_open() {
    // On the in-memory ledger a restart would hand the replica a fresh share.
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    let err = deployment
        .try_boot_with(|s| s.ledger.quota_split_replicas = 2)
        .await
        .err()
        .expect("a split on the in-memory ledger must not boot");
    assert!(err.contains("sqlite"), "{err}");

    let err = deployment
        .try_boot_with(|s| {
            deployment.local_ledger("margin.sqlite3", Some(2))(s);
            s.ledger.quota_split_margin_percent = 0;
        })
        .await
        .err()
        .expect("a 0% margin must not boot");
    assert!(err.contains("margin"), "{err}");
}

#[tokio::test]
async fn local_ledger_a_gateway_will_not_start_without_its_ledger() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    let err = deployment
        .try_boot_with(|s| {
            s.ledger.backend = llm_gateway::config::LedgerBackend::Sqlite;
            s.ledger.sqlite_path = std::path::PathBuf::from("/definitely/not/a/dir/ledger.sqlite3");
        })
        .await
        .err()
        .expect("an unopenable ledger must stop the gateway starting");
    assert!(err.contains("ledger"), "{err}");
}

/// Wait until at least 15 s remain in the current UTC minute, so a test that
/// counts within one rate window cannot straddle two.
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

#[tokio::test]
async fn local_ledger_a_long_stream_is_never_swept_while_live() {
    // A 1 s lease and a stream held open for 4 s. Without renewal the
    // sweeper would take the live reservation, bill it in full and ignore
    // its real settlement. With it, the stream settles at its real usage.
    let provider = MockProvider::start(Provider::HeldMidStream).await;
    let deployment = Deployment::new(&provider);
    let gw = deployment
        .boot_with(|s| {
            deployment.local_ledger("lease.sqlite3", None)(s);
            s.ledger.lease_secs = 1;
            s.ledger.sweep_interval_secs = 1;
        })
        .await;

    let mut response = reqwest::Client::new()
        .post(gw.url("/v1/chat/stream"))
        .header("x-api-key", "key-app")
        .json(&standard_request())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let reserved: u64 = response.headers()["x-reserved-nano-usd"].to_str().unwrap().parse().unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    provider.open_gate(1);
    let mut body = String::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        body.push_str(&String::from_utf8_lossy(&chunk));
    }
    assert!(body.contains("event: end"), "{body}");
    eventually_async("the settlement is durable", || async { gw.spent("key-app").await != reserved }).await;
    assert_eq!(gw.spent("key-app").await, HEALTHY_ANSWER_COST, "billed its real usage, never swept");
}

#[tokio::test]
async fn quota_split_divides_the_rate_limit_too() {
    // `paced` allows 6 requests a minute; each of 2 split replicas enforces 3.
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    let replica = deployment.boot_with(deployment.local_ledger("paced-split.sqlite3", Some(2))).await;
    wait_for_mid_minute().await;
    let mut admitted = 0;
    for _ in 0..6 {
        if replica.chat(Some("key-paced"), small_request()).await.status == StatusCode::OK {
            admitted += 1;
        }
    }
    assert_eq!(admitted, 3, "this replica's share of the minute");
}

#[tokio::test]
async fn readiness_names_the_ledger_consistency_scope() {
    // A bare backend name invites mistaking a per-instance ledger for a
    // shared one; readiness spells out what the ledger guarantees.
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    let scope = |gw: &Gateway| {
        let url = gw.url("/health/ready");
        async move {
            let reply = reqwest::get(url).await.unwrap();
            assert_eq!(reply.status(), StatusCode::OK);
            reply.json::<Value>().await.unwrap()["ledger"].clone()
        }
    };

    let memory = deployment.boot().await;
    assert_eq!(
        scope(&memory).await,
        json!({ "backend": "memory", "durability": "ephemeral", "replica_mode": "single_process" })
    );
    let local = deployment.boot_with(deployment.local_ledger("scope.sqlite3", None)).await;
    assert_eq!(
        scope(&local).await,
        json!({ "backend": "sqlite", "durability": "persistent", "replica_mode": "single_instance_only" })
    );
    let split = deployment.boot_with(deployment.local_ledger("scope-split.sqlite3", Some(3))).await;
    assert_eq!(
        scope(&split).await,
        json!({ "backend": "sqlite", "durability": "persistent",
                "replica_mode": { "quota_split": { "replicas": 3, "margin_percent": 100 } } })
    );
    if let Some(url) = test_database_url() {
        let shared = Deployment::shared(&provider, &url).boot().await;
        assert_eq!(
            scope(&shared).await,
            json!({ "backend": "postgres", "durability": "persistent", "replica_mode": "shared" })
        );
    }
}

// ===========================================================================
// HOLD: requests that wait for a person
// ===========================================================================
//
// A tenant's policy can hold a request instead of executing it. A held
// request must never reach the provider until someone with `approve:holds`
// approves it, and then only once, only as approved, and only if everything
// else still admits it.

/// Held because of its model, whatever it costs.
fn held_request() -> Value {
    json!({
        "model": "mock/other",
        "messages": [{ "role": "user", "content": "please approve me" }],
        "params": { "max_tokens": 5 }
    })
}

/// Ask for `body`, expect it held, and return the hold's id.
async fn held(gw: &Gateway, key: &str, body: Value) -> String {
    let reply = gw.chat(Some(key), body).await;
    assert_eq!(reply.status, StatusCode::ACCEPTED, "{}", reply.body);
    let json = reply.json();
    assert_eq!(json["status"], "held");
    assert_eq!(json["hold"]["state"], "pending");
    let id = json["hold"]["id"].as_str().expect("hold id").to_string();
    assert_eq!(reply.headers["hold-id"], id.as_str());
    assert_eq!(reply.headers["location"], format!("/v1/holds/{id}").as_str());
    id
}

async fn decide(gw: &Gateway, key: &str, id: &str, verdict: &str) -> Reply {
    gw.send(
        reqwest::Client::new()
            .post(gw.url(&format!("/v1/admin/holds/{id}/{verdict}")))
            .header("x-api-key", key)
            .json(&json!({ "note": format!("{verdict} by the test") })),
    )
    .await
}

async fn approve(gw: &Gateway, id: &str) {
    let reply = decide(gw, "key-approver", id, "approve").await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert_eq!(reply.json()["hold"]["state"], "approved");
}

/// Send `body` again, presenting the hold.
async fn with_hold(gw: &Gateway, key: &str, id: &str, body: &Value) -> Reply {
    gw.send(
        reqwest::Client::new()
            .post(gw.url("/v1/chat/stream"))
            .header("x-api-key", key)
            .header("hold-id", id)
            .json(body),
    )
    .await
}

async fn hold_state(gw: &Gateway, key: &str, id: &str) -> String {
    let reply = gw.get(&format!("/v1/holds/{id}"), key).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    reply.json()["hold"]["state"].as_str().unwrap_or_default().to_string()
}

#[tokio::test]
async fn hold_a_held_request_never_reaches_the_provider() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let id = held(&gw, "key-guarded", held_request()).await;
    assert_eq!(provider.calls(), 0, "held, not executed");
    assert_eq!(gw.spent("key-guarded").await, 0, "and not charged");
    let reply = gw.get(&format!("/v1/holds/{id}"), "key-guarded").await;
    let hold = &reply.json()["hold"];
    assert_eq!(hold["state"], "pending");
    assert_eq!(hold["model"], "mock/other");
    assert_eq!(hold["requested_by"], "ak_guarded");
    assert!(hold["reason"].as_str().unwrap().contains("mock/other"), "{hold}");
    assert!(!reply.body.contains("please approve me"), "a hold never carries the prompt: {}", reply.body);
    // Asking again is asking again: a second hold, still nothing executed.
    let again = held(&gw, "key-guarded", held_request()).await;
    assert_ne!(again, id);
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn hold_the_cost_rule_holds_only_what_is_above_the_threshold() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    // 46,000 nano-USD at worst: under the threshold, so it runs.
    assert_eq!(gw.chat(Some("key-guarded"), small_request()).await.status, StatusCode::OK);
    assert_eq!(provider.calls(), 1);
    // ~1.03M at worst, the same model: above it, so it waits.
    let id = held(&gw, "key-guarded", standard_request()).await;
    let reply = gw.get(&format!("/v1/holds/{id}"), "key-guarded").await;
    assert!(reply.json()["hold"]["reason"].as_str().unwrap().contains("above the approval threshold"));
    assert_eq!(provider.calls(), 1, "the expensive request was held");
}

#[tokio::test]
async fn hold_an_approved_request_executes_exactly_once() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let id = held(&gw, "key-guarded", held_request()).await;
    approve(&gw, &id).await;
    assert_eq!(provider.calls(), 0, "approving executes nothing by itself");

    // Ten copies of the approved request at once: one runs.
    let body = held_request();
    let replies = futures_util::future::join_all((0..10).map(|_| with_hold(&gw, "key-guarded", &id, &body))).await;
    let ran = replies.iter().filter(|r| r.status == StatusCode::OK).count();
    assert_eq!(ran, 1, "{:?}", replies.iter().map(|r| (r.status, r.body.clone())).collect::<Vec<_>>());
    for refused in replies.iter().filter(|r| r.status != StatusCode::OK) {
        assert_eq!(refused.status, StatusCode::CONFLICT, "{}", refused.body);
        assert_eq!(refused.code(), "hold_consumed");
    }
    eventually("the answer settles", || provider.calls() == 1).await;
    assert_eq!(provider.calls(), 1, "executed exactly once");
    let reply = gw.get(&format!("/v1/holds/{id}"), "key-guarded").await;
    let hold = &reply.json()["hold"];
    assert_eq!(hold["state"], "consumed");
    assert!(hold["reservation_id"].is_string(), "{hold}");
    assert_eq!(hold["decided_by"], "ops/ak_approver");
}

#[tokio::test]
async fn hold_a_pending_hold_cannot_be_used() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let id = held(&gw, "key-guarded", held_request()).await;

    let reply = with_hold(&gw, "key-guarded", &id, &held_request()).await;
    assert_eq!(reply.status, StatusCode::CONFLICT, "{}", reply.body);
    assert_eq!(reply.code(), "hold_pending");
    assert_eq!(reply.headers["retry-after"], "5");
    assert_eq!(provider.calls(), 0);
    assert_eq!(hold_state(&gw, "key-guarded", &id).await, "pending");
}

#[tokio::test]
async fn hold_a_denied_or_revoked_hold_never_executes() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let denied = held(&gw, "key-guarded", held_request()).await;
    let reply = decide(&gw, "key-approver", &denied, "deny").await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert_eq!(reply.json()["hold"]["note"], "deny by the test");
    let reply = with_hold(&gw, "key-guarded", &denied, &held_request()).await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN, "{}", reply.body);
    assert_eq!(reply.code(), "hold_denied");
    let late = decide(&gw, "key-approver", &denied, "approve").await;
    assert_eq!(late.status, StatusCode::CONFLICT, "a denial is final: {}", late.body);
    assert_eq!(late.code(), "hold_not_decidable");

    // An approval can be taken back until it is used.
    let revoked = held(&gw, "key-guarded", held_request()).await;
    approve(&gw, &revoked).await;
    assert_eq!(decide(&gw, "key-approver", &revoked, "deny").await.status, StatusCode::OK);
    let reply = with_hold(&gw, "key-guarded", &revoked, &held_request()).await;
    assert_eq!(reply.code(), "hold_denied");
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn hold_nobody_decides_their_own_request() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    // The lead may approve holds, but this one is the lead's own request.
    let id = held(&gw, "key-guarded-lead", held_request()).await;

    for verdict in ["approve", "deny"] {
        let reply = decide(&gw, "key-guarded-lead", &id, verdict).await;
        assert_eq!(reply.status, StatusCode::FORBIDDEN, "{}", reply.body);
        assert_eq!(reply.code(), "self_approval");
    }
    assert_eq!(hold_state(&gw, "key-guarded-lead", &id).await, "pending");
    // The same lead may decide a colleague's.
    let other = held(&gw, "key-guarded", held_request()).await;
    assert_eq!(decide(&gw, "key-guarded-lead", &other, "approve").await.status, StatusCode::OK);
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn hold_deciding_needs_the_dedicated_scope() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let id = held(&gw, "key-guarded", held_request()).await;

    // `admin` can look at the queue, but not decide: approval is its own grant.
    for key in ["key-admin", "key-app", "key-guarded"] {
        let reply = decide(&gw, key, &id, "approve").await;
        assert_eq!(reply.status, StatusCode::FORBIDDEN, "{key}: {}", reply.body);
    }
    let queue = gw.get("/v1/admin/holds?state=pending", "key-admin").await;
    assert_eq!(queue.status, StatusCode::OK, "{}", queue.body);
    assert_eq!(queue.json()["holds"][0]["id"], id.as_str());
    assert_eq!(gw.get("/v1/admin/holds", "key-guarded").await.status, StatusCode::FORBIDDEN);
    let unauthenticated = decide(&gw, "not-a-key", &id, "approve").await;
    assert_eq!(unauthenticated.status, StatusCode::UNAUTHORIZED);
    assert_eq!(hold_state(&gw, "key-guarded", &id).await, "pending");
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn hold_an_approval_covers_only_the_request_approved() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let id = held(&gw, "key-guarded", held_request()).await;
    approve(&gw, &id).await;

    // Another prompt, a bigger output cap, the other endpoint: none of them
    // is the request that was approved.
    let mut other_prompt = held_request();
    other_prompt["messages"][0]["content"] = json!("something else entirely");
    let mut bigger = held_request();
    bigger["params"]["max_tokens"] = json!(6);
    for body in [&other_prompt, &bigger] {
        let reply = with_hold(&gw, "key-guarded", &id, body).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY, "{}", reply.body);
        assert_eq!(reply.code(), "hold_mismatch");
    }
    let second_door = gw
        .send(
            reqwest::Client::new()
                .post(gw.url("/v1/chat/complete"))
                .header("x-api-key", "key-guarded")
                .header("hold-id", &id)
                .json(&held_request()),
        )
        .await;
    assert_eq!(second_door.code(), "hold_mismatch", "{}", second_door.body);
    assert_eq!(provider.calls(), 0);

    // The failed attempts did not use the approval up.
    assert_eq!(with_hold(&gw, "key-guarded", &id, &held_request()).await.status, StatusCode::OK);
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn hold_a_hold_is_its_tenants_alone() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let id = held(&gw, "key-guarded", held_request()).await;
    approve(&gw, &id).await;

    let peek = gw.get(&format!("/v1/holds/{id}"), "key-app").await;
    assert_eq!(peek.status, StatusCode::NOT_FOUND, "another tenant's hold does not exist for acme");
    let reply = with_hold(&gw, "key-app", &id, &held_request()).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND, "{}", reply.body);
    assert_eq!(reply.code(), "hold_not_found");
    let forged = with_hold(&gw, "key-guarded", "not-a-uuid", &held_request()).await;
    assert_eq!(forged.code(), "invalid_hold_id");
    assert_eq!(provider.calls(), 0);
    assert_eq!(hold_state(&gw, "key-guarded", &id).await, "approved");
}

#[tokio::test]
async fn hold_undecided_holds_and_unused_approvals_expire() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let waiting = held(&gw, "key-hasty", small_request()).await;
    let approved = held(&gw, "key-hasty", small_request()).await;
    approve(&gw, &approved).await;
    tokio::time::sleep(Duration::from_millis(4_200)).await;

    assert_eq!(hold_state(&gw, "key-hasty", &waiting).await, "expired");
    assert_eq!(hold_state(&gw, "key-hasty", &approved).await, "expired");
    let late = decide(&gw, "key-approver", &waiting, "approve").await;
    assert_eq!(late.status, StatusCode::CONFLICT, "{}", late.body);
    for id in [&waiting, &approved] {
        let reply = with_hold(&gw, "key-hasty", id, &small_request()).await;
        assert_eq!(reply.status, StatusCode::GONE, "{}", reply.body);
        assert_eq!(reply.code(), "hold_expired");
    }
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn hold_execution_rechecks_the_tenant_and_the_budget() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    let gw = deployment.boot().await;

    // Approved, but the budget cannot cover it: refused when it would run,
    // and the approval is not used up by the refusal.
    let id = held(&gw, "key-thrifty", standard_request()).await;
    approve(&gw, &id).await;
    let reply = with_hold(&gw, "key-thrifty", &id, &standard_request()).await;
    assert_eq!(reply.status, StatusCode::PAYMENT_REQUIRED, "{}", reply.body);
    assert_eq!(hold_state(&gw, "key-thrifty", &id).await, "approved");

    // Approved, then the tenant is switched off: nothing runs.
    let id = held(&gw, "key-guarded", held_request()).await;
    approve(&gw, &id).await;
    deployment.rewrite_tenants(
        &tenants_yaml(true).replace("tenant_id: guarded\n    enabled: true", "tenant_id: guarded\n    enabled: false"),
    );
    let reload = gw
        .send(reqwest::Client::new().post(gw.url("/v1/admin/registry/reload")).header("x-api-key", "key-admin"))
        .await;
    assert_eq!(reload.status, StatusCode::OK, "{}", reload.body);
    let reply = with_hold(&gw, "key-guarded", &id, &held_request()).await;
    assert!(
        matches!(reply.status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN),
        "{} {}",
        reply.status,
        reply.body
    );
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn hold_a_provider_refusal_gives_the_approval_back() {
    let provider = MockProvider::start(Provider::Refusing).await;
    let gw = Deployment::new(&provider).boot().await;
    let id = held(&gw, "key-guarded", held_request()).await;
    approve(&gw, &id).await;

    // The provider refused before accepting anything: nothing was executed,
    // so the approval is restored, as an idempotency key would be.
    let reply = with_hold(&gw, "key-guarded", &id, &held_request()).await;
    assert_eq!(reply.status, StatusCode::BAD_GATEWAY, "{}", reply.body);
    eventually_async("the approval is restored", || {
        let gw = &gw;
        let id = &id;
        async move { hold_state(gw, "key-guarded", id).await == "approved" }
    })
    .await;
    assert_eq!(with_hold(&gw, "key-guarded", &id, &held_request()).await.status, StatusCode::BAD_GATEWAY);
    assert_eq!(provider.calls(), 2, "usable again after the refusal");
}

#[tokio::test]
async fn hold_too_many_waiting_are_refused() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let first = held(&gw, "key-queued", small_request()).await;
    held(&gw, "key-queued", small_request()).await;

    let reply = gw.chat(Some("key-queued"), small_request()).await;
    assert_eq!(reply.status, StatusCode::TOO_MANY_REQUESTS, "{}", reply.body);
    assert_eq!(reply.code(), "holds_pending_limit");
    // A decided hold no longer waits, and makes room.
    assert_eq!(decide(&gw, "key-approver", &first, "deny").await.status, StatusCode::OK);
    held(&gw, "key-queued", small_request()).await;
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn hold_every_transition_is_on_the_decision_record() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;
    let id = held(&gw, "key-guarded", held_request()).await;
    approve(&gw, &id).await;
    assert_eq!(with_hold(&gw, "key-guarded", &id, &held_request()).await.status, StatusCode::OK);

    let reply = gw.get("/v1/admin/decisions?tenant=guarded", "key-admin").await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let trail: Vec<(String, String)> = reply.json()["decisions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["request_id"] == id.as_str())
        .map(|d| (d["code"].as_str().unwrap().to_string(), d["key_id"].as_str().unwrap().to_string()))
        .collect();
    let expect = |code: &str, key: &str| assert!(trail.contains(&(code.into(), key.into())), "{code} by {key}: {trail:?}");
    expect("hold_requested", "ak_guarded");
    expect("hold_approved", "ak_approver");
    expect("hold_consumed", "ak_guarded");
    assert!(reply.json()["decisions"].as_array().unwrap().iter().all(|d| d["kind"] == "hold"));
    assert!(!reply.body.contains("please approve me"), "no prompt on the record: {}", reply.body);
}

#[tokio::test]
async fn hold_the_second_door_holds_too() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let gw = Deployment::new(&provider).boot().await;

    let reply = gw.complete(Some("key-guarded"), held_request()).await;
    assert_eq!(reply.status, StatusCode::ACCEPTED, "{}", reply.body);
    let id = reply.json()["hold"]["id"].as_str().unwrap().to_string();
    assert_eq!(reply.json()["hold"]["endpoint"], "complete");
    assert_eq!(provider.calls(), 0);
    approve(&gw, &id).await;
    let done = gw
        .send(
            reqwest::Client::new()
                .post(gw.url("/v1/chat/complete"))
                .header("x-api-key", "key-guarded")
                .header("hold-id", &id)
                .json(&held_request()),
        )
        .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.body);
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn local_ledger_a_hold_survives_a_restart() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    let id = {
        let gw = deployment.boot_with(deployment.local_ledger("ledger.sqlite3", None)).await;
        let id = held(&gw, "key-guarded", held_request()).await;
        approve(&gw, &id).await;
        id
    };
    let gw = deployment.boot_with(deployment.local_ledger("ledger.sqlite3", None)).await;
    assert_eq!(hold_state(&gw, "key-guarded", &id).await, "approved", "the approval survived the restart");
    assert_eq!(with_hold(&gw, "key-guarded", &id, &held_request()).await.status, StatusCode::OK);
    assert_eq!(with_hold(&gw, "key-guarded", &id, &held_request()).await.code(), "hold_consumed");
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn shared_ledger_a_hold_is_one_hold_across_replicas() {
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &url);
    let (a, b) = (deployment.boot().await, deployment.boot().await);

    // Held on one replica, approved on the other, visible on both.
    let id = held(&a, "key-guarded", held_request()).await;
    let queue = b.get("/v1/admin/holds?state=pending&tenant=guarded", "key-approver").await;
    assert_eq!(queue.json()["holds"][0]["id"], id.as_str(), "{}", queue.body);
    approve(&b, &id).await;
    assert_eq!(hold_state(&a, "key-guarded", &id).await, "approved");

    // Twenty copies across both replicas at once: one runs.
    let body = held_request();
    let replies = futures_util::future::join_all(
        (0..20).map(|i| with_hold(if i % 2 == 0 { &a } else { &b }, "key-guarded", &id, &body)),
    )
    .await;
    let ran = replies.iter().filter(|r| r.status == StatusCode::OK).count();
    assert_eq!(ran, 1, "{:?}", replies.iter().map(|r| (r.status, r.body.clone())).collect::<Vec<_>>());
    assert!(replies.iter().filter(|r| r.status != StatusCode::OK).all(|r| r.code() == "hold_consumed"));
    assert_eq!(provider.calls(), 1, "executed once across both replicas");
}

#[tokio::test]
async fn shared_ledger_lapsed_holds_are_expired_and_recorded() {
    let Some(url) = test_database_url() else { return };
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &url);
    let gw = deployment.boot().await;
    let waiting = held(&gw, "key-hasty", small_request()).await;
    let unused = held(&gw, "key-hasty", small_request()).await;
    approve(&gw, &unused).await;
    tokio::time::sleep(Duration::from_millis(4_200)).await;

    let LedgerChoice::Shared { schema, .. } = &deployment.ledger else { unreachable!() };
    let ledger = postgres_ledger(&url, schema, false).await.expect("connect ledger");
    assert_eq!(ledger.expire_holds().await.unwrap(), 2);
    assert_eq!(ledger.expire_holds().await.unwrap(), 0, "each hold expires once");
    let record = gw.get("/v1/admin/decisions?tenant=hasty", "key-admin").await.json();
    let reason = |id: &str| {
        record["decisions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["request_id"] == id && d["code"] == "hold_expired")
            .map(|d| d["reason"].as_str().unwrap().to_string())
    };
    assert_eq!(reason(&waiting).as_deref(), Some("nobody decided before the hold expired"));
    assert_eq!(reason(&unused).as_deref(), Some("approved, but not used before the approval lapsed"));
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn shared_ledger_a_hold_decision_that_cannot_be_recorded_does_not_happen() {
    let Some(url) = test_database_url() else { return };
    let relay = relay::Relay::start(&url).await;
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::shared(&provider, &relay.url);
    let gw = deployment.boot().await;
    let id = held(&gw, "key-guarded", held_request()).await;

    relay.cut();
    let reply = decide(&gw, "key-approver", &id, "approve").await;
    assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE, "{}", reply.body);
    // And a request the policy would hold is not let through either.
    let held = gw.chat(Some("key-guarded"), held_request()).await;
    assert_eq!(held.status, StatusCode::SERVICE_UNAVAILABLE, "{}", held.body);
    assert_eq!(provider.calls(), 0);

    // Read through a connection of its own: the gateway's is cut.
    let LedgerChoice::Shared { schema, .. } = &deployment.ledger else { unreachable!() };
    let pool = postgres_ledger(&url, schema, false).await.expect("connect ledger").pool().clone();
    let state: String = sqlx::query_scalar("SELECT state FROM holds WHERE id = $1")
        .bind(uuid::Uuid::parse_str(&id).unwrap())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "pending", "the unrecorded approval did not happen");
}

// ===========================================================================
// 11. Known gaps of the in-memory ledger
// ===========================================================================
//
// The in-memory ledger is per process by design. These pin what that means,
// so nobody mistakes it for the shared ledger above.

#[tokio::test]
async fn gap_memory_ledger_a_restart_forgets_todays_spend() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);

    let first = deployment.boot().await;
    assert_eq!(first.chat(Some("key-oneshot"), small_request()).await.status, StatusCode::OK);
    assert_eq!(first.spent("key-oneshot").await, HEALTHY_ANSWER_COST);
    assert_eq!(
        first.chat(Some("key-oneshot"), small_request()).await.status,
        StatusCode::PAYMENT_REQUIRED,
        "the budget holds while the process lives"
    );
    drop(first); // crash

    let restarted = deployment.boot().await;
    assert_eq!(restarted.spent("key-oneshot").await, 0, "GAP: the ledger died with the process");
    assert_eq!(
        restarted.chat(Some("key-oneshot"), small_request()).await.status,
        StatusCode::OK,
        "GAP: an exhausted tenant spends again after a restart"
    );
    assert_eq!(provider.calls(), 2, "two answers paid for against a budget for one");
}

#[tokio::test]
async fn gap_memory_ledger_replicas_each_enforce_the_full_budget() {
    let provider = MockProvider::start(Provider::Healthy).await;
    let deployment = Deployment::new(&provider);
    let (a, b) = (deployment.boot().await, deployment.boot().await);

    assert_eq!(a.chat(Some("key-oneshot"), small_request()).await.status, StatusCode::OK);
    assert_eq!(a.chat(Some("key-oneshot"), small_request()).await.status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(
        b.chat(Some("key-oneshot"), small_request()).await.status,
        StatusCode::OK,
        "GAP: the second replica has its own, untouched ledger"
    );
    assert_eq!(provider.calls(), 2, "N replicas admit N budgets");
}

/// Wait until an async `condition` holds, or fail with `what`.
async fn eventually_async<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// A TCP relay to Postgres that a test can cut
// ---------------------------------------------------------------------------

mod relay {
    use std::sync::{Arc, Mutex};

    use tokio::{net::TcpListener, task::JoinHandle};

    pub struct Relay {
        /// The database URL, rewritten to go through the relay.
        pub url: String,
        tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    }

    impl Relay {
        pub async fn start(database_url: &str) -> Self {
            let mut url = reqwest::Url::parse(database_url).expect("database URL");
            let target = format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(5432));
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            url.set_host(Some("127.0.0.1")).unwrap();
            url.set_port(Some(port)).unwrap();

            let tasks: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
            let registry = Arc::clone(&tasks);
            let accept = tokio::spawn(async move {
                while let Ok((mut inbound, _)) = listener.accept().await {
                    let target = target.clone();
                    let pipe = tokio::spawn(async move {
                        if let Ok(mut outbound) = tokio::net::TcpStream::connect(&target).await {
                            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                        }
                    });
                    registry.lock().unwrap().push(pipe);
                }
            });
            tasks.lock().unwrap().push(accept);
            Self { url: url.to_string(), tasks }
        }

        /// Kill every connection and stop accepting: the database is gone.
        pub fn cut(&self) {
            for task in self.tasks.lock().unwrap().drain(..) {
                task.abort();
            }
        }
    }

    impl Drop for Relay {
        fn drop(&mut self) {
            self.cut();
        }
    }
}

// ---------------------------------------------------------------------------
// Captured logs
// ---------------------------------------------------------------------------

mod captured_logs {
    use std::sync::{Mutex, OnceLock};

    static BUFFER: Mutex<Vec<u8>> = Mutex::new(Vec::new());
    static INSTALLED: OnceLock<()> = OnceLock::new();

    struct Sink;

    impl std::io::Write for Sink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            BUFFER.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// The shipped log format (JSON, flattened events) at the shipped level.
    pub fn install() {
        INSTALLED.get_or_init(|| {
            let subscriber = tracing_subscriber::fmt()
                .json()
                .flatten_event(true)
                .with_env_filter(tracing_subscriber::EnvFilter::new("info,llm_gateway=debug"))
                .with_writer(|| Sink)
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
    }

    pub fn contents() -> String {
        String::from_utf8_lossy(&BUFFER.lock().unwrap()).into_owned()
    }
}

// ---------------------------------------------------------------------------
// Minimal temp dir
// ---------------------------------------------------------------------------

mod tempdir {
    use std::path::{Path, PathBuf};

    pub struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        pub fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        pub fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
