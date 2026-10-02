//! Crash tests: the real gateway binary, killed without warning.
//!
//! The pressure suite's "restarts" drop a gateway inside the test process,
//! which still runs destructors and flushes queues. These tests start the
//! actual `llm-gateway` executable as a separate process, kill it outright
//! (SIGKILL / TerminateProcess: no destructors, no graceful shutdown) at each
//! critical point of a request, restart it on the same SQLite ledger, and
//! read the ledger file directly.
//!
//! The rule under test: nothing reaches the provider until the reservation
//! is durable, so a crash can leave a reservation open but never an
//! unrecorded provider call, and an open reservation is billed in full once
//! it is swept.
//!
//! Not covered: a kill after the answer completes but before its settlement
//! is written. That window is milliseconds wide and cannot be hit on purpose.
//! Its outcome is the same as the kills below (the reservation stays open and
//! is swept, billed in full), on the same code path.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
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
    http::{StatusCode, header},
    response::Response,
    routing::post,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Mock provider
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Provider {
    /// Accepts the request and never sends a response.
    Silent,
    /// Sends headers and one token, then stalls.
    StallsAfterFirstToken,
}

#[derive(Clone)]
struct ProviderState {
    behaviour: Provider,
    calls: Arc<AtomicU64>,
}

async fn answer(State(state): State<ProviderState>, Json(_body): Json<Value>) -> Response {
    state.calls.fetch_add(1, Ordering::SeqCst);
    if state.behaviour == Provider::Silent {
        std::future::pending::<()>().await;
    }
    let first = format!(
        "data: {}\n\n",
        json!({ "id": "p1", "choices": [{ "index": 0, "delta": { "content": "partial" } }] })
    );
    let stream = futures_util::stream::unfold(0u8, move |n| {
        let first = first.clone();
        async move {
            if n == 0 {
                Some((Ok::<_, std::io::Error>(first), 1))
            } else {
                std::future::pending::<()>().await;
                None
            }
        }
    });
    let mut response = Response::new(Body::from_stream(stream));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
    response
}

async fn start_provider(behaviour: Provider) -> (SocketAddr, Arc<AtomicU64>) {
    let calls = Arc::new(AtomicU64::new(0));
    let app = Router::new()
        .route("/v1/chat/completions", post(answer))
        .with_state(ProviderState { behaviour, calls: Arc::clone(&calls) });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, calls)
}

// ---------------------------------------------------------------------------
// A gateway process
// ---------------------------------------------------------------------------

/// A config directory with a SQLite ledger, shared by every process started
/// from it, as a host's disk is.
struct Install {
    dir: PathBuf,
}

impl Install {
    fn new(provider: SocketAddr) -> Self {
        let dir = std::env::temp_dir().join(format!("llm-gateway-crash-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        // The shipped config, so the binary runs exactly as installed; the
        // test-specific values are environment overrides below.
        std::fs::copy(Path::new(env!("CARGO_MANIFEST_DIR")).join("config/default.toml"), dir.join("default.toml"))
            .unwrap();
        std::fs::write(
            dir.join("models.yaml"),
            format!(
                "schema_version: 1\nmodels:\n  mock/chat:\n    provider: nvidia\n    endpoint: http://{provider}/v1/chat/completions\n    upstream_model: mock-model\n    default_params:\n      max_tokens: 128\n    cost:\n      input_per_mtok_usd: 2.0\n      output_per_mtok_usd: 8.0\n"
            ),
        )
        .unwrap();
        std::fs::write(
            dir.join("tenants.yaml"),
            "tenants:\n  - tenant_id: acme\n    enabled: true\n    credentials:\n      - key_id: ak_app\n        key: key-app\n        scopes: [chat:stream]\n    allowed_models: [\"mock/*\"]\n    limits:\n      requests_per_minute: 1000\n      tokens_per_minute: 10000000\n      max_concurrent_streams: 16\n      max_output_tokens: 4096\n      daily_budget_nano_usd: 100000000000\n",
        )
        .unwrap();
        Self { dir }
    }

    /// Start `llm-gateway` from this install and wait until it is ready.
    async fn start(&self) -> Process {
        let port = {
            let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap().port()
        };
        let log_path = self.dir.join(format!("gateway-{port}.log"));
        let log = std::fs::File::create(&log_path).expect("create the gateway log");
        let child = Command::new(env!("CARGO_BIN_EXE_llm-gateway"))
            .env("LLM_GATEWAY_CONFIG_DIR", &self.dir)
            .env("LLM_GATEWAY__SERVER__BIND_ADDR", "127.0.0.1")
            .env("LLM_GATEWAY__SERVER__PORT", port.to_string())
            .env("LLM_GATEWAY__REGISTRY__HOT_RELOAD", "false")
            // A 2 s lease: a killed process's leases lapse within 2 s, and the
            // sweeper looks every second, so a restart recovers them quickly.
            .env("LLM_GATEWAY__LEDGER__LEASE_SECS", "2")
            .env("LLM_GATEWAY__LEDGER__SWEEP_INTERVAL_SECS", "1")
            .env("LLM_GATEWAY__LEDGER__TIMEOUT_MS", "5000")
            .env("LLM_GATEWAY__TELEMETRY__LOG_FILTER", "warn")
            .env("RUST_BACKTRACE", "1")
            // Its logs (stdout) and any panic (stderr) are kept, and shown if
            // the test fails: a gateway that errors mid-test is otherwise
            // invisible.
            .stdout(Stdio::from(log.try_clone().expect("gateway log")))
            .stderr(Stdio::from(log.try_clone().expect("gateway log")))
            .spawn()
            .expect("start llm-gateway");
        let process = Process { child, base: format!("http://127.0.0.1:{port}"), log: log_path };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(reply) = reqwest::get(format!("{}/health/ready", process.base)).await
                && reply.status() == StatusCode::OK
            {
                return process;
            }
            assert!(tokio::time::Instant::now() < deadline, "llm-gateway never became ready");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The ledger file, read directly, as an operator inspecting it would.
    async fn ledger(&self) -> sqlx::SqlitePool {
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(self.dir.join("ledger.sqlite3"))
            .busy_timeout(Duration::from_secs(5));
        sqlx::sqlite::SqlitePoolOptions::new().max_connections(1).connect_with(options).await.unwrap()
    }

    async fn reservations(&self) -> Vec<(String, i64)> {
        sqlx::query_as("SELECT state, reserved_nano_usd FROM reservations")
            .fetch_all(&self.ledger().await)
            .await
            .unwrap()
    }

    async fn spent(&self) -> i64 {
        sqlx::query_scalar("SELECT COALESCE(SUM(spent_nano_usd), 0) FROM spend_days")
            .fetch_one(&self.ledger().await)
            .await
            .unwrap()
    }

    /// Wait for the restarted gateway's sweeper to close the orphan.
    async fn swept(&self) -> Vec<(String, i64)> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let rows = self.reservations().await;
            if !rows.is_empty() && rows.iter().all(|(state, _)| state != "open") {
                return rows;
            }
            assert!(tokio::time::Instant::now() < deadline, "the orphan was never swept: {rows:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for Install {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct Process {
    child: Child,
    base: String,
    /// This process's output, stdout and stderr.
    log: PathBuf,
}

impl Process {
    /// SIGKILL / TerminateProcess: no destructors, no flush, no drain.
    fn kill(mut self) {
        self.child.kill().expect("kill llm-gateway");
        let _ = self.child.wait();
    }

    fn chat(&self, idempotency_key: Option<&str>) -> reqwest::RequestBuilder {
        let mut request = reqwest::Client::new()
            .post(format!("{}/v1/chat/stream", self.base))
            .header("x-api-key", "key-app")
            .json(&json!({ "model": "mock/chat", "messages": [{ "role": "user", "content": "hi" }] }));
        if let Some(key) = idempotency_key {
            request = request.header("idempotency-key", key);
        }
        request
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            let log = std::fs::read_to_string(&self.log).unwrap_or_default();
            eprintln!("--- llm-gateway output ({}) ---\n{log}--- end ---", self.log.display());
        }
    }
}

async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !condition() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// Kill points
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_kill_before_admission_leaves_no_trace() {
    // The gateway is killed while the request body is still arriving: it was
    // never parsed, so nothing may be reserved or charged.
    let (provider, calls) = start_provider(Provider::Silent).await;
    let install = Install::new(provider);
    let gw = install.start().await;

    use tokio::io::AsyncWriteExt as _;
    let addr = gw.base.trim_start_matches("http://").to_string();
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(
        b"POST /v1/chat/stream HTTP/1.1\r\nhost: x\r\nx-api-key: key-app\r\ncontent-type: application/json\r\ncontent-length: 500\r\n\r\n{\"model\":",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    gw.kill();

    let _restarted = install.start().await;
    assert!(install.reservations().await.is_empty());
    assert_eq!(install.spent().await, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_kill_after_reserving_before_any_answer_bills_the_reservation() {
    // Reserved and sent to the provider, which has not answered. The
    // reservation was durable before the call, so it survives the kill; the
    // restarted gateway sweeps it and keeps the full reservation as the bill.
    let (provider, calls) = start_provider(Provider::Silent).await;
    let install = Install::new(provider);
    let gw = install.start().await;

    let pending = tokio::spawn(gw.chat(None).send());
    until("the provider has the request", || calls.load(Ordering::SeqCst) == 1).await;
    gw.kill();
    pending.abort();

    let _restarted = install.start().await;
    let rows = install.swept().await;
    assert_eq!(rows.len(), 1, "exactly the one reservation: {rows:?}");
    assert_eq!(rows[0].0, "swept");
    assert_eq!(install.spent().await, rows[0].1, "billed the full reservation");
    assert!(rows[0].1 > 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "never re-executed");
}

#[tokio::test]
async fn a_kill_mid_stream_bills_the_reservation() {
    let (provider, calls) = start_provider(Provider::StallsAfterFirstToken).await;
    let install = Install::new(provider);
    let gw = install.start().await;

    let mut response = gw.chat(None).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // Read until the first token has been delivered to the client.
    let mut seen = String::new();
    while !seen.contains("partial") {
        let chunk = tokio::time::timeout(Duration::from_secs(10), response.chunk())
            .await
            .expect("the first token arrives")
            .unwrap()
            .expect("the stream is open");
        seen.push_str(&String::from_utf8_lossy(&chunk));
    }
    gw.kill();
    drop(response);

    let _restarted = install.start().await;
    let rows = install.swept().await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].0, "swept");
    assert_eq!(install.spent().await, rows[0].1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_retry_across_a_kill_is_never_executed_twice() {
    // The client's request dies with the gateway, so it retries with the same
    // key against the restarted process. The first attempt's reservation is
    // still open: "in progress" until it is swept, then "duplicate". Never a
    // second execution.
    let (provider, calls) = start_provider(Provider::Silent).await;
    let install = Install::new(provider);
    let gw = install.start().await;

    let pending = tokio::spawn(gw.chat(Some("retry-1")).send());
    until("the provider has the request", || calls.load(Ordering::SeqCst) == 1).await;
    gw.kill();
    pending.abort();

    let restarted = install.start().await;
    let retry = restarted.chat(Some("retry-1")).send().await.unwrap();
    let status = retry.status();
    let body: Value = retry.json().await.unwrap();
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        ["request_in_progress", "duplicate_request"].contains(&body["error"]["code"].as_str().unwrap()),
        "{body}"
    );

    install.swept().await;
    let after = restarted.chat(Some("retry-1")).send().await.unwrap();
    assert_eq!(after.status(), StatusCode::CONFLICT);
    let body: Value = after.json().await.unwrap();
    assert_eq!(body["error"]["code"], "duplicate_request", "{body}");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "executed exactly once across the crash");
}
