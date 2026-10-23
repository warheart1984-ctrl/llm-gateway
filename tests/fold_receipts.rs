//! Conversation folding for tenants with receipts (docs/plans/folding-receipts.md).
//!
//! The mock provider answers the main model with "Hello, world" (streamed or
//! not) and the summarizer model with whatever summary the test sets. Every
//! request body it receives is kept, so a test can compare what the model was
//! sent with what the receipts and headers say.

use std::sync::{Arc, Mutex};

use axum::{
    body::Body,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use base64::Engine as _;
use llm_gateway::{
    folding::plan::SUMMARY_PREAMBLE,
    receipts::{fold_reply_digest, fold_request_digest, folded_request_digest, request_digest, ReceiptChain, Salt},
};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use uuid::Uuid;

#[derive(Clone)]
struct Mock {
    bodies: Arc<Mutex<Vec<Value>>>,
    summary: Arc<Mutex<String>>,
}

impl Mock {
    fn bodies(&self) -> Vec<Value> {
        self.bodies.lock().unwrap().clone()
    }

    fn summarizer_calls(&self) -> usize {
        self.bodies().iter().filter(|body| body["model"] == "mock-summarizer").count()
    }

    /// The last request the main model received.
    fn last_main(&self) -> Value {
        self.bodies().into_iter().rev().find(|body| body["model"] == "mock-model").expect("a main call")
    }
}

async fn mock_upstream() -> (String, Mock) {
    let state = Mock {
        bodies: Arc::new(Mutex::new(Vec::new())),
        summary: Arc::new(Mutex::new("The user and assistant exchanged filler turns.".into())),
    };
    let app = Router::new().route("/v1/chat/completions", post(handle)).with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr.to_string(), state)
}

async fn handle(State(state): State<Mock>, Json(body): Json<Value>) -> Response {
    state.bodies.lock().unwrap().push(body.clone());
    if body["model"] == "mock-summarizer" {
        let summary = state.summary.lock().unwrap().clone();
        return Json(json!({
            "id": "s1", "object": "chat.completion", "model": "mock-summarizer",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": summary}}],
            "usage": {"prompt_tokens": 50, "completion_tokens": 10, "total_tokens": 60}
        }))
        .into_response();
    }
    if body["stream"] != Value::Bool(true) {
        return Json(json!({
            "id": "c1", "object": "chat.completion", "model": "mock-model",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "Hello, world"}}],
            "usage": {"prompt_tokens": 50, "completion_tokens": 3, "total_tokens": 53}
        }))
        .into_response();
    }
    let frames = [
        r#"{"choices":[{"index":0,"delta":{"content":"Hello, world"}}]}"#,
        r#"{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
    ];
    let mut text: String = frames.iter().map(|frame| format!("data: {frame}\n\n")).collect();
    text.push_str("data: [DONE]\n\n");
    let mut response = Response::new(Body::from(text));
    response
        .headers_mut()
        .insert(axum::http::header::CONTENT_TYPE, axum::http::HeaderValue::from_static("text/event-stream"));
    response
}

struct Gateway {
    addr: String,
    chain: Arc<ReceiptChain>,
}

const FOLDING: &str = r#"    folding:
      enabled: true
      summarizer_model: mock/summarizer
      keep_last: 2
      summary_chars: 200
      summarizer_max_tokens: 64
      min_new_turns: 2
      min_new_tokens: 0
      amortization_requests: 1
"#;

fn config_dir(upstream: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("llm-gw-fold-receipts-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    let models = format!(
        r#"schema_version: 1
models:
  mock/chat:
    provider: nvidia
    endpoint: http://{upstream}/v1/chat/completions
    upstream_model: mock-model
    default_params:
      max_tokens: 64
    cost:
      input_per_mtok_usd: 2.0
      output_per_mtok_usd: 8.0
  mock/summarizer:
    provider: nvidia
    endpoint: http://{upstream}/v1/chat/completions
    upstream_model: mock-summarizer
    default_params:
      max_tokens: 64
    cost:
      input_per_mtok_usd: 0.01
      output_per_mtok_usd: 0.02
"#
    );
    let tenants = format!(
        r#"tenants:
  - tenant_id: acme
    enabled: true
    receipts: true
    credentials:
      - key_id: ak_test
        key: test-key-acme
        scopes: [chat:stream, chat:complete, receipts:read]
    allowed_models: ["mock/chat", "mock/summarizer"]
{FOLDING}"#
    );
    std::fs::write(dir.join("models.yaml"), models).unwrap();
    std::fs::write(dir.join("tenants.yaml"), tenants).unwrap();
    dir
}

fn settings(dir: &std::path::Path) -> llm_gateway::Settings {
    let mut settings = llm_gateway::Settings::default();
    settings.server.bind_addr = "127.0.0.1".into();
    settings.server.port = 0;
    settings.registry.models_path = dir.join("models.yaml");
    settings.registry.tenants_path = dir.join("tenants.yaml");
    settings.registry.hot_reload = false;
    settings.ledger.backend = llm_gateway::config::LedgerBackend::Memory;
    settings.receipts.enabled = true;
    settings.receipts.directory = dir.join("chain");
    settings.receipts.anchor_every = 100;
    settings
}

impl Gateway {
    async fn boot(upstream: &str) -> Self {
        let dir = config_dir(upstream);
        let state = llm_gateway::bootstrap::build(settings(&dir)).await.expect("boot");
        let chain = state.receipts.as_ref().unwrap().chain("acme").unwrap();
        let app = llm_gateway::api::router(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, chain }
    }

    async fn post(&self, path: &str, body: &str, extra: &[(&str, &str)]) -> (StatusCode, reqwest::header::HeaderMap, String) {
        let mut builder = reqwest::Client::new()
            .post(format!("http://{}{path}", self.addr))
            .header("x-api-key", "test-key-acme")
            .header("content-type", "application/json")
            .body(body.to_string());
        for (name, value) in extra {
            builder = builder.header(*name, *value);
        }
        let response = builder.send().await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        (status, headers, response.text().await.unwrap())
    }

    fn rows(&self) -> Vec<Value> {
        std::fs::read_to_string(self.chain.log_path())
            .unwrap()
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn entry(&self, receipt_id: &str) -> Value {
        self.rows().into_iter().find(|row| row["receipt_id"] == receipt_id).expect("an entry with that id")
    }

    fn outcome_of(&self, allow_id: &str) -> Value {
        self.rows().into_iter().find(|row| row["decision_receipt_id"] == allow_id).expect("an outcome for that allow")
    }
}

/// A conversation of `turns` filler turns, as the client sends it.
fn conversation(turns: usize, stream: bool) -> String {
    let messages: Vec<Value> = (0..turns)
        .map(|index| {
            json!({
                "role": if index % 2 == 0 { "user" } else { "assistant" },
                "content": format!("turn {index} {}", "x".repeat(200)),
            })
        })
        .collect();
    let mut body = json!({ "model": "mock/chat", "messages": messages, "params": { "max_tokens": 16 } });
    if stream {
        body["stream"] = json!(true);
    }
    body.to_string()
}

/// The same, with a value the summary must keep in its first turn.
fn conversation_with_pinned_value(turns: usize) -> String {
    let mut body: Value = serde_json::from_str(&conversation(turns, true)).unwrap();
    body["messages"][0]["content"] = json!(format!("The meeting moved to 15:30. {}", "x".repeat(200)));
    body.to_string()
}

fn position(rows: &[Value], receipt_id: &str) -> usize {
    rows.iter().position(|row| row["receipt_id"] == receipt_id).expect("an entry with that id")
}

fn header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).map(|value| value.to_str().unwrap())
}

fn decoded_summary(headers: &reqwest::header::HeaderMap) -> String {
    let encoded = header(headers, "x-gateway-fold-summary").expect("x-gateway-fold-summary");
    String::from_utf8(base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encoded).unwrap()).unwrap()
}

/// The summary inside the request the main model actually received.
fn forwarded_summary(main: &Value) -> String {
    let text = main["messages"][0]["content"].as_str().unwrap();
    text.strip_prefix(SUMMARY_PREAMBLE).and_then(|rest| rest.strip_prefix('\n')).expect("a summary message").to_string()
}

/// Checks a folded response against its receipts and what the model got.
fn assert_folded_binding(gateway: &Gateway, mock: &Mock, headers: &reqwest::header::HeaderMap, body: &str, replaced: usize) {
    assert_eq!(header(headers, "x-gateway-folded"), Some(replaced.to_string().as_str()));
    let summary = decoded_summary(headers);
    assert_eq!(summary, forwarded_summary(&mock.last_main()), "the header carries exactly what the model was sent");

    let salt = Salt::from_hex(header(headers, "x-receipt-salt").unwrap()).unwrap();
    let allow_id = header(headers, "x-receipt-id").unwrap();
    let allow = gateway.entry(allow_id);
    let call_digest = request_digest(&salt, body.as_bytes());
    assert_eq!(allow["call_digest"], call_digest, "the allow still binds the client's body");
    let outcome = gateway.outcome_of(allow_id);
    assert_eq!(
        outcome["request_sha256"],
        folded_request_digest(&salt, &call_digest, replaced, &summary),
        "the outcome binds the folded request"
    );
}

#[tokio::test]
async fn a_folded_stream_outcome_binds_what_the_model_was_sent() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(&upstream).await;
    let body = conversation(30, true);
    let (status, headers, text) = gateway.post("/v1/chat/stream", &body, &[]).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(mock.summarizer_calls(), 1);
    assert_folded_binding(&gateway, &mock, &headers, &body, 28);
}

#[tokio::test]
async fn a_folded_completion_outcome_binds_what_the_model_was_sent() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(&upstream).await;
    let body = conversation(30, false);
    let (status, headers, text) = gateway.post("/v1/chat/complete", &body, &[]).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_folded_binding(&gateway, &mock, &headers, &body, 28);
}

#[tokio::test]
async fn a_reused_summary_is_bound_the_same_way() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(&upstream).await;
    let first = conversation(30, true);
    assert_eq!(gateway.post("/v1/chat/stream", &first, &[]).await.0, StatusCode::OK);
    // One turn more: not enough new turns to fold again, so the committed
    // summary is reused under this request's own salt.
    let second = conversation(31, true);
    let (status, headers, text) = gateway.post("/v1/chat/stream", &second, &[]).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(mock.summarizer_calls(), 1, "no new fold");
    assert_folded_binding(&gateway, &mock, &headers, &second, 28);
}

#[tokio::test]
async fn an_unfolded_request_from_a_folding_tenant_is_receipted_as_today() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(&upstream).await;
    let body = conversation(2, true);
    let (status, headers, text) = gateway.post("/v1/chat/stream", &body, &[]).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(mock.summarizer_calls(), 0);
    assert!(header(&headers, "x-gateway-folded").is_none());
    assert!(header(&headers, "x-gateway-fold-summary").is_none());
    let salt = Salt::from_hex(header(&headers, "x-receipt-salt").unwrap()).unwrap();
    let allow_id = header(&headers, "x-receipt-id").unwrap();
    let call_digest = request_digest(&salt, body.as_bytes());
    assert_eq!(gateway.entry(allow_id)["call_digest"], call_digest);
    assert_eq!(gateway.outcome_of(allow_id)["request_sha256"], call_digest, "unfolded: the outcome binds the body");
    assert_eq!(gateway.rows().len(), 2, "one allow, one outcome");
}

#[tokio::test]
async fn an_idempotent_replay_returns_the_fold_headers() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(&upstream).await;
    let body = conversation(30, false);
    let key = [("idempotency-key", "fold-replay-1")];
    let (status, first, text) = gateway.post("/v1/chat/complete", &body, &key).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let (status, replay, text) = gateway.post("/v1/chat/complete", &body, &key).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(mock.summarizer_calls(), 1, "a replay runs no fold");
    for name in ["x-receipt-id", "x-receipt-salt", "x-gateway-folded", "x-gateway-fold-summary"] {
        assert_eq!(header(&replay, name), header(&first, name), "{name}");
    }
}

#[tokio::test]
async fn background_folding_with_a_receipts_tenant_refuses_to_start() {
    let (upstream, _mock) = mock_upstream().await;
    let dir = config_dir(&upstream);
    let mut settings = settings(&dir);
    settings.folding.background = true;
    let error = match llm_gateway::bootstrap::build(settings).await {
        Ok(_) => panic!("background folding with receipts must not start"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("acme") && error.contains("background"), "{error}");
}

// ---- Phase 2: the summarizer's own receipt (D3) --------------------------------------------

#[tokio::test]
async fn a_fold_writes_its_allow_then_calls_then_writes_its_outcome() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(&upstream).await;
    let body = conversation(30, true);
    let (status, headers, text) = gateway.post("/v1/chat/stream", &body, &[]).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let salt = Salt::from_hex(header(&headers, "x-receipt-salt").unwrap()).unwrap();
    let call_digest = request_digest(&salt, body.as_bytes());
    let fold_id = header(&headers, "x-gateway-fold-receipt-id").expect("x-gateway-fold-receipt-id").to_string();
    let main_id = header(&headers, "x-receipt-id").unwrap().to_string();

    let fold_allow = gateway.entry(&fold_id);
    assert_eq!(fold_allow["verdict"], "allow");
    // No prior summary: turns 0 to 28 are folded, from nothing.
    assert_eq!(fold_allow["call_digest"], fold_request_digest(&salt, &call_digest, 0, 28, ""));
    let fold_outcome = gateway.outcome_of(&fold_id);
    assert_eq!(fold_outcome["status"], "completed");
    assert_eq!(fold_outcome["request_sha256"], fold_allow["call_digest"]);
    assert_eq!(fold_outcome["response_sha256"], fold_reply_digest(&salt, &decoded_summary(&headers)));

    // In order: the fold's allow, its outcome, then the request's own allow.
    let rows = gateway.rows();
    let fold_outcome_at = rows.iter().position(|row| row["decision_receipt_id"] == fold_id.as_str()).unwrap();
    assert!(position(&rows, &fold_id) < fold_outcome_at);
    assert!(fold_outcome_at < position(&rows, &main_id));
    assert_eq!(rows.len(), 4, "a folded request writes four entries");
    assert_eq!(mock.summarizer_calls(), 1);
}

#[tokio::test]
async fn a_second_fold_binds_the_summary_it_extends() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(&upstream).await;
    let (_, first, _) = gateway.post("/v1/chat/stream", &conversation(30, true), &[]).await;
    let prior = decoded_summary(&first);
    *mock.summary.lock().unwrap() = "The conversation went on with more filler.".into();
    let body = conversation(34, true);
    let (status, headers, text) = gateway.post("/v1/chat/stream", &body, &[]).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(mock.summarizer_calls(), 2);
    let salt = Salt::from_hex(header(&headers, "x-receipt-salt").unwrap()).unwrap();
    let call_digest = request_digest(&salt, body.as_bytes());
    let fold_id = header(&headers, "x-gateway-fold-receipt-id").unwrap();
    assert_eq!(gateway.entry(fold_id)["call_digest"], fold_request_digest(&salt, &call_digest, 28, 32, &prior));
    assert_folded_binding(&gateway, &mock, &headers, &body, 32);
}

#[tokio::test]
async fn a_fold_whose_allow_cannot_be_written_calls_no_summarizer() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(&upstream).await;
    gateway.chain.fail_disk_writes_from_now(1);
    let (status, headers, text) = gateway.post("/v1/chat/stream", &conversation(30, true), &[]).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{text}");
    assert!(text.contains("receipt_unavailable"), "{text}");
    assert_eq!(mock.summarizer_calls(), 0, "no summarizer call without a durable allow");
    assert!(mock.bodies().is_empty(), "and no model call at all once the chain stopped");
    assert!(header(&headers, "x-gateway-fold-receipt-id").is_none());
}

#[tokio::test]
async fn a_rejected_fold_writes_a_failed_outcome_and_the_request_goes_out_unfolded() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(&upstream).await;
    // The summary drops the pinned "15:30", so the fold is refused.
    *mock.summary.lock().unwrap() = "Filler was exchanged.".into();
    let body = conversation_with_pinned_value(30);
    let (status, headers, text) = gateway.post("/v1/chat/stream", &body, &[]).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(mock.summarizer_calls(), 1);
    let fold_id = header(&headers, "x-gateway-fold-receipt-id").expect("a fold ran");
    let fold_outcome = gateway.outcome_of(fold_id);
    assert_eq!(fold_outcome["status"], "failed");
    assert!(fold_outcome.get("response_sha256").is_none_or(Value::is_null), "{fold_outcome}");

    assert!(header(&headers, "x-gateway-folded").is_none());
    assert!(header(&headers, "x-gateway-fold-summary").is_none());
    let salt = Salt::from_hex(header(&headers, "x-receipt-salt").unwrap()).unwrap();
    let call_digest = request_digest(&salt, body.as_bytes());
    let main_id = header(&headers, "x-receipt-id").unwrap();
    assert_eq!(gateway.outcome_of(main_id)["request_sha256"], call_digest, "sent unfolded, bound as such");
    assert_eq!(mock.last_main()["messages"].as_array().unwrap().len(), 30, "the model got the whole conversation");
}

#[tokio::test]
async fn the_unchanged_verifier_accepts_a_log_with_folds() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(&upstream).await;
    assert_eq!(gateway.post("/v1/chat/stream", &conversation(30, true), &[]).await.0, StatusCode::OK);
    *mock.summary.lock().unwrap() = "Filler was exchanged.".into();
    assert_eq!(gateway.post("/v1/chat/stream", &conversation_with_pinned_value(30), &[]).await.0, StatusCode::OK);
    assert_eq!(gateway.post("/v1/chat/stream", &conversation(2, true), &[]).await.0, StatusCode::OK);
    let Some(python) = llm_gateway::receipts::test_python() else {
        return;
    };
    let out = std::process::Command::new(python)
        .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor/wicket/verifier/ickverify.py"))
        .arg(gateway.chain.log_path())
        .arg("--trusted-keys")
        .arg(gateway.chain.public_key_path())
        .arg("--require-signatures")
        .output()
        .expect("run ickverify.py");
    assert_eq!(out.status.code(), Some(0), "{}", String::from_utf8_lossy(&out.stdout));
}
