//! Receipts on the stream path: kernel verdicts, fail-closed, and the
//! decision queue staying best-effort when receipts are off.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use axum::{
    body::Body,
    extract::State,
    http::StatusCode,
    response::Response,
    routing::post,
    Json, Router,
};
use llm_gateway::{
    governance::ledger::decisions::{Decision, DecisionKind, DECISION_QUEUE},
    receipts::{ReceiptChain, ReplyDigest, Salt, request_digest},
};
use serde_json::Value;
use tokio::net::TcpListener;
use uuid::Uuid;

#[derive(Clone)]
struct Mock {
    calls: Arc<AtomicU64>,
}

async fn mock_upstream() -> (SocketAddr, Mock) {
    let state = Mock { calls: Arc::new(AtomicU64::new(0)) };
    let app = Router::new()
        .route("/v1/chat/completions", post(handle))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, state)
}

async fn handle(State(state): State<Mock>, Json(_body): Json<Value>) -> Response {
    state.calls.fetch_add(1, Ordering::SeqCst);
    let frames = [
        sse(r#"{"choices":[{"index":0,"delta":{"content":"Hello"}}]}"#),
        sse(r#"{"choices":[{"index":0,"delta":{"content":", "}}]}"#),
        sse(r#"{"choices":[{"index":0,"delta":{"content":"world"}}]}"#),
        sse(r#"{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#),
        "data: [DONE]\n\n".to_string(),
    ];
    let body = Body::from_stream(futures_util::stream::iter(frames.into_iter().map(|frame| {
        Ok::<_, std::io::Error>(frame)
    })));
    let mut response = Response::new(body);
    response
        .headers_mut()
        .insert(axum::http::header::CONTENT_TYPE, axum::http::HeaderValue::from_static("text/event-stream"));
    response
}

fn sse(payload: &str) -> String {
    format!("data: {payload}\n\n")
}

struct Gateway {
    addr: SocketAddr,
    chain: Option<Arc<ReceiptChain>>,
    state: Arc<llm_gateway::AppState>,
}

impl Gateway {
    async fn boot(upstream: SocketAddr, receipts: bool, tenant_receipts: bool, holds: bool) -> Self {
        let dir = std::env::temp_dir().join(format!("llm-gw-receipt-http-{}", Uuid::new_v4().simple()));
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
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
  mock/denied:
    provider: nvidia
    endpoint: http://{upstream}/v1/chat/completions
    upstream_model: mock-model
    default_params:
      max_tokens: 64
    cost:
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
  mock/held:
    provider: nvidia
    endpoint: http://{upstream}/v1/chat/completions
    upstream_model: mock-model
    default_params:
      max_tokens: 64
    cost:
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
"#
        );
        let holds_yaml = if holds {
            "    holds:\n      models: [\"mock/held\"]\n"
        } else {
            ""
        };
        let flag = if tenant_receipts { "true" } else { "false" };
        let tenants = format!(
            r#"tenants:
  - tenant_id: acme
    enabled: true
    receipts: {flag}
{holds_yaml}    credentials:
      - key_id: ak_test
        key: test-key-acme
        scopes: [chat:stream]
    allowed_models: ["mock/chat", "mock/held"]
"#
        );
        std::fs::write(dir.join("models.yaml"), models).unwrap();
        std::fs::write(dir.join("tenants.yaml"), tenants).unwrap();
        let receipt_dir = dir.join("chain");
        let mut settings = llm_gateway::Settings::default();
        settings.server.bind_addr = "127.0.0.1".into();
        settings.server.port = 0;
        settings.registry.models_path = dir.join("models.yaml");
        settings.registry.tenants_path = dir.join("tenants.yaml");
        settings.registry.hot_reload = false;
        settings.ledger.backend = llm_gateway::config::LedgerBackend::Memory;
        settings.receipts.enabled = receipts;
        settings.receipts.directory = receipt_dir;
        settings.receipts.anchor_every = 100;
        let state = llm_gateway::bootstrap::build(settings).await.expect("boot");
        let chain = state.receipts.clone();
        let app = llm_gateway::api::router(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, chain, state }
    }

    async fn post(&self, body: &str) -> (StatusCode, String) {
        let (status, _, text) = self.post_with_headers(body).await;
        (status, text)
    }

    async fn post_with_headers(&self, body: &str) -> (StatusCode, reqwest::header::HeaderMap, String) {
        let response = reqwest::Client::new()
            .post(format!("http://{}/v1/chat/stream", self.addr))
            .header("x-api-key", "test-key-acme")
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let text = response.text().await.unwrap();
        (status, headers, text)
    }
}

const PROMPT: &str = "TOP-SECRET-PROMPT-xyz";

fn stream_body(model: &str) -> String {
    format!(
        r#"{{"model":"{model}","stream":true,"messages":[{{"role":"user","content":"{PROMPT}"}}],"params":{{"max_tokens":16}}}}"#
    )
}

#[tokio::test]
async fn stream_events_map_onto_kernel_receipts_an_auditor_can_verify() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, true).await;

    let body = stream_body("mock/chat");
    let (status, headers, text) = gateway.post_with_headers(&body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert!(text.contains("Hello"), "{text}");
    assert_eq!(mock.calls.load(Ordering::SeqCst), 1);
    let salt = Salt::from_hex(headers["x-receipt-salt"].to_str().unwrap()).expect("a salt header");
    let allow_id = headers["x-receipt-id"].to_str().unwrap().to_string();

    // A malformed request is not a governance decision: no receipt.
    let (status, text) = gateway.post(r#"{"model":"mock/chat","stream":true,"messages":[]}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(text.contains("no_messages"), "{text}");
    assert_eq!(gateway.chain.as_ref().unwrap().len(), 2, "a 400 writes no receipt");

    // A model the tenant may not use is: a 403 becomes a kernel deny.
    let (status, text) = gateway.post(&stream_body("mock/denied")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{text}");
    let refusal: Value = serde_json::from_str(&text).unwrap();
    let refusal_code = refusal["error"]["code"].as_str().unwrap().to_ascii_uppercase();

    let (status, text) = gateway.post(&stream_body("mock/held")).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{text}");
    assert!(text.contains("held"), "{text}");
    assert_eq!(mock.calls.load(Ordering::SeqCst), 1, "a hold must not call the model");

    let chain = gateway.chain.as_ref().unwrap();
    let log = std::fs::read_to_string(chain.log_path()).unwrap();
    assert!(!log.contains(PROMPT), "the log must not keep the prompt: {log}");
    assert!(!log.contains("Hello, world"), "the log must not keep the reply: {log}");
    let rows: Vec<Value> = log.lines().filter(|line| !line.is_empty()).map(|line| serde_json::from_str(line).unwrap()).collect();
    assert_eq!(rows[0]["verdict"], "allow");
    assert_eq!(rows[0]["receipt_id"], allow_id);
    // The client can recompute both digests from what it sent and received.
    assert_eq!(rows[0]["call_digest"], request_digest(&salt, body.as_bytes()));
    assert_eq!(rows[1]["version"], "infinity.outcome.v1");
    assert_eq!(rows[1]["status"], "completed");
    assert_eq!(rows[1]["request_sha256"], request_digest(&salt, body.as_bytes()));
    let mut reply = ReplyDigest::new(salt);
    reply.content("Hello, world");
    assert_eq!(rows[1]["response_sha256"], reply.finish());
    // Someone with only the log cannot test a guessed prompt.
    let unsalted = format!("sha256:{:x}", <sha2::Sha256 as sha2::Digest>::digest(body.as_bytes()));
    assert!(!log.contains(&unsalted), "{log}");
    assert!(rows[1].get("content").is_none());
    assert_eq!(rows[2]["verdict"], "deny");
    // The receipt says why: the gateway's own code, marked as decided outside the kernel.
    assert_eq!(rows[2]["reason_codes"], serde_json::json!(["EXTERNAL_DENY", refusal_code]));
    assert_eq!(rows[3]["verdict"], "await_human_approval");

    let Some(python) = llm_gateway::receipts::test_python() else {
        return;
    };
    let status = std::process::Command::new(python)
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/vendor/wicket/verifier/ickverify.py"))
        .arg(chain.log_path())
        .arg("--trusted-keys")
        .arg(chain.public_key_path())
        .arg("--require-signatures")
        .status()
        .unwrap();
    assert!(status.success(), "unchanged ickverify.py rejected the replica log");
}

#[tokio::test]
async fn a_failed_receipt_write_withholds_the_reply() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    gateway.chain.as_ref().unwrap().fail_next_appends(1);
    let (status, text) = gateway.post(&stream_body("mock/chat")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{text}");
    assert!(text.contains("receipt_unavailable"), "{text}");
    assert!(!text.contains(PROMPT), "{text}");
    assert!(!text.contains("Hello"), "{text}");
    assert_eq!(mock.calls.load(Ordering::SeqCst), 0, "the model must not be called");
    assert_eq!(gateway.chain.as_ref().unwrap().len(), 0);
}

#[tokio::test]
async fn receipts_off_still_answers_when_the_decision_queue_is_full() {
    let (upstream, _mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, false, false).await;
    let ledger = gateway.state.limits.ledger();
    for _ in 0..DECISION_QUEUE {
        ledger.record_decision(Decision {
            request_id: Uuid::new_v4(),
            tenant_id: "acme".into(),
            key_id: "ak_test".into(),
            kind: DecisionKind::Refused,
            endpoint: "stream".into(),
            model: None,
            code: "filled".into(),
            reason: "filled".into(),
        });
    }
    assert_eq!(ledger.decisions_dropped(), 0);
    let before = gateway.chain.as_ref().unwrap().len();
    let (status, text) = gateway.post(&stream_body("mock/denied")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{text}");
    assert!(!text.contains("receipt_unavailable"), "{text}");
    assert!(ledger.decisions_dropped() >= 1, "the full queue drops the record");
    assert_eq!(gateway.chain.as_ref().unwrap().len(), before, "receipts off writes nothing");
}

#[tokio::test]
async fn receipts_on_a_tenant_without_a_process_chain_refuses_to_boot() {
    let dir = std::env::temp_dir().join(format!("llm-gw-receipt-boot-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("models.yaml"),
        "schema_version: 1\nmodels:\n  mock/chat:\n    provider: nvidia\n    endpoint: http://127.0.0.1:9/v1/chat/completions\n    upstream_model: m\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("tenants.yaml"),
        "tenants:\n  - tenant_id: acme\n    enabled: true\n    receipts: true\n    credentials:\n      - key_id: ak\n        key: secret\n        scopes: [chat:stream]\n    allowed_models: [\"*\"]\n",
    )
    .unwrap();
    let mut settings = llm_gateway::Settings::default();
    settings.registry.models_path = dir.join("models.yaml");
    settings.registry.tenants_path = dir.join("tenants.yaml");
    settings.registry.hot_reload = false;
    settings.ledger.backend = llm_gateway::config::LedgerBackend::Memory;
    settings.receipts.enabled = false;
    match llm_gateway::bootstrap::build(settings).await {
        Err(err) => assert!(err.to_string().contains("receipts"), "{err}"),
        Ok(_) => panic!("a tenant with receipts on must not boot without a process chain"),
    }
}
