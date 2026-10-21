//! Receipts end to end: kernel verdicts on both endpoints, what every
//! failure path writes, fail-closed, and the decision queue staying
//! best-effort when receipts are off.
//!
//! The mock provider picks its behaviour from the upstream model name:
//! `mock-model` answers, `mock-refuse` returns 500 before streaming,
//! `mock-break` sends "Hello" then an in-band error, and `mock-slow` sends
//! "partial" and then nothing more (as a completion, nothing at all).

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
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use futures_util::StreamExt;
use llm_gateway::{
    governance::ledger::decisions::{Decision, DecisionKind, DECISION_QUEUE},
    receipts::{RawReplyDigest, ReceiptChain, ReplyDigest, Salt, request_digest},
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

async fn handle(State(state): State<Mock>, Json(body): Json<Value>) -> Response {
    state.calls.fetch_add(1, Ordering::SeqCst);
    let model = body["model"].as_str().unwrap_or_default().to_string();
    if model == "mock-refuse" {
        let mut response = Response::new(Body::from(r#"{"error":{"message":"refused"}}"#));
        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        return response;
    }
    if body["stream"] != Value::Bool(true) {
        if model == "mock-slow" {
            std::future::pending::<()>().await;
        }
        return Json(serde_json::json!({
            "id": "c1", "object": "chat.completion", "model": "mock-model",
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "Hello, world"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}
        }))
        .into_response();
    }
    let frames: Vec<String> = match model.as_str() {
        "mock-break" => vec![
            sse(r#"{"choices":[{"index":0,"delta":{"content":"Hello"}}]}"#),
            sse(r#"{"error":{"message":"boom"}}"#),
        ],
        "mock-slow" => vec![sse(r#"{"choices":[{"index":0,"delta":{"content":"partial"}}]}"#)],
        _ => vec![
            sse(r#"{"choices":[{"index":0,"delta":{"content":"Hello"}}]}"#),
            sse(r#"{"choices":[{"index":0,"delta":{"content":", "}}]}"#),
            sse(r#"{"choices":[{"index":0,"delta":{"content":"world"}}]}"#),
            sse(r#"{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#),
            "data: [DONE]\n\n".to_string(),
        ],
    };
    let slow = model == "mock-slow";
    let stream = futures_util::stream::iter(frames.into_iter().map(Ok::<_, std::io::Error>)).chain(
        futures_util::stream::once(async move {
            if slow {
                std::future::pending::<()>().await;
            }
            Ok(String::new())
        }),
    );
    let body = Body::from_stream(stream);
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
  mock/refuse:
    provider: nvidia
    endpoint: http://{upstream}/v1/chat/completions
    upstream_model: mock-refuse
    default_params:
      max_tokens: 64
    cost:
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
  mock/break:
    provider: nvidia
    endpoint: http://{upstream}/v1/chat/completions
    upstream_model: mock-break
    default_params:
      max_tokens: 64
    cost:
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
  mock/slow:
    provider: nvidia
    endpoint: http://{upstream}/v1/chat/completions
    upstream_model: mock-slow
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
        let others = if receipts { "true" } else { "false" };
        let tenants = format!(
            r#"tenants:
  - tenant_id: acme
    enabled: true
    receipts: {flag}
{holds_yaml}    credentials:
      - key_id: ak_test
        key: test-key-acme
        scopes: [chat:stream, chat:complete, receipts:read]
      - key_id: ak_chat_only
        key: test-key-acme-chat-only
        scopes: [chat:stream]
    allowed_models: ["mock/chat", "mock/held", "mock/refuse", "mock/break", "mock/slow"]
  - tenant_id: globex
    enabled: true
    receipts: {others}
    credentials:
      - key_id: gk_test
        key: test-key-globex
        scopes: [chat:stream, receipts:read]
    allowed_models: ["mock/chat"]
  - tenant_id: plain
    enabled: true
    credentials:
      - key_id: pk_test
        key: test-key-plain
        scopes: [chat:stream, receipts:read]
    allowed_models: ["mock/chat"]
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
        // The tests below speak as tenant `acme`; its chain is its own.
        let chain = state.receipts.as_ref().map(|book| book.chain("acme").unwrap());
        let app = llm_gateway::api::router(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, chain, state }
    }

    async fn get(&self, path: &str, key: Option<&str>) -> (StatusCode, reqwest::header::HeaderMap, Vec<u8>) {
        let mut builder = reqwest::Client::new().get(format!("http://{}{path}", self.addr));
        if let Some(key) = key {
            builder = builder.header("x-api-key", key);
        }
        let response = builder.send().await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        (status, headers, response.bytes().await.unwrap().to_vec())
    }

    async fn post_as(&self, key: &str, body: &str) -> StatusCode {
        reqwest::Client::new()
            .post(format!("http://{}/v1/chat/stream", self.addr))
            .header("x-api-key", key)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap()
            .status()
    }

    async fn post(&self, body: &str) -> (StatusCode, String) {
        let (status, _, text) = self.post_with_headers(body).await;
        (status, text)
    }

    async fn post_with_headers(&self, body: &str) -> (StatusCode, reqwest::header::HeaderMap, String) {
        self.request("/v1/chat/stream", body, &[]).await
    }

    async fn request(
        &self,
        path: &str,
        body: &str,
        extra: &[(&str, &str)],
    ) -> (StatusCode, reqwest::header::HeaderMap, String) {
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

// ---------------------------------------------------------------------------
// Every failure path, with receipts on
// ---------------------------------------------------------------------------

fn rows(gateway: &Gateway) -> Vec<Value> {
    let chain = gateway.chain.as_ref().unwrap();
    std::fs::read_to_string(chain.log_path())
        .unwrap()
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn salt_of(headers: &reqwest::header::HeaderMap) -> Salt {
    Salt::from_hex(headers["x-receipt-salt"].to_str().unwrap()).expect("a salt header")
}

fn outcome_for<'a>(rows: &'a [Value], allow: &Value) -> Option<&'a Value> {
    rows.iter().find(|row| row["decision_receipt_id"] == allow["receipt_id"])
}

fn assert_verifies(gateway: &Gateway) {
    let chain = gateway.chain.as_ref().unwrap();
    let Some(python) = llm_gateway::receipts::test_python() else {
        return;
    };
    let out = std::process::Command::new(python)
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/vendor/wicket/verifier/ickverify.py"))
        .arg(chain.log_path())
        .arg("--trusted-keys")
        .arg(chain.public_key_path())
        .arg("--require-signatures")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stdout));
}

#[tokio::test]
async fn a_provider_refusal_closes_the_allow_with_a_failed_outcome_and_no_reply() {
    let (upstream, _mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    let (status, _, text) = gateway.post_with_headers(&stream_body("mock/refuse")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{text}");
    let rows = rows(&gateway);
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0]["verdict"], "allow");
    let outcome = outcome_for(&rows, &rows[0]).expect("an outcome");
    assert_eq!(outcome["status"], "failed");
    assert!(outcome["response_sha256"].is_null(), "nothing came back, so no reply digest: {outcome}");
    assert_verifies(&gateway);
}

#[tokio::test]
async fn a_mid_stream_error_records_exactly_what_was_delivered() {
    let (upstream, _mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    let (status, headers, text) = gateway.post_with_headers(&stream_body("mock/break")).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    // The gateway's contract: an in-band `error` event, then the usual `end`.
    assert!(text.contains("event: error"), "{text}");
    let rows = rows(&gateway);
    let outcome = outcome_for(&rows, &rows[0]).expect("an outcome");
    assert_eq!(outcome["status"], "failed");
    let mut delivered = ReplyDigest::new(salt_of(&headers));
    delivered.content("Hello");
    assert_eq!(outcome["response_sha256"], delivered.finish(), "the digest covers what the client got");
    assert_verifies(&gateway);
}

#[tokio::test]
async fn a_client_hanging_up_mid_stream_records_a_failed_outcome() {
    let (upstream, _mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    let body = stream_body("mock/slow");
    let mut response = reqwest::Client::new()
        .post(format!("http://{}/v1/chat/stream", gateway.addr))
        .header("x-api-key", "test-key-acme")
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    let salt = salt_of(response.headers());
    let mut seen = String::new();
    while !seen.contains("partial") {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(10), response.chunk())
            .await
            .expect("the first token arrives")
            .unwrap()
            .expect("the stream is open");
        seen.push_str(&String::from_utf8_lossy(&chunk));
    }
    drop(response);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let outcome = loop {
        let rows = rows(&gateway);
        if let Some(outcome) = outcome_for(&rows, &rows[0]) {
            break outcome.clone();
        }
        assert!(tokio::time::Instant::now() < deadline, "no outcome after the hang-up: {rows:?}");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert_eq!(outcome["status"], "failed");
    let mut delivered = ReplyDigest::new(salt);
    delivered.content("partial");
    assert_eq!(outcome["response_sha256"], delivered.finish());
    assert_verifies(&gateway);
}

#[tokio::test]
async fn a_passthrough_digest_covers_the_raw_bytes_relayed() {
    let (upstream, _mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    let body = stream_body("mock/chat");
    let (status, headers, text) =
        gateway.request("/v1/chat/stream", &body, &[("x-gateway-framing", "passthrough")]).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let rows = rows(&gateway);
    let outcome = outcome_for(&rows, &rows[0]).expect("an outcome");
    assert_eq!(outcome["status"], "completed");
    let mut raw = RawReplyDigest::new(&salt_of(&headers));
    raw.update(text.as_bytes());
    assert_eq!(outcome["response_sha256"], raw.finish());
    assert_eq!(outcome["request_sha256"], request_digest(&salt_of(&headers), body.as_bytes()));
}

#[tokio::test]
async fn a_stream_whose_outcome_cannot_be_written_ends_in_an_error_not_a_clean_end() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    let chain = gateway.chain.as_ref().unwrap();
    chain.fail_appends_after(1, 1);
    let (status, _, text) = gateway.post_with_headers(&stream_body("mock/chat")).await;
    assert_eq!(status, StatusCode::OK, "headers were already out: {text}");
    assert!(text.contains("receipt_unavailable"), "{text}");
    assert!(!text.contains("event: end"), "no clean end without the outcome: {text}");
    assert_eq!(chain.len(), 1, "the allow, and no outcome");
    assert_eq!(mock.calls.load(Ordering::SeqCst), 1);
    // The test hook fails one write without a disk error, so the chain goes
    // on; a real write error also stops the chain until restart.
    let (status, _, _) = gateway.post_with_headers(&stream_body("mock/chat")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(chain.len(), 3, "the unanswered allow stays in the log as it is");
    assert_verifies(&gateway);
}

// ---------------------------------------------------------------------------
// /v1/chat/complete
// ---------------------------------------------------------------------------

fn complete_body(model: &str) -> String {
    format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"{PROMPT}"}}],"params":{{"max_tokens":16}}}}"#)
}

#[tokio::test]
async fn a_completion_gets_an_allow_and_an_outcome_the_caller_can_check() {
    let (upstream, _mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    let body = complete_body("mock/chat");
    let (status, headers, text) = gateway.request("/v1/chat/complete", &body, &[]).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let answer: Value = serde_json::from_str(&text).unwrap();
    let salt = salt_of(&headers);
    let rows = rows(&gateway);
    assert_eq!(rows[0]["verdict"], "allow");
    assert_eq!(rows[0]["receipt_id"], headers["x-receipt-id"].to_str().unwrap());
    assert_eq!(rows[0]["call_digest"], request_digest(&salt, body.as_bytes()));
    let outcome = outcome_for(&rows, &rows[0]).expect("an outcome");
    assert_eq!(outcome["status"], "completed");
    // Recomputed from the JSON answer alone.
    let mut reply = ReplyDigest::new(salt);
    reply.content(answer["content"].as_str().unwrap());
    reply.reasoning(answer["reasoning"].as_str().unwrap_or_default());
    for (index, call) in answer["tool_calls"].as_array().unwrap().iter().enumerate() {
        reply.tool(
            index as u32,
            call["id"].as_str(),
            call["function"]["name"].as_str(),
            call["function"]["arguments"].as_str(),
        );
    }
    assert_eq!(outcome["response_sha256"], reply.finish());
    assert!(!std::fs::read_to_string(gateway.chain.as_ref().unwrap().log_path()).unwrap().contains(PROMPT));

    // A governance refusal on this endpoint is a deny too.
    let (status, _, _) = gateway.request("/v1/chat/complete", &complete_body("mock/denied"), &[]).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let rows = self::rows(&gateway);
    assert_eq!(rows.last().unwrap()["verdict"], "deny");
    assert_verifies(&gateway);
}

#[tokio::test]
async fn a_completion_refused_upstream_closes_with_a_failed_outcome() {
    let (upstream, _mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    let (status, _, text) = gateway.request("/v1/chat/complete", &complete_body("mock/refuse"), &[]).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{text}");
    let rows = rows(&gateway);
    let outcome = outcome_for(&rows, &rows[0]).expect("an outcome");
    assert_eq!(outcome["status"], "failed");
    assert!(outcome["response_sha256"].is_null());
}

#[tokio::test]
async fn a_completion_whose_outcome_cannot_be_written_is_withheld_and_not_replayable() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    let chain = gateway.chain.as_ref().unwrap();
    chain.fail_appends_after(1, 1);
    let key = [("idempotency-key", "withheld-1")];
    let (status, _, text) = gateway.request("/v1/chat/complete", &complete_body("mock/chat"), &key).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{text}");
    assert!(text.contains("receipt_unavailable"), "{text}");
    assert!(!text.contains("Hello"), "the answer is withheld: {text}");
    assert_eq!(mock.calls.load(Ordering::SeqCst), 1, "the model did answer");

    // The same key must not fetch the withheld answer from the ledger.
    let (status, _, text) = gateway.request("/v1/chat/complete", &complete_body("mock/chat"), &key).await;
    assert_ne!(status, StatusCode::OK, "{text}");
    assert!(!text.contains("Hello"), "a retry must not replay what was withheld: {text}");
}

#[tokio::test]
async fn a_client_hanging_up_on_a_pending_completion_leaves_the_allow_open() {
    let (upstream, mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    let url = format!("http://{}/v1/chat/complete", gateway.addr);
    let pending = tokio::spawn(
        reqwest::Client::new()
            .post(url)
            .header("x-api-key", "test-key-acme")
            .header("content-type", "application/json")
            .body(complete_body("mock/slow"))
            .send(),
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while mock.calls.load(Ordering::SeqCst) == 0 {
        assert!(tokio::time::Instant::now() < deadline, "the provider never got the request");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    pending.abort();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let rows = rows(&gateway);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["verdict"], "allow");
    assert!(
        outcome_for(&rows, &rows[0]).is_none(),
        "whether the provider finished is unknown, so no outcome is invented"
    );
    assert_verifies(&gateway);
}

// ---------------------------------------------------------------------------
// Per-tenant evidence
// ---------------------------------------------------------------------------

fn ids(log: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(log)
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str::<Value>(line).unwrap()["receipt_id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn a_tenant_downloads_only_its_own_evidence_and_can_verify_it_alone() {
    let (upstream, _mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    assert_eq!(gateway.post_as("test-key-acme", &stream_body("mock/chat")).await, StatusCode::OK);
    assert_eq!(gateway.post_as("test-key-globex", &stream_body("mock/held")).await, StatusCode::FORBIDDEN);

    let (status, headers, acme_log) = gateway.get("/v1/receipts/log", Some("test-key-acme")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-receipt-entries"], "2");
    let (_, _, globex_log) = gateway.get("/v1/receipts/log", Some("test-key-globex")).await;
    let (acme_ids, globex_ids) = (ids(&acme_log), ids(&globex_log));
    assert_eq!(acme_ids.len(), 2, "acme's allow and outcome");
    assert_eq!(globex_ids.len(), 1, "globex's refusal");
    assert!(acme_ids.iter().all(|id| !globex_ids.contains(id)));
    let acme_text = String::from_utf8_lossy(&acme_log);
    let globex_text = String::from_utf8_lossy(&globex_log);
    assert!(!acme_text.contains("gk_test") && !globex_text.contains("ak_test"), "no other tenant's key id");
    assert!(globex_text.contains("EXTERNAL_DENY"));

    // Everything a customer needs, from the API alone, verifies on its own.
    let (_, _, key) = gateway.get("/v1/receipts/key", Some("test-key-acme")).await;
    let (_, _, anchor) = gateway.get("/v1/receipts/anchor", Some("test-key-acme")).await;
    let chain = gateway.chain.as_ref().unwrap();
    assert_eq!(key, std::fs::read(chain.public_key_path()).unwrap());
    let out = std::env::temp_dir().join(format!("llm-gw-export-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("receipts.jsonl"), &acme_log).unwrap();
    std::fs::write(out.join("anchor.jsonl"), &anchor).unwrap();
    std::fs::write(out.join("signing.pub"), &key).unwrap();
    if let Some(python) = llm_gateway::receipts::test_python() {
        let status = std::process::Command::new(python)
            .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/vendor/wicket/verifier/ickverify.py"))
            .arg(out.join("receipts.jsonl"))
            .arg("--trusted-keys")
            .arg(out.join("signing.pub"))
            .arg("--require-signatures")
            .output()
            .unwrap()
            .status;
        assert!(status.success(), "the exported evidence must verify by itself");
    }
}

#[tokio::test]
async fn evidence_needs_a_credential_with_receipts_read() {
    let (upstream, _mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    for path in ["/v1/receipts/log", "/v1/receipts/anchor", "/v1/receipts/key"] {
        let (status, _, _) = gateway.get(path, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}");
        let (status, _, _) = gateway.get(path, Some("test-key-acme-chat-only")).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
        let (status, _, _) = gateway.get(path, Some("not-a-key")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}");
    }
}

#[tokio::test]
async fn a_tenant_without_receipts_has_no_evidence_to_fetch() {
    let (upstream, _mock) = mock_upstream().await;
    let gateway = Gateway::boot(upstream, true, true, false).await;
    let (status, _, body) = gateway.get("/v1/receipts/log", Some("test-key-plain")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(String::from_utf8_lossy(&body).contains("receipts_not_enabled"));
    let book = gateway.state.receipts.as_ref().unwrap();
    assert!(book.open_chain("plain").is_none(), "no chain is created for a tenant without receipts");
}
