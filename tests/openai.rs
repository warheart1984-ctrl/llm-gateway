//! `POST /v1/chat/completions`: OpenAI's Chat Completions format in and out,
//! with the gateway's governance and receipts unchanged underneath.
//!
//! The mock provider picks its behaviour from the upstream model name:
//! `mock-model` answers "Hello, world", `mock-tools` calls a tool, and
//! `mock-break` sends "Hello" then fails in-band.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use llm_gateway::receipts::{ReplyDigest, Salt, request_digest};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use uuid::Uuid;

async fn mock_upstream() -> SocketAddr {
    let app = Router::new().route("/v1/chat/completions", post(handle));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

fn sse(payload: Value) -> String {
    format!("data: {payload}\n\n")
}

async fn handle(Json(body): Json<Value>) -> Response {
    let model = body["model"].as_str().unwrap_or_default().to_string();
    if body["stream"] != Value::Bool(true) {
        return Json(json!({
            "id": "up-1", "object": "chat.completion", "model": model,
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "Hello, world"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}
        }))
        .into_response();
    }
    let delta = |d: Value| sse(json!({"choices": [{"index": 0, "delta": d}]}));
    let mut frames = match model.as_str() {
        "mock-tools" => vec![
            delta(json!({"tool_calls": [{"index": 0, "id": "call_1", "type": "function",
                                         "function": {"name": "lookup", "arguments": ""}}]})),
            delta(json!({"tool_calls": [{"index": 0, "function": {"arguments": "{\"q\":"}}]})),
            delta(json!({"tool_calls": [{"index": 0, "function": {"arguments": "1}"}}]})),
            sse(json!({"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]})),
        ],
        "mock-break" => vec![delta(json!({"content": "Hello"})), sse(json!({"error": {"message": "boom"}}))],
        _ => vec![
            delta(json!({"content": "Hello"})),
            delta(json!({"content": ", world"})),
            sse(json!({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})),
        ],
    };
    if model != "mock-break" {
        frames.push(sse(json!({"choices": [], "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}})));
        frames.push("data: [DONE]\n\n".into());
    }
    let mut response = Response::new(Body::from_stream(futures_util::stream::iter(
        frames.into_iter().map(Ok::<_, std::io::Error>),
    )));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("text/event-stream"),
    );
    response
}

struct Gateway {
    addr: SocketAddr,
    state: Arc<llm_gateway::AppState>,
}

async fn boot() -> Gateway {
    let upstream = mock_upstream().await;
    let dir = std::env::temp_dir().join(format!("llm-gw-openai-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    let model = |id: &str, upstream_model: &str| {
        format!(
            "  {id}:\n    provider: nvidia\n    endpoint: http://{upstream}/v1/chat/completions\n    upstream_model: {upstream_model}\n    default_params:\n      max_tokens: 64\n    cost:\n      input_per_mtok_usd: 1.0\n      output_per_mtok_usd: 1.0\n"
        )
    };
    std::fs::write(
        dir.join("models.yaml"),
        format!(
            "schema_version: 1\nmodels:\n{}{}{}{}",
            model("mock/chat", "mock-model"),
            model("mock/tools", "mock-tools"),
            model("mock/break", "mock-break"),
            model("mock/other", "mock-model"),
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("tenants.yaml"),
        r#"tenants:
  - tenant_id: acme
    enabled: true
    receipts: true
    credentials:
      - key_id: ak_test
        key: test-key-acme
        scopes: [chat:stream, receipts:read]
    allowed_models: ["mock/chat", "mock/tools", "mock/break"]
"#,
    )
    .unwrap();
    let mut settings = llm_gateway::Settings::default();
    settings.server.bind_addr = "127.0.0.1".into();
    settings.server.port = 0;
    settings.registry.models_path = dir.join("models.yaml");
    settings.registry.tenants_path = dir.join("tenants.yaml");
    settings.registry.hot_reload = false;
    settings.ledger.backend = llm_gateway::config::LedgerBackend::Memory;
    settings.receipts.enabled = true;
    settings.receipts.directory = dir.join("receipts");
    let state = llm_gateway::bootstrap::build(settings).await.expect("boot");
    let app = llm_gateway::api::router(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Gateway { addr, state }
}

impl Gateway {
    /// The way an OpenAI SDK calls: `Authorization: Bearer`, JSON body.
    async fn call(&self, key: &str, body: &Value) -> (StatusCode, reqwest::header::HeaderMap, String) {
        let response = reqwest::Client::new()
            .post(format!("http://{}/v1/chat/completions", self.addr))
            .bearer_auth(key)
            .json(body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        (status, headers, response.text().await.unwrap())
    }

    fn log(&self) -> Vec<Value> {
        let chain = self.state.receipts.as_ref().unwrap().chain("acme").unwrap();
        std::fs::read_to_string(chain.log_path())
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
}

/// Every `data:` line of an SSE body, parsed; `[DONE]` as a string.
fn data_lines(text: &str) -> Vec<Value> {
    text.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|d| d.trim())
        .filter(|d| !d.is_empty())
        .map(|d| if d == "[DONE]" { Value::String(d.into()) } else { serde_json::from_str(d).unwrap() })
        .collect()
}

fn ask(model: &str) -> Value {
    json!({"model": model, "messages": [{"role": "user", "content": "say hello"}], "max_tokens": 16})
}

#[tokio::test]
async fn a_completion_comes_back_in_openai_shape() {
    let gw = boot().await;
    let (status, headers, text) = gw.call("test-key-acme", &ask("mock/chat")).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert!(body["id"].as_str().unwrap().starts_with("chatcmpl-"));
    assert!(body["created"].as_u64().unwrap() > 1_700_000_000);
    assert_eq!(body["model"], "mock/chat");
    assert_eq!(body["choices"][0]["index"], 0);
    assert_eq!(body["choices"][0]["message"]["role"], "assistant");
    assert_eq!(body["choices"][0]["message"]["content"], "Hello, world");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert_eq!(body["usage"]["total_tokens"], body["usage"]["prompt_tokens"].as_u64().unwrap() + body["usage"]["completion_tokens"].as_u64().unwrap());
    assert!(body.get("cost_nano_usd").is_none(), "no gateway-only fields in the OpenAI shape");
    assert!(headers.contains_key("x-receipt-id"), "receipts work the same");
}

#[tokio::test]
async fn a_stream_comes_back_as_openai_chunks_with_usage_when_asked() {
    let gw = boot().await;
    let mut body = ask("mock/chat");
    body["stream"] = json!(true);
    body["stream_options"] = json!({"include_usage": true});
    let (status, headers, text) = gw.call("test-key-acme", &body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert!(!text.contains("event:"), "no named events in OpenAI's format: {text}");
    let lines = data_lines(&text);
    assert_eq!(lines.last().unwrap(), "[DONE]");
    let chunks: Vec<&Value> = lines.iter().filter(|v| v.is_object()).collect();
    let id = chunks[0]["id"].as_str().unwrap();
    assert!(id.starts_with("chatcmpl-"));
    for chunk in &chunks {
        assert_eq!(chunk["object"], "chat.completion.chunk");
        assert_eq!(chunk["id"], id, "one id for the whole stream");
        assert_eq!(chunk["model"], "mock/chat");
    }
    assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
    let content: String = chunks
        .iter()
        .filter_map(|c| c["choices"].get(0).and_then(|ch| ch["delta"]["content"].as_str()))
        .collect();
    assert_eq!(content, "Hello, world");
    let finish = chunks.iter().find(|c| c["choices"].get(0).is_some_and(|ch| !ch["finish_reason"].is_null())).unwrap();
    assert_eq!(finish["choices"][0]["finish_reason"], "stop");
    let usage = chunks.last().unwrap();
    assert_eq!(usage["choices"], json!([]));
    assert!(usage["usage"]["total_tokens"].as_u64().unwrap() > 0);

    // The receipt covers what this client received, in this format.
    let salt = Salt::from_hex(headers["x-receipt-salt"].to_str().unwrap()).unwrap();
    let log = gw.log();
    assert_eq!(log[0]["call_digest"], request_digest(&salt, serde_json::to_vec(&body).unwrap().as_slice()));
    let mut reply = ReplyDigest::new(salt);
    reply.content(&content);
    assert_eq!(log[1]["response_sha256"], reply.finish());
}

#[tokio::test]
async fn without_include_usage_there_is_no_usage_chunk() {
    let gw = boot().await;
    let mut body = ask("mock/chat");
    body["stream"] = json!(true);
    let (_, _, text) = gw.call("test-key-acme", &body).await;
    assert!(data_lines(&text).iter().all(|v| v.get("usage").is_none()), "{text}");
}

#[tokio::test]
async fn tool_calls_stream_as_openai_tool_call_deltas() {
    let gw = boot().await;
    let mut body = ask("mock/tools");
    body["stream"] = json!(true);
    let (status, _, text) = gw.call("test-key-acme", &body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let lines = data_lines(&text);
    let calls: Vec<&Value> = lines
        .iter()
        .filter_map(|v| v["choices"].get(0).and_then(|c| c["delta"]["tool_calls"].get(0)))
        .collect();
    assert_eq!(calls[0]["index"], 0);
    assert_eq!(calls[0]["id"], "call_1");
    assert_eq!(calls[0]["type"], "function");
    assert_eq!(calls[0]["function"]["name"], "lookup");
    let arguments: String = calls.iter().filter_map(|c| c["function"]["arguments"].as_str()).collect();
    assert_eq!(arguments, "{\"q\":1}");
    assert!(lines.iter().any(|v| v["choices"].get(0).is_some_and(|c| c["finish_reason"] == "tool_calls")));
}

#[tokio::test]
async fn a_failure_mid_stream_is_an_openai_error_line() {
    let gw = boot().await;
    let mut body = ask("mock/break");
    body["stream"] = json!(true);
    let (status, _, text) = gw.call("test-key-acme", &body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let lines = data_lines(&text);
    let error = lines.iter().find(|v| v.get("error").is_some()).expect("an error line");
    assert!(error["error"]["message"].is_string());
    assert!(error["error"]["code"].is_string());
    assert!(
        lines.iter().all(|v| v["choices"].get(0).is_none_or(|c| c["finish_reason"].is_null())),
        "a failed stream does not also claim a normal finish: {text}"
    );
    let log = gw.log();
    assert_eq!(log.last().unwrap()["status"], "failed");
}

#[tokio::test]
async fn refusals_use_openai_error_bodies_and_governance_still_applies() {
    let gw = boot().await;
    for (key, body, status, code) in [
        ("wrong-key", ask("mock/chat"), StatusCode::UNAUTHORIZED, "unauthorized"),
        ("test-key-acme", ask("mock/other"), StatusCode::FORBIDDEN, "model_not_allowed"),
        ("test-key-acme", { let mut b = ask("mock/chat"); b["n"] = json!(2); b }, StatusCode::BAD_REQUEST, "unsupported_parameter"),
        ("test-key-acme", json!({"model": "mock/chat", "messages": []}), StatusCode::BAD_REQUEST, "no_messages"),
    ] {
        let (got, _, text) = gw.call(key, &body).await;
        assert_eq!(got, status, "{text}");
        let error: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(error["error"]["code"], code, "{text}");
        assert!(error["error"]["message"].is_string() && error["error"]["type"].is_string(), "{text}");
    }
    // The 403 is a governance refusal, so it is on the tenant's chain.
    assert_eq!(gw.log().last().unwrap()["verdict"], "deny");
}

#[tokio::test]
async fn an_idempotency_key_never_replays_an_answer_across_formats() {
    let gw = boot().await;
    let native = reqwest::Client::new()
        .post(format!("http://{}/v1/chat/complete", gw.addr))
        .header("x-api-key", "test-key-acme")
        .header("idempotency-key", "same-key")
        .json(&ask("mock/chat"))
        .send()
        .await
        .unwrap();
    assert_eq!(native.status(), StatusCode::OK);
    let response = reqwest::Client::new()
        .post(format!("http://{}/v1/chat/completions", gw.addr))
        .bearer_auth("test-key-acme")
        .header("idempotency-key", "same-key")
        .json(&ask("mock/chat"))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_ne!(status, StatusCode::OK, "a gateway-shaped answer must not be replayed to an OpenAI client: {text}");
}

#[tokio::test]
async fn mode_fields_of_the_wrong_type_are_refused_not_guessed() {
    let gw = boot().await;
    let with = |patch: Value| {
        let mut body = ask("mock/chat");
        for (k, v) in patch.as_object().unwrap() {
            body[k] = v.clone();
        }
        body
    };
    for body in [
        with(json!({"stream": "true"})),
        with(json!({"stream": 1})),
        with(json!({"stream_options": {"include_usage": true}})),
        with(json!({"stream": false, "stream_options": {"include_usage": true}})),
        with(json!({"stream": true, "stream_options": "yes"})),
        with(json!({"stream": true, "stream_options": {"include_usage": "yes"}})),
        with(json!({"n": 1.5})),
    ] {
        let (status, _, text) = gw.call("test-key-acme", &body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body} -> {text}");
        let error: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(error["error"]["type"], "invalid_request_error", "{text}");
    }
    // The valid forms still work.
    // `null` means "not set", as on OpenAI's API.
    for body in [
        with(json!({"stream": false})),
        with(json!({"stream": null})),
        with(json!({"n": 1})),
        with(json!({"n": null})),
        with(json!({"stream_options": null})),
        with(json!({"stream": true, "stream_options": {}})),
    ] {
        let (status, _, text) = gw.call("test-key-acme", &body).await;
        assert_eq!(status, StatusCode::OK, "{body} -> {text}");
    }
}

#[tokio::test]
async fn a_replayed_completion_still_points_at_its_receipt() {
    let gw = boot().await;
    for path in ["/v1/chat/completions", "/v1/chat/complete"] {
        let key = format!("replay-{}", Uuid::new_v4().simple());
        let send = || async {
            let response = reqwest::Client::new()
                .post(format!("http://{}{path}", gw.addr))
                .bearer_auth("test-key-acme")
                .header("idempotency-key", &key)
                .json(&ask("mock/chat"))
                .send()
                .await
                .unwrap();
            let headers = response.headers().clone();
            (response.status(), headers, response.text().await.unwrap())
        };
        let (status, first, first_body) = send().await;
        assert_eq!(status, StatusCode::OK, "{path}: {first_body}");
        let (status, replay, replay_body) = send().await;
        assert_eq!(status, StatusCode::OK, "{path}: {replay_body}");
        assert_eq!(replay["idempotent-replayed"], "true", "{path}");
        assert_eq!(replay_body, first_body, "{path}: the stored answer, unchanged");
        assert_eq!(replay.get("x-receipt-id"), first.get("x-receipt-id"), "{path}: the replay names the original receipt");
        assert_eq!(replay.get("x-receipt-salt"), first.get("x-receipt-salt"), "{path}: and its salt, so the digests can be checked");
    }
}
