//! End-to-end tests against a mock upstream.
//!
//! The mock speaks the same OpenAI SSE dialect as the real vendors but is
//! driven by a scenario enum, so every branch that is awkward to provoke
//! against a paid API — mid-stream error, usage on a separate terminal chunk,
//! interleaved reasoning, a slow first token — is deterministic here.
//!
//! Tests bind an ephemeral port, so they are safe to run in parallel.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use futures_util::{Stream, StreamExt as _};
use serde_json::{json, Value};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Mock upstream
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    /// Three content deltas, usage on the terminal chunk, then `[DONE]`.
    Happy,
    /// Reasoning tokens interleaved with content on separate delta keys.
    Reasoning,
    /// An in-band `{"error": ...}` object after some content.
    MidStreamError,
    /// A non-2xx status before the stream starts.
    UpstreamReject,
    /// First token delayed, to make TTFT observable.
    SlowFirstToken,
    /// Tool call deltas split across frames.
    ToolCalls,
    /// A comment line plus a `data:`-only frame, as OpenRouter emits.
    CommentKeepalive,
}

#[derive(Clone)]
struct MockState {
    scenario: Scenario,
    calls: Arc<AtomicU64>,
    last_body: Arc<std::sync::Mutex<Option<Value>>>,
}

struct MockUpstream {
    addr: SocketAddr,
    state: MockState,
}

impl MockUpstream {
    async fn start(scenario: Scenario) -> Self {
        let state = MockState {
            scenario,
            calls: Arc::new(AtomicU64::new(0)),
            last_body: Arc::new(std::sync::Mutex::new(None)),
        };
        let app = Router::new()
            .route("/v1/chat/completions", post(handle))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("mock addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, state }
    }

    fn calls(&self) -> u64 {
        self.state.calls.load(Ordering::SeqCst)
    }

    fn last_body(&self) -> Option<Value> {
        self.state.last_body.lock().unwrap().clone()
    }

}

fn sse(payload: &str) -> String {
    format!("data: {payload}\n\n")
}

fn chunk(id: &str, content: &str) -> String {
    sse(&json!({
        "id": id,
        "object": "chat.completion.chunk",
        "model": "mock-model",
        "choices": [{ "index": 0, "delta": { "content": content } }]
    })
    .to_string())
}

fn sse_stream(items: Vec<String>, tail_delay: Option<Duration>) -> impl Stream<Item = Result<String, std::io::Error>> {
    futures_util::stream::unfold(
        (items.into_iter(), tail_delay),
        move |(mut items, delay)| async move {
            match items.next() {
                Some(item) => Some((Ok(item), (items, delay))),
                None => {
                    if let Some(d) = delay {
                        tokio::time::sleep(d).await;
                    }
                    None
                }
            }
        },
    )
}

async fn handle(State(state): State<MockState>, Json(body): Json<Value>) -> Response {
    state.calls.fetch_add(1, Ordering::SeqCst);
    *state.last_body.lock().unwrap() = Some(body);



    match state.scenario {
        Scenario::UpstreamReject => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": { "message": "rate limit exceeded", "code": "rate_limited" } })),
        )
            .into_response(),
        Scenario::Happy => sse_response(vec![
            chunk("c1", "Hello"),
            chunk("c1", ", "),
            chunk("c1", "world"),
            sse(&json!({
                "id": "c1",
                "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }]
            })
            .to_string()),
            sse(&json!({
                "id": "c1",
                "choices": [],
                "usage": { "prompt_tokens": 12, "completion_tokens": 3, "total_tokens": 15 }
            })
            .to_string()),
            "data: [DONE]\n\n".to_string(),
        ]),
        Scenario::Reasoning => sse_response(vec![
            sse(&json!({ "id": "c2", "choices": [{ "index": 0, "delta": { "reasoning_content": "let me think" } }] })
                .to_string()),
            chunk("c2", "answer"),
            sse(&json!({ "id": "c2", "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }).to_string()),
            "data: [DONE]\n\n".to_string(),
        ]),
        Scenario::MidStreamError => sse_response(vec![
            chunk("c3", "partial"),
            sse(
                &json!({ "error": { "message": "upstream exploded", "code": "internal" } }).to_string(),
            ),
        ]),
        Scenario::SlowFirstToken => {
            // The first frame is delayed, so the gateway's TTFT measurement is
            // distinguishable from zero.
            let body = Body::from_stream(
                futures_util::stream::once(async {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    Ok::<_, std::io::Error>(chunk("c4", "eventual"))
                })
                .chain(futures_util::stream::once(async {
                    Ok::<_, std::io::Error>("data: [DONE]\n\n".to_string())
                })),
            );
            let mut resp = Response::new(body);
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("text/event-stream"),
            );
            resp
        }
        Scenario::ToolCalls => sse_response(vec![
            sse(&json!({
                "id": "c5",
                "choices": [{ "index": 0, "delta": { "tool_calls": [{
                    "index": 0, "id": "call_1", "function": { "name": "get_weather", "arguments": "" }
                }]}}]
            })
            .to_string()),
            sse(&json!({
                "id": "c5",
                "choices": [{ "index": 0, "delta": { "tool_calls": [{
                    "index": 0, "function": { "arguments": "{\"city\":" }
                }]}}]
            })
            .to_string()),
            sse(&json!({
                "id": "c5",
                "choices": [{ "index": 0, "delta": { "tool_calls": [{
                    "index": 0, "function": { "arguments": "\"NYC\"}" }
                }]}}]
            })
            .to_string()),
            sse(&json!({ "id": "c5", "choices": [{ "index": 0, "delta": {}, "finish_reason": "tool_calls" }] }).to_string()),
            "data: [DONE]\n\n".to_string(),
        ]),
        Scenario::CommentKeepalive => sse_response(vec![
            ": keepalive comment\n\n".to_string(),
            ": another\n\n".to_string(),
            chunk("c6", "ok"),
            "data: [DONE]\n\n".to_string(),
        ]),
    }
}

fn sse_response(frames: Vec<String>) -> Response {
    let mut resp = Response::new(Body::from_stream(sse_stream(frames, None)));
    let h = resp.headers_mut();
    h.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
    resp
}

// ---------------------------------------------------------------------------
// Gateway harness
// ---------------------------------------------------------------------------

const TEST_KEY: &str = "test-key-acme";

/// A model with no `default_params.max_tokens`, so a caller that supplies none
/// leaves the gateway with no cost ceiling.
const UNCAPPED_MODEL: &str = r#"  mock/uncapped:
    provider: nvidia
    endpoint: http://REPLACE_ME/v1/chat/completions
    upstream_model: mock-model
    cost:
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
"#;

struct Gateway {
    addr: SocketAddr,
    _dir: tempdir::TempDir,
}

impl Gateway {
    /// The tenant roster. `throttled` and `broke` exist so rate limiting and
    /// budget exhaustion can be driven from a test without reconfiguring
    /// `acme`, whose generous limits keep the other cases readable.
    async fn start(upstream: &MockUpstream) -> Self {
        Self::start_with(upstream, UNCAPPED_MODEL).await
    }

    async fn start_with(upstream: &MockUpstream, extra_models: &str) -> Self {
        let dir = tempdir::TempDir::new("llm-gateway-test");
        // Test-provided models may carry a placeholder endpoint, so substitute
        // the live mock address into every entry.
        let extra_models = extra_models.replace("REPLACE_ME", &upstream.addr.to_string());
        let models = format!(
            r#"schema_version: 1
models:
  mock/chat:
    provider: nvidia
    endpoint: http://{}/v1/chat/completions
    upstream_model: mock-model
    default_params:
      temperature: 0.5
      max_tokens: 256
    cost:
      input_per_mtok_usd: 2.0
      output_per_mtok_usd: 8.0
    aliases: [mock]
{}
"#,
            upstream.addr, extra_models
        );
        let tenants = r#"
tenants:
  - tenant_id: acme
    enabled: true
    credentials:
      - key_id: ak_test
        key: test-key-acme
        scopes: [chat:stream, models:read, admin]
    allowed_models: ["*"]
    limits:
      requests_per_minute: 60
      tokens_per_minute: 200000
      max_concurrent_streams: 8
      max_output_tokens: 4096
      daily_budget_micro_usd: 100000000
    model_params:
      mock/chat:
        temperature: 0.1
  - tenant_id: locked
    enabled: true
    credentials:
      - key_id: ak_locked
        key: test-key-locked
        scopes: [chat:stream]
    allowed_models: ["groq/only-this"]
    limits:
      requests_per_minute: 2
      tokens_per_minute: 1000
      max_concurrent_streams: 1
      max_output_tokens: 64
      daily_budget_micro_usd: 100000000
  - tenant_id: throttled
    enabled: true
    credentials:
      - key_id: ak_throttled
        key: test-key-throttled
        scopes: [chat:stream]
    allowed_models: ["*"]
    limits:
      requests_per_minute: 2
      tokens_per_minute: 200000
      max_concurrent_streams: 8
      max_output_tokens: 4096
      daily_budget_micro_usd: 100000000
  - tenant_id: broke
    enabled: true
    credentials:
      - key_id: ak_broke
        key: test-key-broke
        scopes: [chat:stream]
    allowed_models: ["*"]
    limits:
      requests_per_minute: 60
      tokens_per_minute: 200000
      max_concurrent_streams: 8
      max_output_tokens: 4096
      daily_budget_micro_usd: 50
  - tenant_id: nocreds
    enabled: true
    credentials:
      - key_id: ak_nc
        key: test-key-nocreds
        scopes: [models:read]
    allowed_models: ["*"]
    limits:
      requests_per_minute: 1
      tokens_per_minute: 1
      max_concurrent_streams: 1
      max_output_tokens: 1
      daily_budget_micro_usd: 1
"#;
        std::fs::write(dir.path().join("models.yaml"), models).unwrap();
        std::fs::write(dir.path().join("tenants.yaml"), tenants).unwrap();

        let settings = llm_gateway::config::Settings {
            server: llm_gateway::config::ServerConfig {
                bind_addr: "127.0.0.1".into(),
                port: 0,
                metrics_enabled: true,
                ..Default::default()
            },
            registry: llm_gateway::config::RegistryConfig {
                models_path: dir.path().join("models.yaml"),
                tenants_path: dir.path().join("tenants.yaml"),
                hot_reload: false,
                reload_interval_ms: 60_000,
            },
            auth: llm_gateway::config::AuthConfig {
                allow_anonymous: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let state = llm_gateway::bootstrap::build(settings)
            .await
            .expect("build gateway state");
        let app = llm_gateway::api::router(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind gateway");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        // Give the listener a moment to start accepting.
        tokio::time::sleep(Duration::from_millis(30)).await;
        Self { addr, _dir: dir }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    async fn post_chat(&self, key: &str, body: Value) -> (StatusCode, HeaderMap, String) {
        let client = reqwest::Client::new();
        let resp = client
            .post(self.url("/v1/chat/stream"))
            .header("x-api-key", key)
            .json(&body)
            .send()
            .await
            .expect("post chat");
        let status = resp.status();
        let headers = resp.headers().clone();
        let text = resp.text().await.unwrap_or_default();
        (status, headers, text)
    }

    async fn get(&self, path: &str, key: &str) -> (StatusCode, String) {
        let client = reqwest::Client::new();
        let resp = client
            .get(self.url(path))
            .header("x-api-key", key)
            .send()
            .await
            .expect("get");
        (resp.status(), resp.text().await.unwrap_or_default())
    }
}

fn chat_body() -> Value {
    json!({
        "model": "mock/chat",
        "messages": [{ "role": "user", "content": "hi" }],
        "stream": true,
        "params": { "max_tokens": 128 }
    })
}

/// Parse an SSE transcript into `(event_name, data)` pairs.
fn parse_sse(text: &str) -> Vec<(Option<String>, String)> {
    let mut out = Vec::new();
    let mut event: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            if !data_lines.is_empty() {
                out.push((event.take(), data_lines.join("\n")));
                data_lines.clear();
            }
            event = None;
            continue;
        }
        if let Some(rest) = line.strip_prefix(':') {
            let _ = rest;
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.trim_start().to_string());
        }
    }
    if !data_lines.is_empty() {
        out.push((event, data_lines.join("\n")));
    }
    out
}

fn tokens_in(text: &str) -> Vec<String> {
    parse_sse(text)
        .into_iter()
        .filter(|(ev, _)| ev.is_none())
        .filter_map(|(_, data)| serde_json::from_str::<Value>(&data).ok())
        .filter_map(|v| v.get("token").and_then(Value::as_str).map(str::to_string))
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn streams_tokens_under_the_v1_contract() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

        let (status, headers, body) = gw.post_chat(TEST_KEY, chat_body()).await;
        assert_eq!(status, StatusCode::OK, "body: {body}");

        // Contract headers a client depends on.
    assert_eq!(headers.get("x-gateway-version").unwrap(), "v1");
    assert_eq!(headers.get("x-model").unwrap(), "mock/chat");
    assert_eq!(headers.get("x-provider").unwrap(), "nvidia");
    assert!(headers.get("x-request-id").is_some(), "every response carries a request id");
    let cache = headers.get("cache-control").unwrap().to_str().unwrap();
    assert!(cache.contains("no-transform"), "intermediaries must not buffer: {cache}");
    assert_eq!(headers.get("x-accel-buffering").unwrap(), "no");

    assert_eq!(tokens_in(&body), vec!["Hello", ", ", "world"]);

    // Print the transcript when an assertion fails; an SSE bug is hard to read
    // from a diff of a serde_json::Value.
    let events = parse_sse(&body);
    assert_eq!(events.first().map(|(e, _)| e.as_deref()), Some(Some("start")), "transcript: {events:?}");
    assert!(
        events.iter().any(|(e, _)| e.as_deref() == Some("end")),
        "missing terminal `end` frame: {events:?}"
    );
    assert_eq!(events.last().unwrap().1, "[DONE]", "transcript: {events:?}");

    let end = events
        .iter()
        .find(|(e, _)| e.as_deref() == Some("end"))
        .map(|(_, d)| serde_json::from_str::<Value>(d).unwrap())
        .expect("end frame");
    assert_eq!(end["finish_reason"], "stop");
    assert_eq!(end["usage"]["prompt_tokens"], 12);
    assert_eq!(end["usage"]["completion_tokens"], 3);
    // 12 * 2 + 3 * 8 micro-USD.
    assert_eq!(end["cost_micro_usd"], 48);
}

#[tokio::test]
async fn reasoning_is_never_mixed_into_tokens() {
    let upstream = MockUpstream::start(Scenario::Reasoning).await;
    let gw = Gateway::start(&upstream).await;
    let (_, _, body) = gw.post_chat(TEST_KEY, chat_body()).await;

    // The visible token stream must not contain the chain of thought.
    assert_eq!(tokens_in(&body), vec!["answer"]);

    let events = parse_sse(&body);
    let reasoning = events
        .iter()
        .find(|(e, _)| e.as_deref() == Some("reasoning"))
        .map(|(_, d)| serde_json::from_str::<Value>(d).unwrap());
    assert_eq!(reasoning.unwrap()["reasoning"], "let me think");
}

#[tokio::test]
async fn mid_stream_error_is_reported_as_an_event_not_a_silent_truncation() {
    let upstream = MockUpstream::start(Scenario::MidStreamError).await;
    let gw = Gateway::start(&upstream).await;

    // HTTP is 200 because the stream had already started; the failure has to be
    // visible in-band, and the transcript must not look like a clean finish.
    let (status, _, body) = gw.post_chat(TEST_KEY, chat_body()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tokens_in(&body), vec!["partial"]);

    let events = parse_sse(&body);
    let err = events
        .iter()
        .find(|(e, _)| e.as_deref() == Some("error"))
        .map(|(_, d)| serde_json::from_str::<Value>(d).unwrap())
        .expect("error event");
    assert!(err["error"]["message"].as_str().unwrap().contains("upstream exploded"), "{err}");
    // An in-band `error` object is an upstream failure reported with HTTP 200,
    // so the adapter synthesises a 502 and the code is `upstream_error`.
    assert_eq!(err["error"]["code"], "upstream_error");
    // The provider sent a 200 and then failed, so the adapter cannot tell a
    // transient fault from a deterministic one. Reporting it as retryable is
    // the safer default: a caller that retries loses nothing, and one that
    // treats it as fatal would drop a stream that may have been recoverable.
    assert_eq!(err["error"]["retryable"], true);
}

#[tokio::test]
async fn a_refused_upstream_is_not_counted_as_a_stream() {
    // An upstream 401 produced no tokens. Counting it as a stream would inflate
    // the completion rate and hide real regressions, so it lands on the error
    // counters instead.
    let upstream = MockUpstream::start(Scenario::UpstreamReject).await;
    let gw = Gateway::start(&upstream).await;

    let (status, _, _) = gw.post_chat(TEST_KEY, chat_body()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);

    let (_, metrics) = gw.get("/metrics", "").await;
    assert!(metrics.contains("gw_streams_started_total 0"), "{metrics}");
    assert!(metrics.contains("gw_upstream_errors_total 1"), "{metrics}");
    assert!(
        metrics.contains("gw_model_errors_total{provider=\"nvidia\",model=\"mock/chat\"} 1"),
        "{metrics}"
    );
    // The request still consumed the reservation, so cost is accounted.
    let (_, usage) = gw.get("/v1/usage", TEST_KEY).await;
    let usage: Value = serde_json::from_str(&usage).unwrap();
    assert_eq!(usage["streams_in_flight"], 0, "the slot must be released");
}

#[tokio::test]
async fn upstream_4xx_before_streaming_becomes_a_gateway_status() {
    let upstream = MockUpstream::start(Scenario::UpstreamReject).await;
    let gw = Gateway::start(&upstream).await;

    let (status, headers, body) = gw.post_chat(TEST_KEY, chat_body()).await;
    // Pre-stream failures are real HTTP statuses, not a 200 with a broken body.
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let err: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(err["error"]["code"], "upstream_rate_limited");
    assert_eq!(err["error"]["retryable"], true);
    assert!(headers.get("x-error-retryable").is_some());
}

#[tokio::test]
async fn tool_call_deltas_are_forwarded_intact() {
    let upstream = MockUpstream::start(Scenario::ToolCalls).await;
    let gw = Gateway::start(&upstream).await;
    let (_, _, body) = gw.post_chat(TEST_KEY, chat_body()).await;

    let tool_frames: Vec<Value> = parse_sse(&body)
        .into_iter()
        .filter(|(e, _)| e.as_deref() == Some("tool"))
        .map(|(_, d)| serde_json::from_str(&d).unwrap())
        .collect();

    assert_eq!(tool_frames.len(), 3);
    assert_eq!(tool_frames[0]["name"], "get_weather");
    assert_eq!(tool_frames[1]["arguments"], "{\"city\":");
    assert_eq!(tool_frames[2]["arguments"], "\"NYC\"}");
    // No content tokens: a tool call is not a token.
    assert!(tokens_in(&body).is_empty());
}

#[tokio::test]
async fn passthrough_relays_upstream_bytes_verbatim() {
    let upstream = MockUpstream::start(Scenario::CommentKeepalive).await;
    let gw = Gateway::start(&upstream).await;

    let mut body = chat_body();
    body["framing"] = json!("passthrough");
    let (status, headers, text) = gw.post_chat(TEST_KEY, body).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-framing").unwrap(), "passthrough");

    // Comments survive: the whole point of passthrough is that the gateway
    // does not touch the byte stream.
    assert!(text.contains(": keepalive comment"), "comments dropped: {text}");
    assert!(text.contains("data: [DONE]"));

    // And it really is the upstream dialect, not the normalized contract.
    let first: Value = serde_json::from_str(
        &text.lines().find(|l| l.starts_with("data: {")).unwrap()["data: ".len()..],
    )
    .unwrap();
    assert_eq!(first["choices"][0]["delta"]["content"], "ok");
}

#[tokio::test]
async fn registry_defaults_are_merged_but_the_request_wins() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    // No params at all: registry default max_tokens applies.
    let (status, _, _) = gw
        .post_chat(
            TEST_KEY,
            json!({
                "model": "mock/chat",
                "messages": [{ "role": "user", "content": "hi" }],
                "stream": true
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let sent = upstream.last_body().unwrap();
    assert_eq!(sent["max_tokens"], 256);
    // Tenant policy in `tenants.yaml` overrides the registry default.
    assert_eq!(sent["temperature"], 0.1);
    assert_eq!(sent["stream"], true);

    // Explicit request params beat the registry default.
    let mut body = chat_body();
    body["params"] = json!({ "max_tokens": 64, "temperature": 0.9 });
    gw.post_chat(TEST_KEY, body).await;
    let sent = upstream.last_body().unwrap();
    assert_eq!(sent["max_tokens"], 64);
    // ...but not the tenant policy, which is governance rather than a
    // preference. `acme` pins `temperature: 0.1` for this model, and a
    // constraint a caller can override is not a constraint.
    assert_eq!(sent["temperature"], 0.1);
}

#[tokio::test]
async fn unknown_vendor_parameters_pass_through_untouched() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let mut body = chat_body();
    body["params"] = json!({ "max_tokens": 32, "top_k": 40, "repetition_penalty": 1.1 });
    let (status, _, _) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::OK);

    let sent = upstream.last_body().unwrap();
    // A parameter the gateway has never heard of still reaches the provider.
    assert_eq!(sent["top_k"], 40);
    assert_eq!(sent["repetition_penalty"], 1.1);
}

#[tokio::test]
async fn a_caller_cannot_smuggle_stream_false_past_the_gateway() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let mut body = chat_body();
    body["params"] = json!({ "max_tokens": 32, "stream": false, "model": "attacker/model" });
    let (status, _, _) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::OK);

    let sent = upstream.last_body().unwrap();
    assert_eq!(sent["stream"], true);
    assert_eq!(sent["model"], "mock-model");
}

#[tokio::test]
async fn the_api_key_is_never_sent_to_the_client() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;
    let (_, _, body) = gw.post_chat(TEST_KEY, chat_body()).await;
    assert!(!body.contains(TEST_KEY), "credential echoed in the stream body");
}

#[tokio::test]
async fn model_not_on_the_allowlist_is_forbidden() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let extra = r#"  mock/other:
    provider: nvidia
    endpoint: http://127.0.0.1:9/v1/chat/completions
    upstream_model: mock-model
    cost:
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
"#;
    let gw = Gateway::start_with(&upstream, &format!("{extra}{UNCAPPED_MODEL}")).await;

    // `acme` allows "*", so this must reach the (unreachable) endpoint instead
    // of being denied by policy.
    let mut body = chat_body();
    body["model"] = json!("mock/other");
    let (status, _, _) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "policy should have allowed it");

    // `locked` only allows groq/only-this.
    let mut body = chat_body();
    body["model"] = json!("mock/chat");
    let (status, _, text) = gw.post_chat("test-key-locked", body).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(text.contains("model_not_allowed"), "{text}");
}

#[tokio::test]
async fn max_tokens_above_the_tenant_ceiling_is_clamped() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let mut body = chat_body();
    body["params"] = json!({ "max_tokens": 100_000 });
    let (status, _, _) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::OK);

    let sent = upstream.last_body().unwrap();
    assert_eq!(sent["max_tokens"], 4096, "must be clamped to the tenant ceiling");
}

#[tokio::test]
async fn missing_credentials_are_rejected_before_any_upstream_call() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let (status, _, body) = gw.post_chat("wrong-key", chat_body()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(body.contains("unauthorized"), "{body}");
    assert_eq!(upstream.calls(), 0, "a rejected request must not reach the provider");
}

#[tokio::test]
async fn a_credential_without_the_chat_scope_cannot_stream() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let (status, _, body) = gw.post_chat("test-key-nocreds", chat_body()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("forbidden"), "{body}");
    assert_eq!(upstream.calls(), 0);
}

#[tokio::test]
async fn missing_max_tokens_is_refused_because_cost_cannot_be_reserved() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    // The registry sets `default_params.max_tokens: 256`, so a caller who omits
    // `params` entirely still gets a cap and a cost reservation.
    let (status, _, body) = gw
        .post_chat(
            TEST_KEY,
            json!({
                "model": "mock/chat",
                "messages": [{ "role": "user", "content": "hi" }],
                "stream": true
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(upstream.last_body().unwrap()["max_tokens"], 256);

    // A model with no registry cap and no caller value has no ceiling at all.
    // The cost reservation is impossible, so the request is refused before any
    // connection is opened.
    let before = upstream.calls();
    let body = json!({
        "model": "mock/uncapped",
        "messages": [{ "role": "user", "content": "hi" }],
        "stream": true
    });
    let (status, _, body) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("max_tokens_required"), "{body}");
    assert_eq!(upstream.calls(), before, "must not reach the provider");
}

#[tokio::test]
async fn rate_limit_returns_429_with_retry_after() {
    // `throttled` is configured at 2 requests/minute.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    for i in 0..2 {
        let (status, _, body) = gw.post_chat("test-key-throttled", chat_body()).await;
        assert_eq!(status, StatusCode::OK, "request {i} should be admitted: {body}");
    }

    let (status, headers, body) = gw.post_chat("test-key-throttled", chat_body()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers.get("retry-after").unwrap(), "60");
    let err: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(err["error"]["code"], "rate_limited");
    assert_eq!(err["error"]["type"], "rate_limit_error");
    // The third request never reached the provider.
    assert_eq!(upstream.calls(), 2);
}

#[tokio::test]
async fn budget_exhaustion_returns_402_before_spending_anything() {
    // `broke` has a 50 micro-USD daily budget. A request reserving prompt plus
    // 128 output tokens at 2/8 micro-USD per token costs 10 + 1024 micro-USD,
    // which is more than the whole budget.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let (status, _, body) = gw.post_chat("test-key-broke", chat_body()).await;
    assert_eq!(status, StatusCode::PAYMENT_REQUIRED, "{body}");
    let err: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(err["error"]["code"], "budget_exhausted");
    assert_eq!(err["error"]["type"], "billing_error");
    assert_eq!(upstream.calls(), 0, "a budget refusal must not open a connection");
}

#[tokio::test]
async fn usage_endpoint_reports_the_callers_own_tenant() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;
    gw.post_chat(TEST_KEY, chat_body()).await;

    let (status, text) = gw.get("/v1/usage", TEST_KEY).await;
    assert_eq!(status, StatusCode::OK);
    let usage: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(usage["tenant_id"], "acme");
    assert_eq!(usage["requests_last_minute"], 1);
    assert_eq!(usage["spent_micro_usd"], 48);
    assert_eq!(usage["streams_in_flight"], 0);
    assert!(usage["budget_day"].as_str().unwrap().len() == 10);
}

#[tokio::test]
async fn models_endpoint_lists_only_permitted_models() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let extra = r#"  mock/other:
    provider: nvidia
    endpoint: http://127.0.0.1:9/v1/chat/completions
    upstream_model: mock-model
    cost:
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
"#;
    let gw = Gateway::start_with(&upstream, &format!("{extra}{UNCAPPED_MODEL}")).await;

    let (status, text) = gw.get("/v1/models", TEST_KEY).await;
    assert_eq!(status, StatusCode::OK);
    let listed: Value = serde_json::from_str(&text).unwrap();
    let ids: Vec<&str> = listed["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"mock/chat"));
    // `acme` allows "*", so both are listed.
    assert!(ids.contains(&"mock/other"));

    // `locked` sees only its own allowlist.
    let (_, text) = gw.get("/v1/models", "test-key-locked").await;
    let listed: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(listed["data"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn metrics_expose_request_and_stream_counters() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;
    gw.post_chat(TEST_KEY, chat_body()).await;

    let (status, text) = gw.get("/metrics", "").await;
    assert_eq!(status, StatusCode::OK);
    assert!(text.contains("gw_streams_completed_total 1"), "{text}");
    assert!(text.contains("gw_prompt_tokens_total 12"), "{text}");
    assert!(text.contains("gw_completion_tokens_total 3"), "{text}");
    assert!(text.contains("gw_cost_micro_usd_total 48"), "{text}");
    assert!(text.contains("gw_tenant_streams_total{tenant=\"acme\"} 1"), "{text}");
    assert!(text.contains("# TYPE gw_streams_completed_total counter"));
}

#[tokio::test]
async fn health_endpoints_do_not_require_credentials() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let (status, _) = gw.get("/health/live", "").await;
    assert_eq!(status, StatusCode::OK);

    let (status, text) = gw.get("/health/ready", "").await;
    assert_eq!(status, StatusCode::OK);
    let ready: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(ready["status"], "ready");
    assert_eq!(ready["models"], 2, "mock/chat and mock/uncapped");
}

#[tokio::test]
async fn an_oversized_body_is_rejected_with_413() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    // Declare a 2 MiB body but send none of it. The gateway rejects on the
    // declared length alone, then closes; streaming the body through reqwest
    // races that close, and on Windows the client can see a reset before it
    // reads the 413.
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut sock = tokio::net::TcpStream::connect(gw.addr).await.expect("connect");
    let head = format!(
        "POST /v1/chat/stream HTTP/1.1\r\nhost: {}\r\nx-api-key: {TEST_KEY}\r\n\
         content-type: application/json\r\ncontent-length: {}\r\n\r\n",
        gw.addr,
        2 * 1024 * 1024
    );
    sock.write_all(head.as_bytes()).await.expect("write head");
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut buf))
        .await
        .expect("gateway answered without waiting for the body")
        .expect("read status line");
    let status_line = String::from_utf8_lossy(&buf[..n]);
    assert!(status_line.starts_with("HTTP/1.1 413"), "got {status_line:?}");
    assert_eq!(upstream.calls(), 0);
}

#[tokio::test]
async fn an_unsupported_message_role_is_a_400() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let (status, _, text) = gw
        .post_chat(
            TEST_KEY,
            json!({
                "model": "mock/chat",
                "messages": [{ "role": "wizard", "content": "hi" }],
                "stream": true,
                "params": { "max_tokens": 32 }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text.contains("invalid_json"), "{text}");
    assert_eq!(upstream.calls(), 0);
}

#[tokio::test]
async fn an_unknown_model_is_a_404() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let mut body = chat_body();
    body["model"] = json!("mock/does-not-exist");
    let (status, _, text) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(text.contains("not_found"), "{text}");
    assert_eq!(upstream.calls(), 0);
}

#[tokio::test]
async fn an_out_of_range_parameter_is_rejected_before_connecting() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    // `top_p` rather than `temperature`: `acme` pins `temperature: 0.1` in
    // policy, which would mask an out-of-range value before validation.
    let mut body = chat_body();
    body["params"] = json!({ "max_tokens": 32, "top_p": 99.0 });
    let (status, _, text) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(text.contains("invalid_parameter"), "{text}");
    assert_eq!(upstream.calls(), 0, "must not connect to validate a parameter");
}

#[tokio::test]
async fn ttft_is_measured_and_reported() {
    let upstream = MockUpstream::start(Scenario::SlowFirstToken).await;
    let gw = Gateway::start(&upstream).await;

    let (status, _, body) = gw.post_chat(TEST_KEY, chat_body()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tokens_in(&body), vec!["eventual"]);

    let end = parse_sse(&body)
        .into_iter()
        .find(|(e, _)| e.as_deref() == Some("end"))
        .map(|(_, d)| serde_json::from_str::<Value>(&d).unwrap())
        .expect("end frame");
    let ttft = end["ttft_ms"].as_u64().expect("ttft_ms present");
    assert!(ttft >= 100, "ttft should reflect the upstream delay, got {ttft}ms");
}

#[tokio::test]
async fn client_disconnect_releases_quota_and_stops_upstream_work() {
    // A client that hangs up must not keep holding a concurrency slot or a
    // cost reservation, and the upstream connection must close rather than
    // generating tokens nobody will read.
    let upstream = MockUpstream::start(Scenario::SlowFirstToken).await;
    let gw = Gateway::start(&upstream).await;

    // Open a stream and drop the response body mid-flight.
    {
        let client = reqwest::Client::new();
        let resp = client
            .post(gw.url("/v1/chat/stream"))
            .header("x-api-key", TEST_KEY)
            .json(&chat_body())
            .send()
            .await
            .expect("stream started");
        assert_eq!(resp.status(), StatusCode::OK);
        // Dropping `resp` closes the body, which drops the gateway stream.
    }

    // The slot is released, so the tenant can stream again.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (status, _, body) = gw.post_chat(TEST_KEY, chat_body()).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // And the interrupted stream is recorded as a client abort, not a success.
    let (_, metrics) = gw.get("/metrics", "").await;
    assert!(
        metrics.contains("gw_streams_client_aborted_total 1"),
        "expected one client abort: {metrics}"
    );
}

#[tokio::test]
async fn a_disconnected_stream_still_settles_its_prompt_cost() {
    // Cost is reserved before the connection opens, so an abort cannot be a way
    // to spend for free. The prompt half is kept, the completion half refunded.
    let upstream = MockUpstream::start(Scenario::SlowFirstToken).await;
    let gw = Gateway::start(&upstream).await;

    {
        let client = reqwest::Client::new();
        let resp = client
            .post(gw.url("/v1/chat/stream"))
            .header("x-api-key", TEST_KEY)
            .json(&chat_body())
            .send()
            .await
            .expect("stream started");
        assert_eq!(resp.status(), StatusCode::OK);
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    let (status, text) = gw.get("/v1/usage", TEST_KEY).await;
    assert_eq!(status, StatusCode::OK);
    let usage: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(usage["streams_in_flight"], 0, "the slot must be released");
    // Prompt is a few tokens at 2 micro-USD each; completion is refunded.
    let spent = usage["spent_micro_usd"].as_u64().unwrap();
    assert!(spent > 0, "an abort must still be billed for its prompt: {text}");
    assert!(spent < 2_048, "the completion reservation must be refunded: {text}");
}

#[tokio::test]
async fn an_interrupted_stream_does_not_leak_concurrency_slots() {
    // `throttled` allows 8 concurrent streams. Opening and abandoning more than
    // that must not exhaust the tenant permanently.
    let upstream = MockUpstream::start(Scenario::SlowFirstToken).await;
    let gw = Gateway::start(&upstream).await;

    for _ in 0..12 {
        let client = reqwest::Client::new();
        let _ = client
            .post(gw.url("/v1/chat/stream"))
            .header("x-api-key", "test-key-throttled")
            .json(&chat_body())
            .send()
            .await;
        // Response dropped immediately.
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (status, text) = gw.get("/v1/usage", "test-key-throttled").await;
    assert_eq!(status, StatusCode::OK);
    let usage: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(usage["streams_in_flight"], 0, "slots leaked: {text}");
}

// ---------------------------------------------------------------------------
// Minimal temp dir
// ---------------------------------------------------------------------------
// Minimal temp dir, to avoid a dev-dependency
// ---------------------------------------------------------------------------

mod tempdir {
    use std::path::{Path, PathBuf};

    pub struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        pub fn new(prefix: &str) -> Self {
            let unique = uuid::Uuid::new_v4();
            let path = std::env::temp_dir().join(format!("{prefix}-{unique}"));
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
