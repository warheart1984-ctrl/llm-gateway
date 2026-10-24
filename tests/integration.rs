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
    /// `Happy`, but response headers wait for a permit on the mock's gate, so
    /// a test can hold any number of streams open at the upstream.
    Gated,
    /// Content deltas and a finish, but no usage anywhere.
    NoUsage,
    /// A 404 whose body names the operator's account and quotes a key, the
    /// shape NVIDIA answers with for a function the account cannot see.
    LeakyReject,
    /// An in-band `{"error": ...}` carrying the same account id and key.
    LeakyInBand,
    /// Non-streaming summarizer replies used to exercise fail-closed checks.
    SummarizerEmpty,
    SummarizerTooLong,
    SummarizerPinnedDrop,
    SummarizerLength,
    SummarizerReasoningRetry,
    /// The first non-streaming call is a provider error. The main stream still runs.
    SummarizerDown,
    /// First summarizer call succeeds. The next one is empty. Streams stay happy.
    SummarizerThenEmpty,
    /// First summarizer call succeeds. The next one is a provider error.
    SummarizerThenDown,
    /// Non-streaming calls wait on the gate. Streams return immediately.
    SummarizerHold,
    /// The first summarizer call reports a fact ("appointment" = 15:30); the
    /// next reports none. Streams stay happy.
    SummarizerFacts,
}

/// Stands in for the operator's provider account id in [`Scenario::LeakyReject`]
/// and [`Scenario::LeakyInBand`]. Must reach the logs, never a tenant.
const ACCOUNT_MARKER: &str = "acct-7c1e9f02d4";
/// A key-shaped string in the same bodies. Must reach neither.
const LEAKED_KEY: &str = "nvapi-leakedKey0123456789abcdef";

fn leaky_detail() -> String {
    format!("Function 'f-1': Not found for account '{ACCOUNT_MARKER}' (key {LEAKED_KEY})")
}

#[derive(Clone)]
struct MockState {
    scenario: Scenario,
    calls: Arc<AtomicU64>,
    gate: Arc<tokio::sync::Semaphore>,
    last_body: Arc<std::sync::Mutex<Vec<Value>>>,
    complete_calls: Arc<AtomicU64>,
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
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
            last_body: Arc::new(std::sync::Mutex::new(Vec::new())),
            complete_calls: Arc::new(AtomicU64::new(0)),
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
        self.state.last_body.lock().unwrap().last().cloned()
    }

    fn bodies(&self) -> Vec<Value> {
        self.state.last_body.lock().unwrap().clone()
    }

    fn open_gate(&self, permits: usize) {
        self.state.gate.add_permits(permits);
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

/// The gateway's summarizer asks for a JSON object of fact updates and a
/// context note. The scenarios below answer it in text; a summarizer reply is
/// wrapped as that object with no updates, so each scenario keeps its meaning.
/// `SummarizerTooLong`'s first reply is left as it is: not the JSON asked for.
async fn handle(state: State<MockState>, body: Json<Value>) -> Response {
    let summarizer = body.0["messages"][0]["content"]
        .as_str()
        .is_some_and(|text| text.starts_with("You keep the record of a conversation"));
    let unwrapped_first = state.0.scenario == Scenario::SummarizerTooLong && state.0.complete_calls.load(Ordering::SeqCst) == 0;
    let response = handle_upstream(state, body).await;
    if !summarizer || unwrapped_first {
        return response;
    }
    let (parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.expect("mock body");
    let Ok(mut answer) = serde_json::from_slice::<Value>(&bytes) else {
        return Response::from_parts(parts, Body::from(bytes));
    };
    if let Some(text) = answer["choices"][0]["message"]["content"].as_str().map(str::to_string)
        && !text.is_empty()
        && !serde_json::from_str::<Value>(&text).is_ok_and(|parsed| parsed.is_object())
    {
        answer["choices"][0]["message"]["content"] = json!(json!({"updates": [], "context": text}).to_string());
    }
    let mut parts = parts;
    parts.headers.remove(header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(answer.to_string()))
}

async fn handle_upstream(State(state): State<MockState>, Json(body): Json<Value>) -> Response {
    state.calls.fetch_add(1, Ordering::SeqCst);
    let streaming = body["stream"] != json!(false);
    state.last_body.lock().unwrap().push(body);

    let scenario = match state.scenario {
        Scenario::Gated => {
            state.gate.acquire().await.expect("gate open").forget();
            Scenario::Happy
        }
        Scenario::SummarizerHold | Scenario::SummarizerThenDown => Scenario::Happy,
        other => other,
    };

    let call_number = state.calls.load(Ordering::SeqCst);
    let complete_call = if !streaming {
        state.complete_calls.fetch_add(1, Ordering::SeqCst) + 1
    } else {
        0
    };
    if !streaming && state.scenario == Scenario::SummarizerHold {
        state.gate.acquire().await.expect("gate open").forget();
        return completion_answer(Scenario::Happy).await;
    }
    if !streaming && state.scenario == Scenario::SummarizerThenDown {
        if complete_call == 1 {
            return completion_answer(Scenario::Happy).await;
        }
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": { "message": "summarizer down", "code": "upstream_error" } })),
        )
            .into_response();
    }
    if !streaming && state.scenario == Scenario::SummarizerFacts {
        let reply = if complete_call == 1 {
            json!({"instructions": [{"text": "Standing rule: always answer in one sentence."}],
                   "updates": [{"field": "appointment", "value": "15:30", "evidence": "The appointment moved to 15:30"}],
                   "context": "first note"})
        } else {
            json!({"updates": [], "context": "second note"})
        };
        return Json(json!({
            "id": "summarizer-facts",
            "model": "mock-summarizer",
            "choices": [{ "index": 0, "message": { "role": "assistant", "content": reply.to_string() }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 7, "completion_tokens": 4, "total_tokens": 11 }
        }))
        .into_response();
    }
    if !streaming && state.scenario == Scenario::SummarizerDown && complete_call == 1 {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": { "message": "summarizer down", "code": "upstream_error" } })),
        )
            .into_response();
    }
    if !streaming && state.scenario == Scenario::SummarizerThenEmpty {
        if complete_call == 1 {
            return completion_answer(Scenario::Happy).await;
        }
        if complete_call == 2 {
            return summarizer_failure_answer(Scenario::SummarizerEmpty);
        }
    }
    if !streaming
        && (call_number == 1
            || (scenario == Scenario::SummarizerLength && call_number == 2))
    {
        if matches!(
            scenario,
            Scenario::SummarizerEmpty
                | Scenario::SummarizerTooLong
                | Scenario::SummarizerPinnedDrop
                | Scenario::SummarizerLength
        ) {
            return summarizer_failure_answer(scenario);
        }
        if scenario == Scenario::SummarizerReasoningRetry {
            return Json(json!({
                "id": "summarizer-1",
                "model": "mock-summarizer",
                "choices": [{
                    "index": 0,
                    "message": { "content": "", "reasoning_content": "thinking" },
                    "finish_reason": "length"
                }],
                "usage": { "prompt_tokens": 7, "completion_tokens": 64, "total_tokens": 71 }
            }))
            .into_response();
        }
    }

    if !streaming && !matches!(scenario, Scenario::UpstreamReject | Scenario::LeakyReject) {
        return completion_answer(
            if matches!(
                scenario,
                Scenario::SummarizerEmpty
                    | Scenario::SummarizerTooLong
                    | Scenario::SummarizerPinnedDrop
                    | Scenario::SummarizerLength
                    | Scenario::SummarizerReasoningRetry
                    | Scenario::SummarizerDown
                    | Scenario::SummarizerThenEmpty
            ) {
                Scenario::Happy
            } else {
                scenario
            },
        )
        .await;
    }

    match scenario {
        Scenario::Gated | Scenario::SummarizerHold | Scenario::SummarizerThenDown => {
            unreachable!("rewritten to Happy above")
        }
        Scenario::NoUsage => sse_response(vec![
            chunk("c7", "one two three four"),
            chunk("c7", " five six seven eight"),
            sse(&json!({ "id": "c7", "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }).to_string()),
            "data: [DONE]\n\n".to_string(),
        ]),
        Scenario::UpstreamReject => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": { "message": "rate limit exceeded", "code": "rate_limited" } })),
        )
            .into_response(),
        Scenario::LeakyReject => (
            StatusCode::NOT_FOUND,
            Json(json!({ "status": 404, "title": "Not Found", "detail": leaky_detail() })),
        )
            .into_response(),
        Scenario::LeakyInBand => sse_response(vec![
            chunk("c8", "partial"),
            sse(&json!({ "error": { "message": leaky_detail(), "code": "internal" } }).to_string()),
        ]),
        Scenario::Happy
        | Scenario::SummarizerEmpty
        | Scenario::SummarizerTooLong
        | Scenario::SummarizerPinnedDrop
        | Scenario::SummarizerLength
        | Scenario::SummarizerReasoningRetry
        | Scenario::SummarizerDown
        | Scenario::SummarizerThenEmpty
        | Scenario::SummarizerFacts => sse_response(vec![
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

/// The non-streaming counterpart of each scenario: one JSON document.
fn summarizer_failure_answer(scenario: Scenario) -> Response {
    let (content, finish_reason) = match scenario {
        Scenario::SummarizerEmpty => (String::new(), "stop"),
        Scenario::SummarizerTooLong => ("x".repeat(400), "stop"),
        Scenario::SummarizerPinnedDrop => ("A summary without the changed value".into(), "stop"),
        Scenario::SummarizerLength => (String::new(), "length"),
        _ => unreachable!("not a summarizer failure scenario"),
    };
    Json(json!({
        "id": "summarizer-failure",
        "model": "mock-summarizer",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content },
            "finish_reason": finish_reason
        }],
        "usage": { "prompt_tokens": 7, "completion_tokens": 4, "total_tokens": 11 }
    }))
    .into_response()
}

async fn completion_answer(scenario: Scenario) -> Response {
    let message = |message: Value, finish: &str| {
        json!({ "id": "cc1", "model": "mock-model",
                "choices": [{ "index": 0, "message": message, "finish_reason": finish }] })
    };
    let with_usage = |mut v: Value, prompt: u32, completion: u32| {
        v["usage"] = json!({ "prompt_tokens": prompt, "completion_tokens": completion,
                             "total_tokens": prompt + completion });
        v
    };
    let body = match scenario {
        Scenario::Happy | Scenario::CommentKeepalive => {
            with_usage(message(json!({ "role": "assistant", "content": "Hello, world" }), "stop"), 12, 3)
        }
        Scenario::Reasoning => with_usage(
            message(json!({ "content": "answer", "reasoning_content": "let me think" }), "stop"),
            12,
            5,
        ),
        Scenario::NoUsage => message(
            json!({ "content": "one two three four five six seven eight" }),
            "stop",
        ),
        Scenario::ToolCalls => with_usage(
            message(
                json!({ "content": null, "tool_calls": [{ "id": "call_1", "type": "function",
                        "function": { "name": "get_weather", "arguments": "{\"city\":\"NYC\"}" } }] }),
                "tool_calls",
            ),
            12,
            9,
        ),
        Scenario::MidStreamError => json!({ "error": { "message": "upstream exploded", "code": "internal" } }),
        Scenario::LeakyInBand => json!({ "error": { "message": leaky_detail(), "code": "internal" } }),
        Scenario::SlowFirstToken => {
            tokio::time::sleep(Duration::from_millis(300)).await;
            with_usage(message(json!({ "content": "eventual" }), "stop"), 12, 1)
        }
        Scenario::UpstreamReject | Scenario::LeakyReject | Scenario::Gated => unreachable!("handled by the caller"),
        Scenario::SummarizerEmpty
        | Scenario::SummarizerTooLong
        | Scenario::SummarizerPinnedDrop
        | Scenario::SummarizerLength
        | Scenario::SummarizerReasoningRetry
        | Scenario::SummarizerDown
        | Scenario::SummarizerThenEmpty
        | Scenario::SummarizerThenDown
        | Scenario::SummarizerHold
        | Scenario::SummarizerFacts => unreachable!("handled by the caller"),
    };
    Json(body).into_response()
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
    state: std::sync::Arc<llm_gateway::AppState>,
}

impl Gateway {
    /// The tenant roster. `throttled` and `broke` exist so rate limiting and
    /// budget exhaustion can be driven from a test without reconfiguring
    /// `acme`, whose generous limits keep the other cases readable.
    async fn start(upstream: &MockUpstream) -> Self {
        Self::start_with(upstream, UNCAPPED_MODEL).await
    }

    async fn start_with(upstream: &MockUpstream, extra_models: &str) -> Self {
        Self::start_with_folding(upstream, extra_models, None).await
    }

    async fn start_with_folding(
        upstream: &MockUpstream,
        extra_models: &str,
        folding: Option<&str>,
    ) -> Self {
        Self::start_folding(upstream, extra_models, folding, None, "", false).await
    }

    async fn start_folding(
        upstream: &MockUpstream,
        extra_models: &str,
        folding: Option<&str>,
        acme_budget: Option<u64>,
        acme_yaml: &str,
        background: bool,
    ) -> Self {
        captured_logs::install();
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
      daily_budget_nano_usd: 100000000000
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
      daily_budget_nano_usd: 100000000000
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
      daily_budget_nano_usd: 100000000000
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
      daily_budget_nano_usd: 50000
  - tenant_id: tight
    enabled: true
    credentials:
      - key_id: ak_tight
        key: test-key-tight
        scopes: [chat:stream]
    allowed_models: ["*"]
    limits:
      requests_per_minute: 1000
      tokens_per_minute: 10000000
      max_concurrent_streams: 64
      max_output_tokens: 4096
      # Room for three 128-token reservations on mock/chat (~1.03M nano-USD
      # each), never four.
      daily_budget_nano_usd: 3500000
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
      daily_budget_nano_usd: 1000
"#;
        let mut tenants = match folding {
            Some(config) => tenants.replacen(
                "    allowed_models: [\"*\"]\n    limits:",
                &format!(
                    "    allowed_models: [\"*\"]\n{acme_yaml}    folding:\n{config}\n    limits:"
                ),
                1,
            ),
            None => tenants.to_string(),
        };
        if let Some(budget) = acme_budget {
            tenants = tenants.replacen(
                "daily_budget_nano_usd: 100000000000",
                &format!("daily_budget_nano_usd: {budget}"),
                1,
            );
        }
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
            // Explicitly the in-memory ledger: the default is a SQLite file,
            // and parallel tests must not share one.
            ledger: llm_gateway::config::LedgerConfig {
                backend: llm_gateway::config::LedgerBackend::Memory,
                ..Default::default()
            },
            folding: llm_gateway::config::FoldingConfig {
                background,
                ..Default::default()
            },
            ..Default::default()
        };

        let state = llm_gateway::bootstrap::build(settings)
            .await
            .expect("build gateway state");
        let app = llm_gateway::api::router(std::sync::Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind gateway");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        // Give the listener a moment to start accepting.
        tokio::time::sleep(Duration::from_millis(30)).await;
        Self { addr, _dir: dir, state }
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

    async fn post_complete(&self, key: &str, body: Value) -> (StatusCode, HeaderMap, String) {
        self.post_json("/v1/chat/complete", key, body, &[]).await
    }

    async fn post_json(
        &self,
        path: &str,
        key: &str,
        body: Value,
        extra: &[(&str, &str)],
    ) -> (StatusCode, HeaderMap, String) {
        let client = reqwest::Client::new();
        let mut request = client.post(self.url(path)).header("x-api-key", key);
        for (name, value) in extra {
            request = request.header(*name, *value);
        }
        let resp = request.json(&body).send().await.expect("post");
        let status = resp.status();
        let headers = resp.headers().clone();
        (status, headers, resp.text().await.unwrap_or_default())
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

const FOLDING_SUMMARIZER_MODEL: &str = r#"  mock/summarizer:
    provider: nvidia
    endpoint: http://REPLACE_ME/v1/chat/completions
    upstream_model: mock-summarizer
    default_params:
      max_tokens: 64
    cost:
      input_per_mtok_usd: 0.01
      output_per_mtok_usd: 0.02
"#;

const FOLDING_TENANT: &str = r#"      enabled: true
      summarizer_model: mock/summarizer
      keep_last: 2
      summary_chars: 200
      summarizer_max_tokens: 64
      min_new_turns: 2
      min_new_tokens: 0
      amortization_requests: 1
"#;

fn folding_body(turns: usize) -> Value {
    let messages: Vec<Value> = (0..turns)
        .map(|index| {
            json!({
                "role": if index % 2 == 0 { "user" } else { "assistant" },
                "content": format!("turn {index} {}", "x".repeat(400)),
            })
        })
        .collect();
    json!({
        "model": "mock/chat",
        "messages": messages,
        "stream": true,
        "params": { "max_tokens": 64 },
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
    // 12 * 2,000 + 3 * 8,000 nano-USD.
    assert_eq!(end["cost_nano_usd"], 48_000);
}

#[tokio::test]
async fn foreground_folding_summarizes_before_the_main_upstream_call() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(
        &upstream,
        FOLDING_SUMMARIZER_MODEL,
        Some(FOLDING_TENANT),
    )
    .await;

    let (status, headers, body) = gw.post_chat(TEST_KEY, folding_body(30)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "28");
    // The summary and fold receipt headers are for receipts tenants only:
    // without receipts nobody checks them, and the summary could be 8,000
    // characters in a header.
    assert!(headers.get("x-gateway-fold-summary").is_none());
    assert!(headers.get("x-gateway-fold-receipt-id").is_none());
    assert_eq!(upstream.calls(), 2, "one summarizer call and one main call");

    let sent = upstream.last_body().expect("main request body");
    let messages = sent["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 3, "summary plus the verbatim tail");
    assert_eq!(messages[0]["role"], "user");
    assert!(messages[0]["content"].as_str().unwrap().contains("Hello, world"));
    assert!(messages[1]["content"].as_str().unwrap().starts_with("turn 28 "));
    assert!(messages[2]["content"].as_str().unwrap().starts_with("turn 29 "));

    let reserved: u64 = headers["x-reserved-nano-usd"].to_str().unwrap().parse().unwrap();
    let forwarded = serde_json::from_value::<Vec<llm_gateway::providers::ChatMessage>>(sent["messages"].clone()).unwrap();
    let prompt = llm_gateway::router::estimate_prompt_tokens(&forwarded);
    // mock/chat is $2 / $8 per million tokens; the request caps output at 64.
    let expected = prompt as u64 * 2_000 + 64 * 8_000;
    assert_eq!(reserved, expected, "the reservation is the folded estimate");
    let original = serde_json::from_value::<Vec<llm_gateway::providers::ChatMessage>>(
        folding_body(30)["messages"].clone(),
    )
    .unwrap();
    let unfolded = llm_gateway::router::estimate_prompt_tokens(&original) as u64 * 2_000 + 64 * 8_000;
    assert!(reserved < unfolded, "folding must reserve less than the client body");
}

#[tokio::test]
async fn a_committed_summary_is_reused_until_a_new_batch_is_due() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(
        &upstream,
        FOLDING_SUMMARIZER_MODEL,
        Some(FOLDING_TENANT),
    )
    .await;

    let (first_status, _, _) = gw.post_chat(TEST_KEY, folding_body(6)).await;
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(upstream.calls(), 2);

    let (second_status, second_headers, _) = gw.post_chat(TEST_KEY, folding_body(7)).await;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(second_headers.get("x-gateway-folded").unwrap(), "4");
    assert_eq!(upstream.calls(), 3, "the second request only calls the main model");
}

#[tokio::test]
async fn a_later_fold_forwards_the_facts_its_summarizer_did_not_mention() {
    let upstream = MockUpstream::start(Scenario::SummarizerFacts).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let body = |turns: usize| {
        let mut body = folding_body(turns);
        body["messages"][0]["content"] = json!("The appointment moved to 15:30");
        body["messages"][1]["content"] = json!("standing   rule: always answer in one sentence.");
        body
    };
    let summary_sent = |upstream: &MockUpstream| {
        let main = upstream.last_body().unwrap();
        main["messages"].as_array().unwrap().iter().find_map(|message| {
            message["content"].as_str().filter(|text| text.contains("Summary of the earlier conversation")).map(str::to_string)
        })
    };
    let (status, headers, _) = gw.post_chat(TEST_KEY, body(6)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "4");
    let first = summary_sent(&upstream).expect("folded");
    // The instruction in the conversation's own words, not the summarizer's.
    let kept = "Standing instructions from the user, still in force:\n- standing rule: always answer in one sentence.\n\n";
    assert!(first.ends_with(&format!("{kept}Current facts:\n- appointment: 15:30\n\nContext:\nfirst note")), "{first}");

    // Two more turns: a second fold, whose summarizer reports no facts.
    let (status, headers, _) = gw.post_chat(TEST_KEY, body(8)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "6");
    assert_eq!(summarizer_bodies(&upstream).len(), 2);
    // It was shown the facts the gateway keeps.
    assert!(summarizer_bodies(&upstream)[1].to_string().contains("appointment: 15:30"));
    let second = summary_sent(&upstream).expect("folded");
    assert!(second.ends_with(&format!("{kept}Current facts:\n- appointment: 15:30\n\nContext:\nsecond note")), "{second}");
}

#[tokio::test]
async fn a_tenant_without_folding_keeps_the_upstream_body_byte_for_byte() {
    let baseline_upstream = MockUpstream::start(Scenario::Happy).await;
    let baseline = Gateway::start(&baseline_upstream).await;
    let body = folding_body(6);
    baseline.post_chat(TEST_KEY, body.clone()).await;
    let expected = baseline_upstream.last_body().unwrap();

    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let (status, headers, _) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none());
    assert_eq!(upstream.last_body().unwrap(), expected);
    assert_eq!(upstream.calls(), 1);
}

#[tokio::test]
async fn folding_applies_to_the_non_streaming_endpoint_too() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(
        &upstream,
        FOLDING_SUMMARIZER_MODEL,
        Some(FOLDING_TENANT),
    )
    .await;
    let mut body = folding_body(6);
    body["stream"] = json!(false);
    let (status, headers, _) = gw.post_complete(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "4");
    assert_eq!(upstream.calls(), 2);
}

async fn assert_summarizer_failure(scenario: Scenario, body: Value) {
    let upstream = MockUpstream::start(scenario).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let (status, headers, _) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none());
    // A length reply is retried at double the budget; a summary that fails
    // its checks gets one corrective retry (the mock's second reply, "Hello,
    // world", fails them again).
    let expected_calls = if matches!(scenario, Scenario::SummarizerLength | Scenario::SummarizerPinnedDrop) { 3 } else { 2 };
    assert_eq!(upstream.calls(), expected_calls, "the failed summarizer must not fail the main request");
}

#[tokio::test]
async fn empty_summarizer_reply_is_rejected_without_failing_the_main_request() {
    assert_summarizer_failure(Scenario::SummarizerEmpty, folding_body(6)).await;
}

#[tokio::test]
async fn a_reply_that_is_not_json_is_fixed_by_its_corrective_retry() {
    let upstream = MockUpstream::start(Scenario::SummarizerTooLong).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let (status, headers, _) = gw.post_chat(TEST_KEY, folding_body(6)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_some(), "the retried summary is committed");
    let summarizer = summarizer_bodies(&upstream);
    assert_eq!(summarizer.len(), 2);
    // The retry shows the summarizer its rejected summary and what was wrong.
    let retry = summarizer[1]["messages"].as_array().unwrap();
    assert_eq!(retry.len(), 4);
    assert_eq!(summarizer[1]["max_tokens"], summarizer[0]["max_tokens"], "a corrective retry gets the usual budget");
    assert_eq!(retry[2]["role"], "assistant");
    assert_eq!(retry[2]["content"], "x".repeat(400));
    assert!(retry[3]["content"].as_str().unwrap().contains("It is not the JSON object asked for."), "{}", retry[3]);
    let (decision, fold) = decision_record(prompt_tokens_of(&upstream.last_body().unwrap()));
    assert_eq!(fold["outcome"], "applied", "{decision}");
    assert_eq!(fold["corrected_checks"]["format"], 1, "{fold}");
    assert!(fold.get("failed_checks").is_none(), "{fold}");
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert!(metrics.contains("gw_fold_corrective_retries_total{tenant=\"acme\",result=\"committed\"} 1"), "{metrics}");
    assert!(metrics.contains("gw_fold_corrective_retries_total{tenant=\"acme\",result=\"rejected\"} 0"), "{metrics}");
    assert_eq!(fold_counter(&metrics, "committed"), 1, "{metrics}");
}

#[tokio::test]
async fn a_summary_that_drops_a_pinned_value_is_rejected() {
    let mut body = folding_body(6);
    body["messages"][0]["content"] = json!("The appointment moved to 15:30");
    assert_summarizer_failure(Scenario::SummarizerPinnedDrop, body).await;
}

#[tokio::test]
async fn a_rejected_summary_names_the_checks_it_failed_and_nothing_else() {
    let upstream = MockUpstream::start(Scenario::SummarizerPinnedDrop).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let mut body = folding_body(6);
    body["messages"][0]["content"] = json!("The appointment moved to 15:30");
    let (status, _, _) = gw.post_chat(TEST_KEY, body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let (decision, fold) = decision_record(prompt_tokens_of(&upstream.last_body().unwrap()));
    assert_eq!(fold["reason"], "checks_failed");
    // Both the summary and its corrective retry dropped 15:30.
    assert_eq!(fold["failed_checks"]["pinned_facts_present"], 1, "{fold}");
    assert_eq!(fold["corrected_checks"]["pinned_facts_present"], 1, "{fold}");
    assert!(!decision.contains("15:30"), "a missing value reached the decision line: {decision}");
    // The summarizer itself is told what is missing.
    let summarizer = summarizer_bodies(&upstream);
    assert_eq!(summarizer.len(), 2);
    assert!(summarizer[1]["messages"][3]["content"].as_str().unwrap().contains("It leaves out '15:30'"), "{}", summarizer[1]);
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    let series = |check: &str| format!("gw_fold_checks_failed_total{{tenant=\"acme\",check=\"{check}\"}} ");
    assert!(metrics.contains(&format!("{}2", series("pinned_facts_present"))), "{metrics}");
    assert!(metrics.contains(&format!("{}0", series("non_empty"))), "{metrics}");
    assert!(metrics.contains("gw_fold_corrective_retries_total{tenant=\"acme\",result=\"rejected\"} 1"), "{metrics}");
    assert_eq!(fold_counter(&metrics, "rejected"), 1, "{metrics}");
}

#[tokio::test]
async fn pinned_values_that_cannot_fit_skip_the_fold_before_it_is_paid_for() {
    // Every turn carries an id, so the values a summary would have to keep run
    // far past half of summary_chars (200). No summary could pass the check,
    // so the summarizer is never called and the request goes out unfolded.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let mut body = folding_body(30);
    for (index, message) in body["messages"].as_array_mut().unwrap().iter_mut().enumerate() {
        message["content"] = json!(format!("release build-{index}-candidate, ticket OPS-{index}"));
    }
    let before = spent(&gw, TEST_KEY).await;
    let (status, headers, _) = gw.post_chat(TEST_KEY, body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none());
    assert!(summarizer_bodies(&upstream).is_empty(), "no summarizer call for a fold that cannot pass");
    let sent = upstream.last_body().unwrap();
    assert_eq!(sent["messages"], body["messages"], "the request goes out unfolded");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let charged = spent(&gw, TEST_KEY).await - before;
    let reserved = reserved_of(&headers);
    assert!(charged <= reserved, "only the main request was charged: {charged} > {reserved}");
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert_eq!(fold_counter(&metrics, "pinned_overflow"), 1, "{metrics}");
    assert!(!metrics.contains("kind=\"prompt\"} 1"), "{metrics}");
}

#[tokio::test]
async fn field_pins_count_towards_the_overflow_check() {
    // "the new lead7 is Q" pins `lead7: Q` and no bare value ("lead" is not
    // one of the words the value patterns know), so only the field pins run
    // past half of summary_chars.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let mut body = folding_body(30);
    for (index, message) in body["messages"].as_array_mut().unwrap().iter_mut().enumerate() {
        message["content"] = json!(format!("the new lead{index} is Q"));
    }
    let (status, headers, _) = gw.post_chat(TEST_KEY, body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none());
    assert!(summarizer_bodies(&upstream).is_empty(), "no summarizer call for a fold that cannot pass");
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert_eq!(fold_counter(&metrics, "pinned_overflow"), 1, "{metrics}");
}

#[tokio::test]
async fn a_rejected_fold_backs_off_before_paying_again() {
    // The first fold is rejected (its summary drops "15:30"). Until
    // backoff_turns (8) more turns arrive, the conversation is not folded
    // again; after that it is.
    let upstream = MockUpstream::start(Scenario::SummarizerPinnedDrop).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let body = |turns: usize| {
        let mut body = folding_body(turns);
        body["messages"][0]["content"] = json!("The appointment moved to 15:30");
        body
    };
    // The summary and its corrective retry both drop 15:30.
    assert_eq!(gw.post_chat(TEST_KEY, body(6)).await.0, StatusCode::OK);
    assert_eq!(summarizer_bodies(&upstream).len(), 2);
    let (status, headers, _) = gw.post_chat(TEST_KEY, body(8)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none());
    assert_eq!(summarizer_bodies(&upstream).len(), 2, "backing off: no second paid fold");
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert_eq!(fold_counter(&metrics, "rejected"), 1, "{metrics}");
    assert_eq!(fold_counter(&metrics, "backoff"), 1, "{metrics}");
    // 8 more turns than the head the rejected fold covered: fold again.
    assert_eq!(gw.post_chat(TEST_KEY, body(14)).await.0, StatusCode::OK);
    assert_eq!(summarizer_bodies(&upstream).len(), 4, "the backoff ends after backoff_turns");
    // That fold was rejected too: the next wait is twice as long (16 turns
    // past the 12-turn head it covered, so not at 20, but at 28).
    assert_eq!(gw.post_chat(TEST_KEY, body(22)).await.0, StatusCode::OK);
    assert_eq!(summarizer_bodies(&upstream).len(), 4, "a second rejection backs off twice as long");
    assert_eq!(gw.post_chat(TEST_KEY, body(30)).await.0, StatusCode::OK);
    assert_eq!(summarizer_bodies(&upstream).len(), 6, "and then folds again");
}

#[tokio::test]
async fn a_length_reply_twice_is_rejected_without_failing_the_main_request() {
    assert_summarizer_failure(Scenario::SummarizerLength, folding_body(6)).await;
}

#[tokio::test]
async fn a_reasoning_only_reply_is_retried_once_at_double_budget() {
    let upstream = MockUpstream::start(Scenario::SummarizerReasoningRetry).await;
    let gw = Gateway::start_with_folding(
        &upstream,
        FOLDING_SUMMARIZER_MODEL,
        Some(FOLDING_TENANT),
    )
    .await;
    let (status, headers, _) = gw.post_chat(TEST_KEY, folding_body(6)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "4");
    assert_eq!(upstream.calls(), 3, "retry, then main request");
    let bodies = upstream.bodies();
    assert_eq!(bodies[0]["max_tokens"], 64, "the first attempt uses the base budget");
    assert_eq!(bodies[1]["max_tokens"], 128, "the retry doubles the budget");
}

fn openai_folding_body(turns: usize, stream: bool) -> Value {
    let mut body = folding_body(turns);
    let max_tokens = body["params"]["max_tokens"].clone();
    body.as_object_mut().unwrap().remove("params");
    body["max_tokens"] = max_tokens;
    body["stream"] = json!(stream);
    body
}

fn billed_nano(messages: &Value, input_nano_per_token: u64, completion_tokens: u64, output_nano_per_token: u64) -> u64 {
    let parsed = serde_json::from_value::<Vec<llm_gateway::providers::ChatMessage>>(messages.clone()).unwrap();
    let prompt = u64::from(llm_gateway::router::estimate_prompt_tokens(&parsed));
    prompt * input_nano_per_token + completion_tokens * output_nano_per_token
}

#[tokio::test]
async fn the_summarizer_is_its_own_settled_ledger_request() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let (status, _, _) = gw.post_chat(TEST_KEY, folding_body(6)).await;
    assert_eq!(status, StatusCode::OK);
    let bodies = upstream.bodies();
    assert_eq!(bodies.len(), 2);
    // Summarizer catalogue rate is $0.01 / $0.02 per million tokens (10 and 20
    // nano-USD per token). The main model is $2 / $8. Reported completion is 3.
    let summarizer = billed_nano(&bodies[0]["messages"], 10, 3, 20);
    let main = billed_nano(&bodies[1]["messages"], 2_000, 3, 8_000);
    let (_, usage) = gw.get("/v1/usage", TEST_KEY).await;
    let usage: Value = serde_json::from_str(&usage).unwrap();
    assert_eq!(usage["requests_last_minute"], 2, "the summarizer is a second request: {usage}");
    assert_eq!(
        usage["spent_nano_usd"],
        summarizer + main,
        "each call is settled from its own usage: {usage}"
    );
}

#[tokio::test]
async fn a_summarizer_the_budget_refuses_leaves_the_main_request_unfolded() {
    const EXPENSIVE: &str = r#"  mock/summarizer:
    provider: nvidia
    endpoint: http://REPLACE_ME/v1/chat/completions
    upstream_model: mock-summarizer
    cost:
      input_per_mtok_usd: 0.01
      output_per_mtok_usd: 50.0
"#;
    const AMORTIZED: &str = r#"      enabled: true
      summarizer_model: mock/summarizer
      keep_last: 2
      summary_chars: 200
      summarizer_max_tokens: 64
      min_new_turns: 2
      min_new_tokens: 0
      amortization_requests: 40
"#;
    let upstream = MockUpstream::start(Scenario::Happy).await;
    // Amortizing the cut over 40 requests makes the fold profitable, so the
    // cost check lets it through. 64 output tokens at $50 / million is 3.2M
    // nano-USD, which this budget refuses. The unfolded main call still fits.
    let gw = Gateway::start_folding(&upstream, EXPENSIVE, Some(AMORTIZED), Some(2_500_000), "", false).await;
    let body = folding_body(6);
    let (status, headers, _) = gw.post_chat(TEST_KEY, body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none());
    assert_eq!(upstream.calls(), 1, "admission failure must not call the summarizer");
    let sent = serde_json::to_string(&upstream.last_body().unwrap()["messages"]).unwrap();
    let original = serde_json::to_string(&body["messages"]).unwrap();
    assert_eq!(sent, original);
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert!(
        metrics.contains("gw_folds_total{tenant=\"acme\",outcome=\"refused_admission\"} 1"),
        "the refusal has to be the budget, not the cost check: {metrics}"
    );
}

#[tokio::test]
async fn a_held_request_never_calls_the_summarizer() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_folding(
        &upstream,
        FOLDING_SUMMARIZER_MODEL,
        Some(FOLDING_TENANT),
        None,
        "    holds:\n      above_nano_usd: 1\n",
        false,
    )
    .await;
    let (status, _, body) = gw.post_chat(TEST_KEY, folding_body(6)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(upstream.calls(), 0);
}

#[tokio::test]
async fn an_idempotent_replay_returns_the_folded_header_without_a_new_summarizer_call() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let mut body = folding_body(6);
    body["stream"] = json!(false);
    let first = gw
        .post_json("/v1/chat/complete", TEST_KEY, body.clone(), &[("idempotency-key", "fold-once")])
        .await;
    assert_eq!(first.0, StatusCode::OK, "{}", first.2);
    assert_eq!(first.1.get("x-gateway-folded").unwrap(), "4");
    assert_eq!(upstream.calls(), 2);

    let replay = gw
        .post_json("/v1/chat/complete", TEST_KEY, body, &[("idempotency-key", "fold-once")])
        .await;
    assert_eq!(replay.0, StatusCode::OK, "{}", replay.2);
    assert_eq!(replay.1.get("x-gateway-folded").unwrap(), "4");
    assert_eq!(replay.1.get("idempotent-replayed").unwrap(), "true");
    assert_eq!(upstream.calls(), 2, "a replay must not call the summarizer again");
}

#[tokio::test]
async fn folding_applies_on_the_openai_completions_endpoint() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;

    let (status, headers, body) = gw
        .post_json("/v1/chat/completions", TEST_KEY, openai_folding_body(6, true), &[])
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "4");

    let (status, headers, body) = gw
        .post_json("/v1/chat/completions", TEST_KEY, openai_folding_body(6, false), &[])
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "4");
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
    // The vendor's words stay in the logs; the client gets the class.
    assert_eq!(err["error"]["message"], "the upstream provider failed to answer (HTTP 502)", "{err}");
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

    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert!(metrics.contains("gw_streams_started_total 0"), "{metrics}");
    assert!(metrics.contains("gw_upstream_errors_total 1"), "{metrics}");
    assert!(
        metrics.contains("gw_model_errors_total{provider=\"nvidia\",model=\"mock/chat\"} 1"),
        "{metrics}"
    );
    // The slot is released; the cost side is `a_refused_upstream_costs_the_tenant_nothing`.
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
async fn max_tokens_above_u32_limit_is_rejected_before_upstream_call() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let mut body = chat_body();
    body["params"] = json!({ "max_tokens": u64::from(u32::MAX) + 1 });
    let (status, _, text) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(text.contains("max_tokens"), "{text}");
    assert_eq!(upstream.calls(), 0, "a rejected request must not reach the provider");
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
    // `broke` has a 50,000 nano-USD daily budget. A request reserving prompt plus
    // 128 output tokens at 2/8 nano-USD per token costs 10 + 1024 nano-USD,
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
    assert_eq!(usage["spent_nano_usd"], 48_000);
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

    let (status, text) = gw.get("/metrics", TEST_KEY).await;
    assert_eq!(status, StatusCode::OK);
    assert!(text.contains("gw_streams_completed_total 1"), "{text}");
    assert!(text.contains("gw_prompt_tokens_total 12"), "{text}");
    assert!(text.contains("gw_completion_tokens_total 3"), "{text}");
    assert!(text.contains("gw_cost_nano_usd_total 48000"), "{text}");
    assert!(text.contains("gw_tenant_streams_total{tenant=\"acme\"} 1"), "{text}");
    assert!(text.contains("# TYPE gw_streams_completed_total counter"));
    assert!(text.contains("# TYPE gw_ledger_swept_reservations_total counter"), "{text}");
    // Every line is a comment or a sample, starting in the first column.
    for line in text.lines() {
        assert!(!line.starts_with(char::is_whitespace), "indented exposition line: {line:?}");
    }
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
async fn readiness_keeps_a_valid_fallback_when_another_model_is_unavailable() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let extra_models = r#"  unavailable/cloud:
    provider: nvidia
    upstream_model: cloud-model
    cost:
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
  unknown/model:
    provider: unknown
    upstream_model: unknown-model
    cost:
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
"#;
    let gw = Gateway::start_with(&upstream, extra_models).await;

    let (status, text) = gw.get("/health/ready", "").await;
    assert_eq!(status, StatusCode::OK, "the mock route remains a fallback: {text}");
    let ready: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(ready["routable_models"], 1, "mock/chat remains the fallback");
    let unavailable = ready["unavailable_models"].as_array().unwrap();
    assert_eq!(unavailable.len(), 2, "the cloud and unknown routes are reported");
    assert!(unavailable.iter().any(|issue| issue.as_str().unwrap().contains("NVIDIA_API_KEY")));
    assert!(unavailable.iter().any(|issue| issue.as_str().unwrap().contains("unknown to this gateway")));
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
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
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
    // Prompt is a few tokens at 2,000 nano-USD each; completion is refunded.
    let spent = usage["spent_nano_usd"].as_u64().unwrap();
    assert!(spent > 0, "an abort must still be billed for its prompt: {text}");
    assert!(spent < 2_048_000, "the completion reservation must be refunded: {text}");
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

async fn spent(gw: &Gateway, key: &str) -> u64 {
    let (status, text) = gw.get("/v1/usage", key).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    serde_json::from_str::<Value>(&text).unwrap()["spent_nano_usd"]
        .as_u64()
        .unwrap()
}

#[tokio::test]
async fn a_refused_upstream_costs_the_tenant_nothing() {
    // The upstream answered 429 before streaming: it processed no prompt and
    // bills nothing, so neither does the gateway.
    let upstream = MockUpstream::start(Scenario::UpstreamReject).await;
    let gw = Gateway::start(&upstream).await;
    let (status, _, _) = gw.post_chat(TEST_KEY, chat_body()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(spent(&gw, TEST_KEY).await, 0);
}

#[tokio::test]
async fn an_unreachable_upstream_costs_the_tenant_nothing() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let extra = r#"  mock/down:
    provider: nvidia
    endpoint: http://127.0.0.1:9/v1/chat/completions
    upstream_model: mock-model
    default_params:
      max_tokens: 64
    cost:
      input_per_mtok_usd: 1.0
      output_per_mtok_usd: 1.0
"#;
    let gw = Gateway::start_with(&upstream, extra).await;
    let mut body = chat_body();
    body["model"] = json!("mock/down");
    let (status, _, _) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(spent(&gw, TEST_KEY).await, 0);
    let (_, usage) = gw.get("/v1/usage", TEST_KEY).await;
    assert!(usage.contains("\"streams_in_flight\":0"), "{usage}");
}

#[tokio::test]
async fn a_models_template_overhead_is_reserved_but_never_billed() {
    // The same request to two models priced alike, one of which declares 100
    // tokens of vendor template: it reserves 100 prompt tokens more, and the
    // bill, set by the reported usage, is the same.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let extra = r#"  mock/templated:
    provider: nvidia
    endpoint: http://REPLACE_ME/v1/chat/completions
    upstream_model: mock-model
    prompt_overhead_tokens: 100
    cost:
      input_per_mtok_usd: 2.0
      output_per_mtok_usd: 8.0
"#;
    let gw = Gateway::start_with(&upstream, extra).await;
    let mut measured = Vec::new();
    for model in ["mock/chat", "mock/templated"] {
        let mut body = chat_body();
        body["model"] = json!(model);
        let before = spent(&gw, TEST_KEY).await;
        let (status, headers, text) = gw.post_chat(TEST_KEY, body).await;
        assert_eq!(status, StatusCode::OK, "{text}");
        let reserved: u64 = headers["x-reserved-nano-usd"].to_str().unwrap().parse().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        measured.push((reserved, spent(&gw, TEST_KEY).await - before));
    }
    let [(plain_reserved, plain_billed), (templated_reserved, templated_billed)] = measured[..] else {
        unreachable!()
    };
    assert_eq!(templated_reserved - plain_reserved, 100 * 2_000, "100 tokens more reserved, at the input rate");
    assert_eq!(templated_billed, plain_billed, "and not a token more billed");
    assert!(templated_billed > 0);
}

#[tokio::test]
async fn passthrough_is_billed_the_full_reservation() {
    // The gateway cannot see passthrough usage, so the reservation is the
    // bill. Refunding the completion half would make passthrough a way
    // around the budget: stream a whole completion, pay for the prompt.
    let upstream = MockUpstream::start(Scenario::CommentKeepalive).await;
    let gw = Gateway::start(&upstream).await;

    let mut body = chat_body();
    body["framing"] = json!("passthrough");
    let (status, headers, _) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::OK);
    let reserved: u64 = headers["x-reserved-nano-usd"].to_str().unwrap().parse().unwrap();
    assert!(reserved >= 128 * 8_000, "reserved {reserved}");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(spent(&gw, TEST_KEY).await, reserved);
}

#[tokio::test]
async fn a_stream_without_reported_usage_bills_what_it_delivered() {
    // No usage from the upstream used to settle at zero completion tokens:
    // a whole answer for the price of its prompt. The delivered deltas are
    // the floor, and the `end` frame reports exactly what the ledger holds.
    let upstream = MockUpstream::start(Scenario::NoUsage).await;
    let gw = Gateway::start(&upstream).await;

    let (status, headers, body) = gw.post_chat(TEST_KEY, chat_body()).await;
    assert_eq!(status, StatusCode::OK);
    let end = parse_sse(&body)
        .into_iter()
        .find(|(e, _)| e.as_deref() == Some("end"))
        .map(|(_, d)| serde_json::from_str::<Value>(&d).unwrap())
        .expect("end frame");
    let completion = end["usage"]["completion_tokens"].as_u64().unwrap();
    assert!(completion >= 2, "two content deltas were delivered: {end}");
    let reserved: u64 = headers["x-reserved-nano-usd"].to_str().unwrap().parse().unwrap();
    let billed = spent(&gw, TEST_KEY).await;
    assert_eq!(billed, end["cost_nano_usd"].as_u64().unwrap(), "end frame and ledger agree");
    assert!(billed < reserved, "the unused ceiling is refunded");
}

#[tokio::test]
async fn concurrent_streams_near_the_budget_admit_exactly_what_fits() {
    // Ten simultaneous streams against a budget with room for three. The mock
    // holds every admitted stream at its gate, so all ten reservations are
    // live at once and none can settle early and free budget.
    let upstream = MockUpstream::start(Scenario::Gated).await;
    let gw = Arc::new(Gateway::start(&upstream).await);

    let rejected = Arc::new(AtomicU64::new(0));
    let requests: Vec<_> = (0..10)
        .map(|_| {
            let gw = Arc::clone(&gw);
            let rejected = Arc::clone(&rejected);
            tokio::spawn(async move {
                let (status, _, _) = gw.post_chat("test-key-tight", chat_body()).await;
                if status == StatusCode::PAYMENT_REQUIRED {
                    rejected.fetch_add(1, Ordering::SeqCst);
                }
                status
            })
        })
        .collect();

    // Every request is decided once it was either refused or reached the
    // upstream. Nothing is released until then.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while upstream.calls() + rejected.load(Ordering::SeqCst) < 10 {
        assert!(tokio::time::Instant::now() < deadline, "requests never settled");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(upstream.calls(), 3, "exactly three reservations fit");

    // Another tenant is admitted while `tight` is pinned at its ceiling: it
    // reaches the upstream with the gate still shut.
    let other = {
        let gw = Arc::clone(&gw);
        tokio::spawn(async move { gw.post_chat(TEST_KEY, chat_body()).await.0 })
    };
    while upstream.calls() < 4 {
        assert!(tokio::time::Instant::now() < deadline, "tenant budgets are not isolated");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    upstream.open_gate(10);
    assert_eq!(other.await.unwrap(), StatusCode::OK);
    let mut statuses = Vec::new();
    for r in requests {
        statuses.push(r.await.unwrap());
    }
    assert_eq!(statuses.iter().filter(|s| **s == StatusCode::OK).count(), 3);
    assert_eq!(rejected.load(Ordering::SeqCst), 7);
}

#[tokio::test]
async fn credentials_and_prompts_never_reach_the_logs() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;
    let marker = format!("prompt-marker-{}", uuid::Uuid::new_v4());

    // A streamed request, a refused one, and a malformed one, all carrying
    // the key and the marker.
    let mut body = chat_body();
    body["messages"] = json!([{ "role": "user", "content": marker }]);
    let (status, _, _) = gw.post_chat(TEST_KEY, body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let mut denied = body.clone();
    denied["model"] = json!("mock/chat");
    gw.post_chat("test-key-locked", denied).await;
    let mut malformed = body;
    malformed["messages"] = json!([{ "role": "user", "content": marker, "bogus": { "deep": marker } }]);
    malformed["params"] = json!({ "max_tokens": "not-a-number" });
    gw.post_chat(TEST_KEY, malformed).await;
    // Bearer form too.
    let client = reqwest::Client::new();
    client
        .get(gw.url("/v1/usage"))
        .header("authorization", format!("Bearer {TEST_KEY}"))
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let logs = captured_logs::contents();
    assert!(logs.contains("stream complete"), "log capture is not working");
    for secret in [TEST_KEY, "test-key-locked", marker.as_str(), LEAKED_KEY] {
        assert!(!logs.contains(secret), "`{secret}` reached a log line");
    }
}

#[tokio::test]
async fn vendor_error_bodies_reach_the_logs_but_never_the_tenant() {
    let leaks = |text: &str| text.contains(ACCOUNT_MARKER) || text.contains("leakedKey");

    // Refused before streaming: a real status, a generic sentence.
    let upstream = MockUpstream::start(Scenario::LeakyReject).await;
    let gw = Gateway::start(&upstream).await;
    let (status, _, text) = gw.post_chat(TEST_KEY, chat_body()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(!leaks(&text), "a vendor body reached the tenant: {text}");
    let err: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(err["error"]["code"], "upstream_rejected_request");
    assert_eq!(err["error"]["retryable"], false);
    assert_eq!(err["error"]["message"], "the upstream provider refused the request (HTTP 404)");
    let (status, _, text) = gw.post_complete(TEST_KEY, complete_body()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(!leaks(&text), "a vendor body reached the tenant: {text}");
    let refused = gw;

    // Failed in-band: the SSE `error` event and the completion's error body.
    let upstream = MockUpstream::start(Scenario::LeakyInBand).await;
    let gw = Gateway::start(&upstream).await;
    let (status, _, text) = gw.post_chat(TEST_KEY, chat_body()).await;
    assert_eq!(status, StatusCode::OK);
    let event = parse_sse(&text)
        .into_iter()
        .find(|(e, _)| e.as_deref() == Some("error"))
        .map(|(_, d)| d)
        .expect("error event");
    assert!(!leaks(&event), "a vendor body reached the SSE error event: {event}");
    let (status, _, text) = gw.post_complete(TEST_KEY, complete_body()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(!leaks(&text), "a vendor body reached the tenant: {text}");
    let err: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(err["error"]["code"], "upstream_error");
    assert_eq!(err["error"]["retryable"], true);

    // The decision record keeps the class, not the vendor's words.
    tokio::time::sleep(Duration::from_millis(100)).await;
    for gw in [&refused, &gw] {
        let (status, record) = gw.get("/v1/admin/decisions?tenant=acme&limit=100", TEST_KEY).await;
        assert_eq!(status, StatusCode::OK, "{record}");
        assert!(record.contains("the upstream provider"), "no provider failure was recorded: {record}");
        assert!(!leaks(&record), "a vendor body reached the decision record: {record}");
    }

    // The operator still sees what the vendor said, minus the key.
    let logs = captured_logs::contents();
    assert!(logs.contains(ACCOUNT_MARKER), "the vendor's detail must reach the logs");
    assert!(logs.contains("upstream failed mid-stream"), "the in-band failure was not logged");
    assert!(!logs.contains("leakedKey"), "a key-shaped string reached a log line");
}

#[tokio::test]
async fn an_oversized_header_block_is_refused_before_routing() {
    // hyper's defaults cap a request at 100 headers; the gateway relies on
    // that rather than re-implementing it. This pins the behavior so a server
    // upgrade that changes it is noticed.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut sock = tokio::net::TcpStream::connect(gw.addr).await.expect("connect");
    let mut head = format!("GET /health/live HTTP/1.1\r\nhost: {}\r\n", gw.addr);
    for i in 0..200 {
        head.push_str(&format!("x-filler-{i}: {i}\r\n"));
    }
    head.push_str("\r\n");
    sock.write_all(head.as_bytes()).await.expect("write head");
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut buf))
        .await
        .expect("answered")
        .unwrap_or(0);
    let status_line = String::from_utf8_lossy(&buf[..n]);
    assert!(
        status_line.starts_with("HTTP/1.1 431"),
        "expected 431, got {status_line:?}"
    );
}

#[tokio::test]
async fn metric_labels_come_from_config_never_from_the_request() {
    // A label value per request is unbounded cardinality: a scraper-killer
    // an unauthenticated caller could trigger. Unknown models, unknown keys
    // and junk tenants must not mint a series.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;
    let junk = format!("junk{}", uuid::Uuid::new_v4().simple());

    let mut body = chat_body();
    body["model"] = json!(format!("mock/{junk}"));
    gw.post_chat(TEST_KEY, body).await;
    gw.post_chat(&junk, chat_body()).await;
    let mut body = chat_body();
    body["metadata"] = json!({ "tenant": junk, "model": junk });
    gw.post_chat(TEST_KEY, body).await;

    let (status, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!metrics.contains(&junk), "a request minted a label: {metrics}");

    // Folding uses the same rule: a label is the tenant id from tenants.yaml.
    let folding_upstream = MockUpstream::start(Scenario::Happy).await;
    let folding = Gateway::start_with_folding(
        &folding_upstream,
        FOLDING_SUMMARIZER_MODEL,
        Some(FOLDING_TENANT),
    )
    .await;
    folding.post_chat(&junk, folding_body(6)).await;
    let mut tagged = folding_body(6);
    tagged["metadata"] = json!({ "tenant": junk });
    let (status, _, _) = folding.post_chat(TEST_KEY, tagged).await;
    assert_eq!(status, StatusCode::OK);
    let (_, metrics) = folding.get("/metrics", TEST_KEY).await;
    assert!(!metrics.contains(&junk), "a fold minted a label from the request: {metrics}");
    assert!(
        metrics.contains("gw_folds_total{tenant=\"acme\",outcome=\"committed\"} 1"),
        "the configured tenant is the only series: {metrics}"
    );
}

/// The decision line of a request, found by its `prompt_tokens` (the estimate
/// of the messages that were forwarded), for tests that do not have its id.
fn decision_record(prompt_tokens: u32) -> (String, Value) {
    let needle = format!("\"prompt_tokens\":{prompt_tokens},");
    let logs = captured_logs::contents();
    let line = logs
        .lines()
        .rev()
        .find(|line| line.contains("stream complete") && line.contains(&needle))
        .unwrap_or_else(|| panic!("no decision line for prompt_tokens {prompt_tokens}"))
        .to_string();
    let event: Value = serde_json::from_str(&line).expect(&line);
    let fold = event
        .pointer("/fields/fold")
        .or_else(|| event.get("fold"))
        .unwrap_or_else(|| panic!("decision line has no fold object: {line}"));
    let fold = serde_json::from_str(fold.as_str().unwrap_or_else(|| panic!("fold is not a string: {line}"))).unwrap();
    (line, fold)
}

fn prompt_tokens_of(body: &Value) -> u32 {
    let messages = serde_json::from_value::<Vec<llm_gateway::providers::ChatMessage>>(body["messages"].clone()).unwrap();
    llm_gateway::router::estimate_prompt_tokens(&messages)
}

fn assert_fold_shape(fold: &Value) {
    for key in [
        "outcome",
        "reason",
        "turns_replaced",
        "turns_covered",
        "prompt_tokens_cut",
        "benefit_nano_usd",
        "overhead_nano_usd",
        "min_saving_nano_usd",
        "summarizer_prompt_tokens",
        "summarizer_completion_tokens",
        "fold_count",
        "prefix_hash",
    ] {
        assert!(fold.get(key).is_some_and(|value| !value.is_null()), "fold is missing {key}: {fold}");
    }
}

fn fold_counter(metrics: &str, outcome: &str) -> u64 {
    let series = format!("gw_folds_total{{tenant=\"acme\",outcome=\"{outcome}\"}}");
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&series).and_then(|rest| rest.trim().parse().ok()))
        .unwrap_or(0)
}

const FOLD_MARKER: &str = "FOLDMARKER9f3c2a";

fn marked_folding_body(turns: usize, tail_pad: usize) -> Value {
    let mut body = folding_body(turns);
    body["messages"][0]["content"] = json!(format!("{FOLD_MARKER} turn 0 {}", "x".repeat(400)));
    let last = body["messages"].as_array().unwrap().len() - 1;
    let content = body["messages"][last]["content"].as_str().unwrap().to_string();
    body["messages"][last]["content"] = json!(format!("{content}{}", "y".repeat(tail_pad)));
    body
}

/// The "stream complete" line whose `field` is `value`.
fn completion_line(field: &str, value: &str) -> Option<Value> {
    captured_logs::contents().lines().rev().find_map(|line| {
        let event: Value = serde_json::from_str(line).ok()?;
        let fields = event.get("fields").unwrap_or(&event);
        (fields["message"] == "stream complete" && fields[field] == value).then(|| fields.clone())
    })
}

#[tokio::test]
async fn the_request_id_a_response_carries_is_the_one_in_the_log() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;
    let (status, headers, _) = gw.post_chat(TEST_KEY, folding_body(3)).await;
    assert_eq!(status, StatusCode::OK);
    let id = headers.get("x-request-id").unwrap().to_str().unwrap().to_string();
    assert!(uuid::Uuid::parse_str(&id).is_ok(), "{id}");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let line = completion_line("request_id", &id).unwrap_or_else(|| panic!("no log line for {id}"));
    assert_eq!(line["client_request_id"], "-");
}

#[tokio::test]
async fn a_client_request_id_is_echoed_and_logged_beside_the_gateways() {
    // A folding tenant, so the line with fold facts carries it too.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let resp = reqwest::Client::new()
        .post(gw.url("/v1/chat/stream"))
        .header("x-api-key", TEST_KEY)
        .header("x-request-id", "client-trace-7f3a")
        .json(&folding_body(3))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("x-request-id").unwrap(), "client-trace-7f3a");
    let _ = resp.text().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let line = completion_line("client_request_id", "client-trace-7f3a").expect("logged with the client's id");
    assert!(line.get("fold").is_some(), "{line}");
    let gateway_id = line["request_id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(gateway_id).is_ok(), "the gateway's own id stays its record: {gateway_id}");
}

#[tokio::test]
async fn the_decision_log_records_each_fold_outcome_without_message_text() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let body = marked_folding_body(6, 17);
    let (status, _, _) = gw.post_chat(TEST_KEY, body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let sent_body = upstream.last_body().unwrap();
    let (decision, fold) = decision_record(prompt_tokens_of(&sent_body));
    assert!(!decision.contains(FOLD_MARKER), "message text reached the decision line: {decision}");
    assert!(!decision.contains("Hello, world"), "summary text reached the decision line: {decision}");

    let messages = serde_json::from_value::<Vec<llm_gateway::providers::ChatMessage>>(body["messages"].clone()).unwrap();
    let (protected, turns) = llm_gateway::folding::split_messages(&messages);
    let plan = llm_gateway::folding::plan::plan_fold(
        &llm_gateway::folding::plan::PlanConfig {
            keep_last: 2,
            summary_chars: 200,
            summarizer_max_tokens: 64,
            max_folds: 8,
            refold_threshold: 8,
            min_new_turns: 2,
            min_new_tokens: 0,
            amortization_requests: 1,
            min_saving_nano_usd: 0,
        },
        &llm_gateway::router::CostModel { input_per_mtok_usd: 2.0, output_per_mtok_usd: 8.0, cached_input_per_mtok_usd: None },
        &llm_gateway::router::CostModel { input_per_mtok_usd: 0.01, output_per_mtok_usd: 0.02, cached_input_per_mtok_usd: None },
        &protected,
        &turns,
        None,
    );
    assert_fold_shape(&fold);
    assert_eq!(fold["outcome"], "applied");
    assert_eq!(fold["reason"], "batch_ready");
    assert_eq!(fold["turns_replaced"], 4);
    assert_eq!(fold["turns_covered"], 4);
    assert_eq!(fold["benefit_nano_usd"], plan.benefit_nano_usd);
    assert_eq!(fold["overhead_nano_usd"], plan.overhead_nano_usd);
    assert_eq!(fold["min_saving_nano_usd"], plan.min_saving_nano_usd);
    // Billed like a main call: the reported 12 prompt tokens, floored at the
    // gateway's estimate of what was sent.
    let summarizer = summarizer_bodies(&upstream);
    assert_eq!(fold["summarizer_prompt_tokens"], prompt_tokens_of(&summarizer[0]).max(12));
    assert_eq!(fold["summarizer_completion_tokens"], 3);
    assert_eq!(fold["fold_count"], 1);
    assert_eq!(fold["prefix_hash"].as_str().unwrap().len(), 64);
    let sent = serde_json::from_value::<Vec<llm_gateway::providers::ChatMessage>>(sent_body["messages"].clone()).unwrap();
    let cut = u64::from(
        llm_gateway::router::estimate_prompt_tokens(&messages)
            .saturating_sub(llm_gateway::router::estimate_prompt_tokens(&sent)),
    );
    assert_eq!(fold["prompt_tokens_cut"], cut);

    let mut skipped = marked_folding_body(6, 19);
    skipped["messages"][1] = json!({
        "role": "assistant",
        "content": "calling a tool",
        "tool_calls": [{ "id": "call_1", "type": "function", "function": { "name": "lookup", "arguments": "{}" } }]
    });
    let (status, _, _) = gw.post_chat(TEST_KEY, skipped).await;
    assert_eq!(status, StatusCode::OK);
    let (_, fold) = decision_record(prompt_tokens_of(&upstream.last_body().unwrap()));
    assert_fold_shape(&fold);
    assert_eq!(fold["outcome"], "skipped");
    assert_eq!(fold["reason"], "head_has_tools");
    assert_eq!(fold["turns_replaced"], 0);
    assert_eq!(fold["summarizer_prompt_tokens"], 0);
}

#[tokio::test]
async fn a_fold_the_cost_check_refuses_is_logged_and_counted_unprofitable() {
    const EXPENSIVE: &str = r#"  mock/summarizer:
    provider: nvidia
    endpoint: http://REPLACE_ME/v1/chat/completions
    upstream_model: mock-summarizer
    cost:
      input_per_mtok_usd: 100.0
      output_per_mtok_usd: 100.0
"#;
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, EXPENSIVE, Some(FOLDING_TENANT)).await;
    let (status, headers, _) = gw.post_chat(TEST_KEY, marked_folding_body(6, 31)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none());
    assert_eq!(upstream.calls(), 1);
    let (_, fold) = decision_record(prompt_tokens_of(&upstream.last_body().unwrap()));
    assert_fold_shape(&fold);
    assert_eq!(fold["outcome"], "refused");
    assert_eq!(fold["reason"], "cost_check");
    assert!(fold["overhead_nano_usd"].as_u64().unwrap() > fold["benefit_nano_usd"].as_u64().unwrap());
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert_eq!(fold_counter(&metrics, "unprofitable"), 1, "{metrics}");
    assert_eq!(fold_counter(&metrics, "committed"), 0, "{metrics}");
}

#[tokio::test]
async fn a_failed_summarizer_is_logged_and_counted_failed() {
    let upstream = MockUpstream::start(Scenario::SummarizerDown).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let (status, headers, _) = gw.post_chat(TEST_KEY, marked_folding_body(6, 47)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none());
    let (decision, fold) = decision_record(prompt_tokens_of(&upstream.last_body().unwrap()));
    assert_fold_shape(&fold);
    assert_eq!(fold["outcome"], "failed");
    assert_eq!(fold["reason"], "provider_error");
    assert!(!decision.contains(FOLD_MARKER), "message text reached the decision line: {decision}");
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert_eq!(fold_counter(&metrics, "failed"), 1, "{metrics}");
}

#[tokio::test]
async fn fold_metric_outcomes_add_up_to_the_attempts() {
    let upstream = MockUpstream::start(Scenario::SummarizerThenEmpty).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;

    let (status, headers, _) = gw.post_chat(TEST_KEY, folding_body(6)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "4");

    let mut reused = folding_body(7);
    let last = reused["messages"].as_array().unwrap().len() - 1;
    let content = reused["messages"][last]["content"].as_str().unwrap().to_string();
    reused["messages"][last]["content"] = json!(format!("{content}{}", "y".repeat(61)));
    let (status, headers, _) = gw.post_chat(TEST_KEY, reused).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "4");
    let (_, fold) = decision_record(prompt_tokens_of(&upstream.last_body().unwrap()));
    assert_fold_shape(&fold);
    assert_eq!(fold["outcome"], "not_due");
    assert_eq!(fold["reason"], "not_enough_new_turns");
    assert_eq!(fold["fold_count"], 1);
    assert_eq!(fold["turns_replaced"], 4);

    let (status, headers, _) = gw.post_chat(TEST_KEY, folding_body(12)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none(), "a rejected fold does not replace the summary");

    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    let committed = fold_counter(&metrics, "committed");
    let rejected = fold_counter(&metrics, "rejected");
    let failed = fold_counter(&metrics, "failed");
    let unprofitable = fold_counter(&metrics, "unprofitable");
    let refused = fold_counter(&metrics, "refused_admission");
    assert_eq!((committed, rejected, failed, unprofitable, refused), (1, 1, 0, 0, 0), "{metrics}");
    assert_eq!(committed + rejected + failed + unprofitable + refused, 2);
    assert!(metrics.contains("gw_fold_requests_total{tenant=\"acme\"} 2"), "{metrics}");
    // Each call billed at its reported prompt tokens (12, then 7), floored at
    // the gateway's estimate of what was sent.
    let summarizer = summarizer_bodies(&upstream);
    let billed: u32 = summarizer.iter().zip([12, 7]).map(|(body, reported)| prompt_tokens_of(body).max(reported)).sum();
    assert!(
        metrics.contains(&format!("gw_fold_summarizer_tokens_total{{tenant=\"acme\",kind=\"prompt\"}} {billed}")),
        "{metrics}"
    );
    assert!(metrics.contains("gw_fold_summarizer_tokens_total{tenant=\"acme\",kind=\"completion\"} 7"), "{metrics}");
    assert!(metrics.contains("gw_fold_store_entries 1"), "{metrics}");
    let cut = metrics.lines().find_map(|line| {
        line.strip_prefix("gw_fold_prompt_tokens_cut_total{tenant=\"acme\"} ")
            .and_then(|rest| rest.trim().parse::<u64>().ok())
    });
    assert!(cut.unwrap_or(0) > 0, "folded requests must record the tokens they removed: {metrics}");
}

fn summarizer_bodies(upstream: &MockUpstream) -> Vec<Value> {
    upstream.bodies().into_iter().filter(|body| body["stream"] == json!(false)).collect()
}

#[tokio::test]
async fn a_background_fold_lands_on_the_next_request() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT), None, "", true).await;

    let (status, headers, _) = gw.post_chat(TEST_KEY, folding_body(6)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none(), "the triggering request is sent unfolded");
    let first = upstream
        .bodies()
        .into_iter()
        .find(|body| body["stream"] == json!(true))
        .expect("main request");
    assert!(first["messages"][0]["content"].as_str().unwrap().contains("turn 0 "));

    for _ in 0..100 {
        let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
        if metrics.contains("gw_folds_total{tenant=\"acme\",outcome=\"committed\"} 1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert!(metrics.contains("gw_folds_total{tenant=\"acme\",outcome=\"committed\"} 1"), "{metrics}");

    let (status, headers, _) = gw.post_chat(TEST_KEY, folding_body(6)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "4");
    let sent = upstream.last_body().unwrap();
    let text = sent.to_string();
    assert!(text.contains("Hello, world"), "the next request uses the committed summary: {text}");
    assert!(!text.contains("turn 0 "), "the covered turns stay out of the next request: {text}");
    assert_eq!(summarizer_bodies(&upstream).len(), 1, "the next request does not fold again");
}

#[tokio::test]
async fn two_concurrent_requests_start_one_background_fold() {
    let upstream = MockUpstream::start(Scenario::SummarizerHold).await;
    let gw = Gateway::start_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT), None, "", true).await;
    let body = folding_body(6);
    let (first, second) = tokio::join!(gw.post_chat(TEST_KEY, body.clone()), gw.post_chat(TEST_KEY, body));
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
    assert!(first.1.get("x-gateway-folded").is_none());
    assert!(second.1.get("x-gateway-folded").is_none());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(summarizer_bodies(&upstream).len(), 1, "one conversation starts one fold");
    upstream.open_gate(1);
    for _ in 0..100 {
        let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
        if metrics.contains("gw_folds_total{tenant=\"acme\",outcome=\"committed\"} 1") {
            assert_eq!(fold_counter(&metrics, "failed"), 0, "{metrics}");
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the single fold did not commit");
}

#[tokio::test]
async fn a_background_failure_keeps_the_committed_summary() {
    let upstream = MockUpstream::start(Scenario::SummarizerThenDown).await;
    let gw = Gateway::start_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT), None, "", true).await;

    assert_eq!(gw.post_chat(TEST_KEY, folding_body(6)).await.0, StatusCode::OK);
    let mut committed = false;
    for _ in 0..100 {
        let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
        if metrics.contains("gw_folds_total{tenant=\"acme\",outcome=\"committed\"} 1") {
            committed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(committed, "the first background fold did not commit");

    let (status, headers, _) = gw.post_chat(TEST_KEY, folding_body(8)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "4");
    assert!(upstream.last_body().unwrap().to_string().contains("Hello, world"));

    for _ in 0..100 {
        let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
        if fold_counter(&metrics, "failed") == 1 {
            assert_eq!(fold_counter(&metrics, "committed"), 1, "{metrics}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert_eq!(fold_counter(&metrics, "failed"), 1, "{metrics}");
    assert_eq!(fold_counter(&metrics, "committed"), 1, "{metrics}");

    let (status, headers, _) = gw.post_chat(TEST_KEY, folding_body(7)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "4");
    let text = upstream.last_body().unwrap().to_string();
    assert!(text.contains("Hello, world"), "the failed fold did not replace the summary: {text}");
    assert_eq!(summarizer_bodies(&upstream).len(), 2, "the reuse does not start a third fold");
}

#[tokio::test]
async fn shutdown_releases_a_background_fold_reservation() {
    let upstream = MockUpstream::start(Scenario::SummarizerHold).await;
    let gw = Gateway::start_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT), None, "", true).await;
    let (status, _, _) = gw.post_chat(TEST_KEY, folding_body(6)).await;
    assert_eq!(status, StatusCode::OK);

    let mut summarizer = None;
    for _ in 0..100 {
        summarizer = summarizer_bodies(&upstream).into_iter().next();
        if summarizer.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let summarizer = summarizer.expect("the background fold did not reach the summarizer");
    let messages = serde_json::from_value::<Vec<llm_gateway::providers::ChatMessage>>(summarizer["messages"].clone()).unwrap();
    let prompt_tokens = llm_gateway::router::estimate_prompt_tokens(&messages);
    let max_tokens = summarizer["max_tokens"].as_u64().unwrap() as u32;
    let reserved = llm_gateway::router::estimate_cost_nano_usd(
        &llm_gateway::router::CostModel {
            input_per_mtok_usd: 0.01,
            output_per_mtok_usd: 0.02,
            cached_input_per_mtok_usd: None,
        },
        prompt_tokens,
        max_tokens,
    );
    let (status, usage) = gw.get("/v1/usage", TEST_KEY).await;
    assert_eq!(status, StatusCode::OK, "{usage}");
    let usage: Value = serde_json::from_str(&usage).unwrap();
    assert!(usage["streams_in_flight"].as_u64().unwrap() >= 1, "the summarizer reservation is outstanding: {usage}");
    let spent_before = usage["spent_nano_usd"].as_u64().unwrap();

    gw.state.folding.cancel();
    let mut spent_after = spent_before;
    for _ in 0..100 {
        let (_, text) = gw.get("/v1/usage", TEST_KEY).await;
        let usage: Value = serde_json::from_str(&text).unwrap();
        spent_after = usage["spent_nano_usd"].as_u64().unwrap();
        if usage["streams_in_flight"] == json!(0) && spent_after < spent_before {
            assert_eq!(spent_before - spent_after, reserved, "release refunds the whole reservation, not just the completion half");
            let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
            assert_eq!(fold_counter(&metrics, "failed"), 1, "{metrics}");
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("reservation stayed outstanding: before {spent_before} after {spent_after} reserved {reserved}");
}

#[tokio::test]
async fn public_metrics_need_an_admin_key() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;
    assert_eq!(gw.get("/metrics", "").await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(gw.get("/metrics", "test-key-throttled").await.0, StatusCode::FORBIDDEN);
    assert_eq!(gw.get("/metrics", TEST_KEY).await.0, StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Non-streaming completions: POST /v1/chat/complete
// ---------------------------------------------------------------------------

fn complete_body() -> Value {
    json!({
        "model": "mock/chat",
        "messages": [{ "role": "user", "content": "hi" }],
        "params": { "max_tokens": 128 },
        "metadata": { "trace": "abc" }
    })
}

fn reserved_of(headers: &HeaderMap) -> u64 {
    headers["x-reserved-nano-usd"].to_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn a_completion_is_one_json_answer_billed_from_reported_usage() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let (status, headers, text) = gw.post_complete(TEST_KEY, complete_body()).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    assert!(headers.get("x-accel-buffering").is_none());
    let answer: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(answer["content"], "Hello, world");
    assert_eq!(answer["finish_reason"], "stop");
    assert_eq!(answer["model"], "mock/chat");
    assert_eq!(answer["tenant"], "acme");
    assert_eq!(answer["upstream_id"], "cc1");
    assert_eq!(answer["metadata"]["trace"], "abc");
    assert_eq!(answer["usage"]["prompt_tokens"], 12);
    assert_eq!(answer["usage"]["completion_tokens"], 3);
    // 12 x 2,000 + 3 x 8,000.
    assert_eq!(answer["cost_nano_usd"], 48_000);
    assert_eq!(spent(&gw, TEST_KEY).await, 48_000, "the answer and the ledger agree");

    // The upstream was asked for a completion, not a stream.
    let sent = upstream.last_body().unwrap();
    assert_eq!(sent["stream"], false);
    assert!(sent.get("stream_options").is_none(), "{sent}");
    assert_eq!(sent["max_tokens"], 128);
}

#[tokio::test]
async fn each_endpoint_refuses_the_other_ones_stream_flag() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let mut body = complete_body();
    body["stream"] = json!(true);
    let (status, _, text) = gw.post_complete(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text.contains("stream_not_allowed"), "{text}");

    let mut body = chat_body();
    body["stream"] = json!(false);
    let (status, _, text) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text.contains("stream_required"), "{text}");
    assert_eq!(upstream.calls(), 0);

    // `stream: false` explicitly is fine on the completion endpoint.
    let mut body = complete_body();
    body["stream"] = json!(false);
    assert_eq!(gw.post_complete(TEST_KEY, body).await.0, StatusCode::OK);
}

#[tokio::test]
async fn a_caller_cannot_turn_a_completion_into_a_stream_through_params() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;
    let mut body = complete_body();
    body["params"] = json!({ "max_tokens": 32, "stream": true, "stream_options": { "include_usage": true } });
    let (status, _, text) = gw.post_complete(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let sent = upstream.last_body().unwrap();
    assert_eq!(sent["stream"], false);
    assert!(sent.get("stream_options").is_none(), "{sent}");
}

#[tokio::test]
async fn a_refused_completion_costs_nothing() {
    let upstream = MockUpstream::start(Scenario::UpstreamReject).await;
    let gw = Gateway::start(&upstream).await;
    let (status, headers, text) = gw.post_complete(TEST_KEY, complete_body()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{text}");
    assert!(text.contains("upstream_rate_limited"), "{text}");
    assert_eq!(headers["x-error-retryable"], "true");
    assert_eq!(spent(&gw, TEST_KEY).await, 0);
}

#[tokio::test]
async fn an_in_band_error_in_a_completion_costs_nothing() {
    // A 200 whose body is `{"error": ...}` is a refusal delivered in-band:
    // retryable, and nothing was processed.
    let upstream = MockUpstream::start(Scenario::MidStreamError).await;
    let gw = Gateway::start(&upstream).await;
    let (status, _, text) = gw.post_complete(TEST_KEY, complete_body()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{text}");
    let err: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(err["error"]["retryable"], true);
    assert_eq!(spent(&gw, TEST_KEY).await, 0);
}

#[tokio::test]
async fn a_completion_without_usage_bills_what_it_returned() {
    let upstream = MockUpstream::start(Scenario::NoUsage).await;
    let gw = Gateway::start(&upstream).await;
    let (status, headers, text) = gw.post_complete(TEST_KEY, complete_body()).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let answer: Value = serde_json::from_str(&text).unwrap();
    let completion = answer["usage"]["completion_tokens"].as_u64().unwrap();
    assert!(completion >= 2, "estimated from the returned text: {answer}");
    let billed = spent(&gw, TEST_KEY).await;
    assert_eq!(billed, answer["cost_nano_usd"].as_u64().unwrap());
    assert!(billed < reserved_of(&headers), "the unused ceiling is refunded");
}

#[tokio::test]
async fn a_completion_forwards_tool_calls_and_keeps_reasoning_separate() {
    let upstream = MockUpstream::start(Scenario::ToolCalls).await;
    let gw = Gateway::start(&upstream).await;
    let (_, _, text) = gw.post_complete(TEST_KEY, complete_body()).await;
    let answer: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(answer["content"], "");
    assert_eq!(answer["finish_reason"], "tool_calls");
    let call = &answer["tool_calls"][0];
    assert_eq!(call["id"], "call_1");
    assert_eq!(call["type"], "function");
    assert_eq!(call["function"]["name"], "get_weather");
    assert_eq!(call["function"]["arguments"], "{\"city\":\"NYC\"}");

    let upstream = MockUpstream::start(Scenario::Reasoning).await;
    let gw = Gateway::start(&upstream).await;
    let (_, _, text) = gw.post_complete(TEST_KEY, complete_body()).await;
    let answer: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(answer["content"], "answer");
    assert_eq!(answer["reasoning"], "let me think");
}

#[tokio::test]
async fn a_client_that_leaves_mid_completion_is_billed_the_reservation() {
    // The upstream is still answering when the client gives up. A
    // non-streaming vendor finishes and bills the whole answer regardless,
    // and the gateway will never see its usage, so the reservation is the
    // bill, exactly as for passthrough.
    let upstream = MockUpstream::start(Scenario::SlowFirstToken).await;
    let gw = Gateway::start(&upstream).await;

    // Same body, so the same reservation; this one is allowed to finish.
    let (status, headers, _) = gw.post_complete(TEST_KEY, complete_body()).await;
    assert_eq!(status, StatusCode::OK);
    let reserved = reserved_of(&headers);
    let before = spent(&gw, TEST_KEY).await;

    let impatient = reqwest::Client::builder()
        .timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let result = impatient
        .post(gw.url("/v1/chat/complete"))
        .header("x-api-key", TEST_KEY)
        .json(&complete_body())
        .send()
        .await;
    assert!(result.is_err(), "the client must time out before the answer");
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(spent(&gw, TEST_KEY).await - before, reserved);
    let (_, usage) = gw.get("/v1/usage", TEST_KEY).await;
    assert!(usage.contains("\"streams_in_flight\":0"), "{usage}");
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert!(metrics.contains("gw_streams_client_aborted_total 1"), "{metrics}");
}

#[tokio::test]
async fn completions_are_governed_like_streams() {
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start(&upstream).await;

    let (status, _, text) = gw.post_complete("test-key-broke", complete_body()).await;
    assert_eq!(status, StatusCode::PAYMENT_REQUIRED, "{text}");
    let (status, _, _) = gw.post_complete("wrong-key", complete_body()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, _) = gw.post_complete("test-key-locked", complete_body()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let mut body = complete_body();
    body["model"] = json!("mock/uncapped");
    body["params"] = json!({});
    let (status, _, text) = gw.post_complete(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text.contains("max_tokens_required"), "{text}");
    for _ in 0..2 {
        assert_eq!(gw.post_complete("test-key-throttled", complete_body()).await.0, StatusCode::OK);
    }
    assert_eq!(
        gw.post_complete("test-key-throttled", complete_body()).await.0,
        StatusCode::TOO_MANY_REQUESTS,
        "completions and streams share one rate limit"
    );
    assert_eq!(upstream.calls(), 2, "no refused request reached the provider");
}

#[tokio::test]
async fn a_value_in_the_system_prompt_does_not_block_a_fold() {
    // System messages are sent verbatim and never reach the summarizer, so
    // their values are not pinned. Pinning them refused every fold after the
    // summarizer had been paid for.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let mut body = folding_body(30);
    let mut messages = body["messages"].as_array().unwrap().clone();
    messages.insert(0, json!({ "role": "system", "content": "Office hours are 09:00 to 17:00." }));
    body["messages"] = Value::Array(messages);
    let (status, headers, _) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "28");
    assert_eq!(summarizer_bodies(&upstream).len(), 1);
    let sent = upstream.bodies().into_iter().rev().find(|b| b["stream"] == json!(true)).unwrap();
    assert_eq!(sent["messages"][0]["content"], "Office hours are 09:00 to 17:00.", "the system prompt still goes out verbatim");
}

#[tokio::test]
async fn a_fold_never_takes_the_last_rate_limit_slot() {
    // acme allows 60 requests a minute. With 59 used, a fold would take the
    // 60th slot and the request it serves would get 429. It must go out
    // unfolded instead, with no summarizer call.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    for _ in 0..59 {
        assert_eq!(gw.post_chat(TEST_KEY, folding_body(2)).await.0, StatusCode::OK);
    }
    let (status, headers, text) = gw.post_chat(TEST_KEY, folding_body(30)).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert!(headers.get("x-gateway-folded").is_none());
    assert!(summarizer_bodies(&upstream).is_empty(), "no summarizer call without room for it");
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    assert_eq!(fold_counter(&metrics, "refused_admission"), 1, "{metrics}");
}

#[tokio::test]
async fn a_summarizer_that_reports_no_usage_is_billed_its_output() {
    // NoUsage: the summarizer reply carries no usage. Its output is billed at
    // the gateway's estimate, as a main completion's is, not as zero.
    let upstream = MockUpstream::start(Scenario::NoUsage).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let (status, headers, _) = gw.post_chat(TEST_KEY, folding_body(30)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "28");
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    let completion = metrics
        .lines()
        .find_map(|line| line.strip_prefix("gw_fold_summarizer_tokens_total{tenant=\"acme\",kind=\"completion\"} "))
        .unwrap_or_else(|| panic!("{metrics}"));
    assert!(completion.parse::<u64>().unwrap() > 0, "{metrics}");
    let prompt = metrics
        .lines()
        .find_map(|line| line.strip_prefix("gw_fold_summarizer_tokens_total{tenant=\"acme\",kind=\"prompt\"} "))
        .unwrap();
    assert!(prompt.parse::<u64>().unwrap() > 0, "{metrics}");
}

#[tokio::test]
async fn two_concurrent_requests_start_one_foreground_fold() {
    let upstream = MockUpstream::start(Scenario::SummarizerHold).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;
    let body = folding_body(6);
    let first = tokio::spawn({
        let gw_url = gw.url("/v1/chat/stream");
        let body = body.clone();
        async move {
            reqwest::Client::new().post(gw_url).header("x-api-key", TEST_KEY).json(&body).send().await.unwrap()
        }
    });
    for _ in 0..100 {
        if summarizer_bodies(&upstream).len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The first request's fold is held at the summarizer.
    let (status, headers, _) = gw.post_chat(TEST_KEY, body).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-gateway-folded").is_none(), "no summary yet, so unfolded");
    assert_eq!(summarizer_bodies(&upstream).len(), 1, "the second request started no fold");
    upstream.open_gate(1);
    let first = first.await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert!(first.headers().get("x-gateway-folded").is_some());
}

#[tokio::test]
async fn the_summarizer_is_resolved_with_the_tenant_model_suffix() {
    // A tenant with `model_suffix: ":prod"` calls `mock/chat:prod`, and its
    // summarizer `mock/summarizer` must resolve as `mock/summarizer:prod`,
    // the entry load-time validation checked.
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let models = r#"  mock/chat:prod:
    provider: nvidia
    endpoint: http://REPLACE_ME/v1/chat/completions
    upstream_model: mock-model-prod
    default_params:
      max_tokens: 256
    cost:
      input_per_mtok_usd: 2.0
      output_per_mtok_usd: 8.0
  mock/summarizer:prod:
    provider: nvidia
    endpoint: http://REPLACE_ME/v1/chat/completions
    upstream_model: mock-summarizer-prod
    default_params:
      max_tokens: 64
    cost:
      input_per_mtok_usd: 0.01
      output_per_mtok_usd: 0.02
"#;
    let gw = Gateway::start_folding(&upstream, models, Some(FOLDING_TENANT), None, "    model_suffix: \":prod\"\n", false).await;
    let (status, headers, text) = gw.post_chat(TEST_KEY, folding_body(30)).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(headers.get("x-gateway-folded").unwrap(), "28");
    let summarizer = summarizer_bodies(&upstream);
    assert_eq!(summarizer.len(), 1);
    assert_eq!(summarizer[0]["model"], "mock-summarizer-prod");
}

/// The same growing conversation sent to two gateways, one without folding
/// and one with it. Spend is each gateway's own ledger, so the summarizer
/// calls, any fold that is refused or fails, and the main calls are counted
/// exactly as the tenant is billed. The mock's usage is canned (12 prompt, 3
/// completion on every reply), so the ledger bills the gateway's own prompt
/// estimate as the floor; the summarizer's output is billed at what the mock
/// reports, which is far below a real summary. Read the net as an upper bound
/// on what folding saves with this summarizer's prices, not as a measurement
/// of a real model.
///
/// Run with `cargo test --locked --test integration fold_economics -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "measurement, not a check"]
async fn fold_economics() {
    const START_TURNS: usize = 30;
    const REQUESTS: usize = 10;

    let plain_upstream = MockUpstream::start(Scenario::Happy).await;
    let plain = Gateway::start(&plain_upstream).await;
    let upstream = MockUpstream::start(Scenario::Happy).await;
    let gw = Gateway::start_with_folding(&upstream, FOLDING_SUMMARIZER_MODEL, Some(FOLDING_TENANT)).await;

    println!("fold_economics: {REQUESTS} requests, {START_TURNS} turns growing by 2 per request");
    println!("  req turns folded | main prompt tok: plain folded | summarizer: calls in_tok | spend nano-USD: plain folded | net");
    let (mut plain_spent, mut folded_spent) = (0u64, 0u64);
    let (mut plain_prompt, mut folded_prompt, mut summarizer_in, mut summarizer_calls) = (0u64, 0u64, 0u64, 0usize);
    let mut seen = 0;
    for request in 0..REQUESTS {
        let turns = START_TURNS + 2 * request;
        let body = folding_body(turns);
        let (plain_before, before) = (spent(&plain, TEST_KEY).await, spent(&gw, TEST_KEY).await);
        assert_eq!(plain.post_chat(TEST_KEY, body.clone()).await.0, StatusCode::OK);
        let (status, headers, _) = gw.post_chat(TEST_KEY, body).await;
        assert_eq!(status, StatusCode::OK);
        // Both reservations settle when their streams close.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let plain_cost = spent(&plain, TEST_KEY).await - plain_before;
        let folded_cost = spent(&gw, TEST_KEY).await - before;

        let plain_tokens = prompt_tokens_of(&plain_upstream.last_body().unwrap());
        let bodies = upstream.bodies();
        let new = &bodies[seen..];
        seen = bodies.len();
        let main = new.iter().rev().find(|item| item["stream"] == json!(true)).expect("main call");
        let folds: Vec<&Value> = new.iter().filter(|item| item["stream"] == json!(false)).collect();
        let fold_in: u64 = folds.iter().map(|item| u64::from(prompt_tokens_of(item))).sum();
        let main_tokens = prompt_tokens_of(main);

        plain_spent += plain_cost;
        folded_spent += folded_cost;
        plain_prompt += u64::from(plain_tokens);
        folded_prompt += u64::from(main_tokens);
        summarizer_in += fold_in;
        summarizer_calls += folds.len();
        let folded = headers.get("x-gateway-folded").map(|v| v.to_str().unwrap().to_string()).unwrap_or_else(|| "-".into());
        println!(
            "  {:>3} {:>5} {:>6} | {:>12} {:>6} | {:>15} {:>6} | {:>21} {:>6} | {:>+8}",
            request + 1,
            turns,
            folded,
            plain_tokens,
            main_tokens,
            folds.len(),
            fold_in,
            plain_cost,
            folded_cost,
            plain_spent as i64 - folded_spent as i64,
        );
    }
    let (_, metrics) = gw.get("/metrics", TEST_KEY).await;
    let reported: Vec<&str> = metrics
        .lines()
        .filter(|line| line.starts_with("gw_fold_summarizer_tokens_total") || line.starts_with("gw_folds_total"))
        .collect();
    println!("  totals: main prompt tokens plain {plain_prompt}, folded {folded_prompt}");
    println!("  totals: summarizer calls {summarizer_calls}, summarizer prompt tokens sent {summarizer_in}");
    println!(
        "  totals: spend plain {plain_spent} nano-USD, folded {folded_spent} nano-USD, net saving {} nano-USD",
        plain_spent as i64 - folded_spent as i64
    );
    println!("  provider-reported (canned by the mock): {}", reported.join(" | "));
}

// ---------------------------------------------------------------------------
// Captured logs
// ---------------------------------------------------------------------------

/// One process-wide subscriber, at the shipped filter's `debug` for the
/// gateway plus everything at `info`, writing into a buffer the redaction
/// test reads. Every test in this binary logs into it, so a leak from any of
/// them fails the check.
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

    pub fn install() {
        INSTALLED.get_or_init(|| {
            let subscriber = tracing_subscriber::fmt()
                .json()
                .with_env_filter(tracing_subscriber::EnvFilter::new("info,llm_gateway=trace"))
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
