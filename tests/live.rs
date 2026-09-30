//! Live provider tests: the gateway against the real Groq, OpenRouter and
//! NVIDIA APIs.
//!
//! Every other suite talks to a mock provider written alongside it, so it
//! proves the governance logic but not that the adapters still match what the
//! vendors actually send. This one checks, per vendor, with the model the
//! shipped catalogue (`config/models.yaml`) names:
//!
//! * a streamed answer follows the v1 contract, and is billed exactly what
//!   its reported usage costs at the catalogue's prices;
//! * **the reservation made before the call covered the bill**, which is what
//!   lets the budget refuse up front;
//! * a completion is one well-formed document, billed the same way;
//! * passthrough relays the vendor's stream and bills the reservation;
//! * a client hanging up mid-stream frees its slot and is billed for what it
//!   received, not for the whole reservation;
//! * a model the vendor does not know is a clean 502 that costs nothing.
//!
//! Content is never asserted, only shape and money.
//!
//! **Opt-in, because it spends money.** Nothing runs unless
//! `LLM_GATEWAY_LIVE` names the providers (`groq,openrouter,nvidia` or
//! `all`); a named provider whose key is not set fails rather than skips.
//! Each test's tenant has a daily budget of 5 cents, so a runaway test is
//! refused by the gateway itself. A full run costs well under a cent.
//!
//! ```text
//! LLM_GATEWAY_LIVE=all cargo test --test live -- --nocapture
//! ```
//!
//! `LLM_GATEWAY_LIVE_GROQ_MODEL` (and `_OPENROUTER_`, `_NVIDIA_`) picks
//! another catalogue model. Each run appends what it measured (estimated vs
//! billed prompt tokens, reserved vs billed) to `target/live-report.jsonl`.
//!
//! The checks themselves are tested on every run, with no key and no cost:
//! `the_checks_pass_against_a_local_openai_compatible_server` runs them all
//! against a local server that speaks the vendors' wire format.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};

const KEY: &str = "live-key";

/// The tenant every live gateway serves: any model, 5 cents a day.
const TENANTS: &str = r#"tenants:
  - tenant_id: live
    enabled: true
    credentials:
      - key_id: ak_live
        key: live-key
        scopes: [chat:stream]
    allowed_models: ["*"]
    limits:
      requests_per_minute: 120
      tokens_per_minute: 1000000
      max_concurrent_streams: 8
      max_output_tokens: 2048
      daily_budget_nano_usd: 50000000
"#;

/// An upstream model name no vendor serves.
const NO_SUCH_MODEL: &str = "llm-gateway-no-such-model";

// ---------------------------------------------------------------------------
// A gateway, booted in process from config files
// ---------------------------------------------------------------------------

struct Gateway {
    addr: SocketAddr,
    task: JoinHandle<()>,
    dir: PathBuf,
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Gateway {
    async fn boot(models_yaml: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("llm-gateway-live-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("models.yaml"), models_yaml).unwrap();
        std::fs::write(dir.join("tenants.yaml"), TENANTS).unwrap();
        let settings = llm_gateway::config::Settings {
            server: llm_gateway::config::ServerConfig {
                bind_addr: "127.0.0.1".into(),
                port: 0,
                ..Default::default()
            },
            registry: llm_gateway::config::RegistryConfig {
                models_path: dir.join("models.yaml"),
                tenants_path: dir.join("tenants.yaml"),
                hot_reload: false,
                reload_interval_ms: 60_000,
            },
            auth: llm_gateway::config::AuthConfig { allow_anonymous: false, ..Default::default() },
            ledger: llm_gateway::config::LedgerConfig {
                backend: llm_gateway::config::LedgerBackend::Memory,
                ..Default::default()
            },
            ..Default::default()
        };
        let state = llm_gateway::bootstrap::build(settings).await.expect("boot gateway");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = llm_gateway::api::router(state);
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, task, dir }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn post(&self, path: &str, body: &Value) -> reqwest::RequestBuilder {
        reqwest::Client::new().post(self.url(path)).header("x-api-key", KEY).json(body)
    }

    async fn usage(&self) -> Value {
        let reply = reqwest::Client::new().get(self.url("/v1/usage")).header("x-api-key", KEY).send().await.unwrap();
        assert_eq!(reply.status(), StatusCode::OK);
        reply.json().await.unwrap()
    }

    async fn spent(&self) -> u64 {
        self.usage().await["spent_nano_usd"].as_u64().unwrap()
    }

    /// Spend, once it has stopped moving: a closing lands just after the
    /// last byte of a response.
    async fn settled_spend(&self) -> u64 {
        let mut last = self.spent().await;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = self.spent().await;
            if now == last && self.usage().await["streams_in_flight"] == 0 {
                return now;
            }
            last = now;
        }
        panic!("spend never settled");
    }
}

// ---------------------------------------------------------------------------
// The checks, run against a real vendor or the local stand-in alike
// ---------------------------------------------------------------------------

/// What one provider is checked with.
struct Target {
    provider: &'static str,
    /// The catalogue model under test.
    model: String,
    /// A catalogue entry for this provider whose upstream model does not exist.
    missing: String,
    /// Nano-USD per prompt and completion token, from the catalogue.
    input_rate: u64,
    output_rate: u64,
}

impl Target {
    async fn new(provider: &'static str, model: String, models_yaml: &Path) -> Self {
        let registry = llm_gateway::router::ModelRegistry::load(models_yaml, false, 0).expect("load catalogue");
        let snapshot = registry.snapshot().await;
        let cost = &snapshot.by_id.get(&model).unwrap_or_else(|| panic!("`{model}` is not in the catalogue")).cost;
        Self {
            provider,
            missing: format!("live/missing-{provider}"),
            input_rate: cost.input_nano_usd_per_token(),
            output_rate: cost.output_nano_usd_per_token(),
            model,
        }
    }

    fn request(&self, prompt: &str, max_tokens: u32) -> Value {
        json!({
            "model": self.model,
            "messages": [{ "role": "user", "content": prompt }],
            "params": { "max_tokens": max_tokens }
        })
    }
}

/// `(event name, data)` for each SSE event in a body.
fn events(body: &str) -> Vec<(Option<String>, String)> {
    body.split("\n\n")
        .filter_map(|block| {
            let mut name = None;
            let mut data = String::new();
            for line in block.lines() {
                if let Some(event) = line.strip_prefix("event: ") {
                    name = Some(event.to_string());
                } else if let Some(chunk) = line.strip_prefix("data: ") {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(chunk);
                }
            }
            (name.is_some() || !data.is_empty()).then_some((name, data))
        })
        .collect()
}

fn reserved(headers: &reqwest::header::HeaderMap) -> u64 {
    headers
        .get("x-reserved-nano-usd")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .expect("x-reserved-nano-usd")
}

fn estimated_prompt_tokens(request: &Value) -> u32 {
    let messages: Vec<llm_gateway::providers::ChatMessage> =
        serde_json::from_value(request["messages"].clone()).expect("messages");
    llm_gateway::router::estimate_prompt_tokens(&messages)
}

/// What one streamed answer measured. Appended to the report.
#[derive(Debug)]
struct Measured {
    upstream_model: String,
    estimated_prompt_tokens: u32,
    /// The larger of the vendor's figure and the estimate: what was billed.
    billed_prompt_tokens: u64,
    completion_tokens: u64,
    finish_reason: String,
    reserved_nano_usd: u64,
    billed_nano_usd: u64,
    ttft_ms: u64,
}

async fn check_stream(gw: &Gateway, t: &Target) -> Measured {
    let request = t.request("Reply with exactly one word: ready.", 256);
    let before = gw.settled_spend().await;
    let reply = gw.post("/v1/chat/stream", &request).send().await.unwrap();
    let status = reply.status();
    let headers = reply.headers().clone();
    let body = reply.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{}: {body}", t.provider);
    assert!(headers[header::CONTENT_TYPE].to_str().unwrap().starts_with("text/event-stream"));

    let evs = events(&body);
    assert_eq!(evs.first().and_then(|e| e.0.as_deref()), Some("start"), "{}: {body}", t.provider);
    assert!(evs.iter().all(|e| e.0.as_deref() != Some("error")), "{}: in-band error: {body}", t.provider);
    assert_eq!(evs.last().map(|e| e.1.as_str()), Some("[DONE]"), "{}: {body}", t.provider);
    let said = evs.iter().any(|(name, data)| {
        (name.is_none() && data.starts_with("{\"token\"")) || name.as_deref() == Some("reasoning")
    });
    assert!(said, "{}: no token or reasoning frame: {body}", t.provider);
    let end: Value = evs
        .iter()
        .find(|e| e.0.as_deref() == Some("end"))
        .map(|e| serde_json::from_str(&e.1).unwrap())
        .unwrap_or_else(|| panic!("{}: no end event: {body}", t.provider));
    let prompt = end["usage"]["prompt_tokens"].as_u64().unwrap_or(0);
    let completion = end["usage"]["completion_tokens"].as_u64().unwrap_or(0);
    assert!(prompt > 0 && completion > 0, "{}: the vendor must report usage: {end}", t.provider);
    let cost = end["cost_nano_usd"].as_u64().expect("cost_nano_usd");

    // Billed at the catalogue's prices, prompt at the larger of what was
    // reported and what was estimated. A cached-prompt discount can only
    // lower the prompt half.
    let estimated = estimated_prompt_tokens(&request);
    let full = prompt.max(u64::from(estimated)) * t.input_rate + completion * t.output_rate;
    assert!(cost <= full && cost >= completion * t.output_rate, "{}: cost {cost}, expected up to {full}", t.provider);
    let billed = gw.settled_spend().await - before;
    assert_eq!(billed, cost, "{}: the ledger bills what the end event says", t.provider);
    let reserved = reserved(&headers);
    assert!(
        billed <= reserved,
        "{}: billed {billed} nano-USD against a reservation of {reserved}: the pre-flight estimate \
         ({estimated} prompt tokens) did not cover the {prompt} billed",
        t.provider
    );
    Measured {
        upstream_model: headers["x-upstream-model"].to_str().unwrap().to_string(),
        estimated_prompt_tokens: estimated,
        billed_prompt_tokens: prompt,
        completion_tokens: completion,
        finish_reason: end["finish_reason"].as_str().unwrap_or_default().to_string(),
        reserved_nano_usd: reserved,
        billed_nano_usd: billed,
        ttft_ms: end["ttft_ms"].as_u64().unwrap_or(0),
    }
}

async fn check_completion(gw: &Gateway, t: &Target) {
    let request = t.request("Reply with exactly one word: ready.", 256);
    let before = gw.settled_spend().await;
    let reply = gw.post("/v1/chat/complete", &request).send().await.unwrap();
    let status = reply.status();
    let reserved = reserved(reply.headers());
    let body: Value = reply.json().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{}: {body}", t.provider);
    let answered = ["content", "reasoning"].iter().any(|f| body[f].as_str().is_some_and(|s| !s.is_empty()));
    assert!(answered, "{}: an empty answer: {body}", t.provider);
    assert!(body["usage"]["prompt_tokens"].as_u64().unwrap_or(0) > 0, "{}: {body}", t.provider);
    let cost = body["cost_nano_usd"].as_u64().expect("cost_nano_usd");
    let billed = gw.settled_spend().await - before;
    assert_eq!(billed, cost, "{}: the ledger bills what the answer says", t.provider);
    assert!(billed <= reserved, "{}: billed {billed} against a reservation of {reserved}", t.provider);
}

async fn check_passthrough(gw: &Gateway, t: &Target) {
    let request = t.request("Reply with exactly one word: ready.", 64);
    let before = gw.settled_spend().await;
    let reply = gw.post("/v1/chat/stream", &request).header("x-gateway-framing", "passthrough").send().await.unwrap();
    let status = reply.status();
    let reserved = reserved(reply.headers());
    let body = reply.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{}: {body}", t.provider);
    assert!(body.contains("data:") && body.contains("[DONE]"), "{}: not the vendor's stream: {body}", t.provider);
    assert!(!body.contains("event: start"), "{}: passthrough must not re-frame", t.provider);
    let billed = gw.settled_spend().await - before;
    assert_eq!(billed, reserved, "{}: passthrough bills the reservation", t.provider);
}

async fn check_hang_up(gw: &Gateway, t: &Target) {
    let request = t.request("Count from 1 to 400, one number per line, nothing else.", 1024);
    let before = gw.settled_spend().await;
    let reply = gw.post("/v1/chat/stream", &request).send().await.unwrap();
    assert_eq!(reply.status(), StatusCode::OK, "{}", t.provider);
    let reserved = reserved(reply.headers());
    let mut stream = reply.bytes_stream();
    let mut seen = String::new();
    while !(seen.contains("{\"token\"") || seen.contains("event: reasoning")) {
        let chunk = tokio::time::timeout(Duration::from_secs(60), stream.next())
            .await
            .unwrap_or_else(|_| panic!("{}: no output within a minute", t.provider))
            .unwrap_or_else(|| panic!("{}: the stream ended before any output: {seen}", t.provider))
            .unwrap();
        seen.push_str(&String::from_utf8_lossy(&chunk));
    }
    drop(stream);

    let billed = gw.settled_spend().await - before;
    assert_eq!(gw.usage().await["streams_in_flight"], 0, "{}: the slot was freed", t.provider);
    if t.output_rate > 0 {
        assert!(billed > 0, "{}: the prompt and what was delivered are owed", t.provider);
        assert!(billed < reserved, "{}: billed {billed}, the whole reservation {reserved}", t.provider);
    }
}

async fn check_unknown_model(gw: &Gateway, t: &Target) -> String {
    let before = gw.settled_spend().await;
    let reply = gw
        .post(
            "/v1/chat/stream",
            &json!({ "model": t.missing, "messages": [{ "role": "user", "content": "hi" }], "params": { "max_tokens": 16 } }),
        )
        .send()
        .await
        .unwrap();
    let status = reply.status();
    let body: Value = reply.json().await.unwrap();
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{}: {body}", t.provider);
    let code = body["error"]["code"].as_str().unwrap_or_default().to_string();
    assert!(code.starts_with("upstream"), "{}: {body}", t.provider);
    assert_eq!(gw.settled_spend().await, before, "{}: a refused request costs nothing", t.provider);
    code
}

/// Every check, in order, against one gateway.
async fn check_all(gw: &Gateway, t: &Target) -> (Measured, String) {
    let measured = check_stream(gw, t).await;
    check_completion(gw, t).await;
    check_passthrough(gw, t).await;
    check_hang_up(gw, t).await;
    let refusal = check_unknown_model(gw, t).await;
    (measured, refusal)
}

// ---------------------------------------------------------------------------
// The real vendors
// ---------------------------------------------------------------------------

fn catalogue() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("config").join("models.yaml")
}

/// The shipped catalogue plus, for each provider, an entry whose upstream
/// model does not exist.
fn catalogue_with_missing_models() -> String {
    let mut yaml = std::fs::read_to_string(catalogue()).expect("config/models.yaml");
    for provider in ["groq", "openrouter", "nvidia"] {
        yaml.push_str(&format!(
            "\n  live/missing-{provider}:\n    provider: {provider}\n    upstream_model: {NO_SUCH_MODEL}\n    default_params:\n      max_tokens: 16\n    cost:\n      input_per_mtok_usd: 1.0\n      output_per_mtok_usd: 1.0\n"
        ));
    }
    yaml
}

/// Whether `LLM_GATEWAY_LIVE` asks for this provider.
fn selected(provider: &str) -> bool {
    std::env::var("LLM_GATEWAY_LIVE")
        .map(|v| v.split(',').map(str::trim).any(|p| p == provider || p == "all"))
        .unwrap_or(false)
}

async fn live(provider: &'static str, key_env: &str, default_model: &str) {
    if !selected(provider) {
        eprintln!("skipped: set LLM_GATEWAY_LIVE={provider} (or all) to call {provider} for real");
        return;
    }
    assert!(
        std::env::var(key_env).is_ok_and(|k| !k.trim().is_empty()),
        "LLM_GATEWAY_LIVE asks for {provider}, but {key_env} is not set"
    );
    let model = std::env::var(format!("LLM_GATEWAY_LIVE_{}_MODEL", provider.to_uppercase()))
        .unwrap_or_else(|_| default_model.to_string());
    let yaml = catalogue_with_missing_models();
    let gw = Gateway::boot(&yaml).await;
    let target = Target::new(provider, model, &gw.dir.join("models.yaml")).await;
    let (m, refusal) = check_all(&gw, &target).await;

    let line = json!({
        "at": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        "provider": provider,
        "model": target.model,
        "upstream_model": m.upstream_model,
        "estimated_prompt_tokens": m.estimated_prompt_tokens,
        "billed_prompt_tokens": m.billed_prompt_tokens,
        // The vendor counted more prompt than the gateway estimated: the
        // estimate is not the over-estimate it is meant to be for this model.
        "estimate_undershot": m.billed_prompt_tokens > u64::from(m.estimated_prompt_tokens),
        "completion_tokens": m.completion_tokens,
        "finish_reason": m.finish_reason,
        "reserved_nano_usd": m.reserved_nano_usd,
        "billed_nano_usd": m.billed_nano_usd,
        "ttft_ms": m.ttft_ms,
        "unknown_model_code": refusal,
    });
    eprintln!("live {provider}: {line}");
    let report = Path::new(env!("CARGO_MANIFEST_DIR")).join("target").join("live-report.jsonl");
    let _ = std::fs::create_dir_all(report.parent().unwrap());
    use std::io::Write as _;
    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&report) {
        let _ = writeln!(file, "{line}");
    }
}

#[tokio::test]
async fn live_groq() {
    live("groq", "GROQ_API_KEY", "groq/gpt-oss-20b").await;
}

#[tokio::test]
async fn live_openrouter() {
    live("openrouter", "OPENROUTER_API_KEY", "openrouter/gpt-4.1-mini").await;
}

#[tokio::test]
async fn live_nvidia() {
    live("nvidia", "NVIDIA_API_KEY", "nvidia/nemotron-4-340b").await;
}

// ---------------------------------------------------------------------------
// The checks, checked: a local server with the vendors' wire format
// ---------------------------------------------------------------------------

fn chunk(payload: Value) -> String {
    format!("data: {payload}\n\n")
}

/// Answers like an OpenAI-compatible vendor: a streamed or whole answer with
/// usage, a long paced stream for a counting prompt, and 404 for a model it
/// does not serve.
async fn openai_compatible(Json(body): Json<Value>) -> Response {
    if body["model"] == NO_SUCH_MODEL {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": { "message": "model not found", "type": "invalid_request_error" } })),
        )
            .into_response();
    }
    let usage = json!({ "prompt_tokens": 9, "completion_tokens": 2, "total_tokens": 11 });
    if body["stream"] != json!(true) {
        return Json(json!({
            "id": "c1", "model": "local-model",
            "choices": [{ "index": 0, "finish_reason": "stop", "message": { "role": "assistant", "content": "ready" } }],
            "usage": usage
        }))
        .into_response();
    }
    let counting = body.to_string().contains("Count from 1");
    let delta = |text: String| {
        chunk(json!({ "id": "c1", "model": "local-model", "choices": [{ "index": 0, "delta": { "content": text } }] }))
    };
    let mut frames: Vec<String> = if counting {
        (1..=400).map(|n| delta(format!("{n}\n"))).collect()
    } else {
        vec![delta("ready".into())]
    };
    frames.push(chunk(json!({ "id": "c1", "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] })));
    frames.push(chunk(json!({ "id": "c1", "choices": [], "usage": usage })));
    frames.push("data: [DONE]\n\n".to_string());
    let paced = futures_util::stream::iter(frames).then(|frame| async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok::<_, std::io::Error>(frame)
    });
    let mut response = Response::new(Body::from_stream(paced));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
    response
}

#[tokio::test]
async fn the_checks_pass_against_a_local_openai_compatible_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let app = Router::new().route("/v1/chat/completions", post(openai_compatible));
        let _ = axum::serve(listener, app).await;
    });
    // NVIDIA's adapter serves a self-hosted endpoint with no key, so the
    // local stand-in needs no credential at all.
    let entry = |id: &str, upstream_model: &str| {
        format!(
            "  {id}:\n    provider: nvidia\n    endpoint: http://{upstream}/v1/chat/completions\n    upstream_model: {upstream_model}\n    cost:\n      input_per_mtok_usd: 2.0\n      output_per_mtok_usd: 8.0\n"
        )
    };
    let yaml = format!(
        "schema_version: 1\nmodels:\n{}{}",
        entry("local/chat", "local-model"),
        entry("live/missing-local", NO_SUCH_MODEL)
    );
    let gw = Gateway::boot(&yaml).await;
    let target = Target::new("local", "local/chat".into(), &gw.dir.join("models.yaml")).await;
    let (measured, refusal) = check_all(&gw, &target).await;
    // The stand-in reports 9 prompt tokens; the estimate is 13, and the
    // larger is billed.
    assert_eq!(measured.estimated_prompt_tokens, 13);
    assert_eq!(measured.billed_prompt_tokens, 13);
    assert_eq!(measured.completion_tokens, 2);
    assert!(refusal.starts_with("upstream"), "{refusal}");
    server.abort();
}
