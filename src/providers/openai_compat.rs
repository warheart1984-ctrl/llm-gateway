//! Shared OpenAI-compatible engine: the SSE stream decoder, and the
//! one-shot JSON completion that shares its request building and errors.
//!
//! Groq, OpenRouter and NVIDIA NIM all speak the same `/chat/completions`
//! dialect. Rather than triplicate the SSE state machine, the wire handling
//! lives here and each adapter supplies a [`OpenAiCompatSpec`] describing its
//! quirks. An adapter that needs genuinely different behaviour (a non-SSE
//! transport, a bespoke error envelope) implements [`super::ChatProvider`]
//! directly instead.

use std::{collections::VecDeque, pin::Pin};

use eventsource_stream::Eventsource;
use futures_core::Stream;
use futures_util::{StreamExt, TryStreamExt};
use serde_json::{Map, Value};

use super::{
    ChatProvider, Completion, HeaderList, ProviderError, ProviderRequest, ProviderStream,
    StreamEvent, Usage, MAX_COMPLETION_TOKENS_MODELS,
};

/// Per-vendor deviations from the common dialect.
#[derive(Debug, Clone, Copy)]
pub struct QuirkFlags {
    /// Delta keys that carry reasoning tokens, in priority order.
    pub reasoning_fields: &'static [&'static str],
    /// Send `stream_options.include_usage`. Providers that reject the field
    /// get `false` and we fall back to counting.
    pub send_stream_options: bool,
    /// Force `max_completion_tokens` even for models that accept `max_tokens`.
    pub force_completion_token_field: bool,
    /// Send `seed` at all. Some vendors 400 on it.
    pub supports_seed: bool,
    /// Accept a newline-delimited JSON fallback when the response is not
    /// `text/event-stream` (self-hosted NIM does this).
    pub accept_json_lines: bool,
}

impl Default for QuirkFlags {
    fn default() -> Self {
        Self {
            reasoning_fields: &["reasoning_content"],
            send_stream_options: true,
            force_completion_token_field: false,
            supports_seed: true,
            accept_json_lines: true,
        }
    }
}

pub struct OpenAiCompatSpec {
    pub provider: &'static str,
    pub default_endpoint: String,
    /// Extra provider headers, e.g. OpenRouter's attribution headers.
    pub extra_headers: fn(&ProviderRequest) -> HeaderList,
    pub quirks: QuirkFlags,
}

pub(crate) struct OpenAiCompatAdapter {
    spec: OpenAiCompatSpec,
    api_key: Option<String>,
    client: reqwest::Client,
    /// The most bytes held for one answer, event or line.
    max_response_bytes: usize,
}

impl OpenAiCompatAdapter {
    /// Build with the transport settings from gateway config. A client that
    /// cannot be constructed is a configuration error and is surfaced at boot,
    /// not silently replaced with defaults.
    pub fn new(spec: OpenAiCompatSpec, api_key: Option<String>, client: reqwest::Client, max_response_bytes: usize) -> Self {
        Self { spec, api_key, client, max_response_bytes }
    }

}

#[async_trait::async_trait]
impl ChatProvider for OpenAiCompatAdapter {
    fn name(&self) -> &'static str {
        self.spec.provider
    }

    fn base_url(&self) -> Option<&str> {
        Some(&self.spec.default_endpoint)
    }

    async fn stream_chat(&self, req: ProviderRequest) -> Result<ProviderStream, ProviderError> {
        let resp = self.post(&req, true).await?;
        Ok(decode(data_stream(resp, self.spec.quirks, self.max_response_bytes), self.spec.quirks))
    }

    async fn complete(&self, req: ProviderRequest) -> Result<Completion, ProviderError> {
        let resp = self.post(&req, false).await?;
        // A non-streaming answer is one JSON document. A body that cannot be
        // read (reset, read timeout) is a transport failure after acceptance.
        let bytes = read_capped(resp, self.max_response_bytes).await?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|e| {
            ProviderError::Protocol(format!(
                "upstream answer was not JSON ({e}): {}",
                super::truncate(&String::from_utf8_lossy(&bytes), 160)
            ))
        })?;
        parse_completion(&value, self.spec.quirks)
    }

    /// True byte relay. `bytes_stream()` is moved into the box without
    /// inspection, so no boundary assumption is made anywhere.
    async fn stream_chat_raw(&self, req: ProviderRequest) -> Result<super::RawStream, ProviderError> {
        let resp = self.post(&req, true).await?;
        let stream = resp
            .bytes_stream()
            .map_err(|e| ProviderError::Stream(e.to_string()));
        Ok(Box::pin(stream))
    }

    fn supports_passthrough(&self) -> bool {
        true
    }
}

impl OpenAiCompatAdapter {
    /// Issue the request and validate the status. Shared by both framings and
    /// the one-shot completion so they cannot drift in headers or error
    /// handling. `streaming` selects the body's `stream` flag and the
    /// `Accept` header: ask for what the caller is prepared to read.
    async fn post(&self, req: &ProviderRequest, streaming: bool) -> Result<reqwest::Response, ProviderError> {
        let url = req.url(&self.spec)?;
        let body = build_body(req, &self.spec, streaming);

        let mut builder = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(
                reqwest::header::ACCEPT,
                if streaming { "text/event-stream" } else { "application/json" },
            );
        if let Some(key) = &self.api_key {
            builder = builder.header(reqwest::header::AUTHORIZATION, format!("Bearer {key}"));
        }
        for (name, value) in (self.spec.extra_headers)(req) {
            builder = builder.header(name, value);
        }

        // reqwest surfaces a client-construction failure as a `Builder` error
        // from `send()`, so the whole error chain is included in the message:
        // "builder error" alone is not diagnosable.
        let resp = builder
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderError::Connect {
                provider: self.spec.provider.to_string(),
                message: describe(&e),
            })?;

        let status = resp.status();
        if !status.is_success() {
            // The body is read here, before streaming starts, so an upstream
            // error is a normal HTTP status the client can act on rather than
            // an error buried inside a 200 SSE stream.
            let snippet = read_snippet(resp, ERROR_BODY_MAX).await;
            return Err(ProviderError::Upstream {
                status: status.as_u16(),
                body: snippet,
                retryable: status.as_u16() == 429 || status.is_server_error(),
            });
        }
        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// Request body
// ---------------------------------------------------------------------------

fn build_body(req: &ProviderRequest, spec: &OpenAiCompatSpec, streaming: bool) -> Map<String, Value> {
    let q = &spec.quirks;
    let mut body = Map::with_capacity(12);
    body.insert("model".into(), Value::String(req.upstream_model.clone()));
    body.insert(
        "messages".into(),
        serde_json::to_value(req.messages.as_ref()).unwrap_or(Value::Array(Vec::new())),
    );
    body.insert("stream".into(), Value::Bool(streaming));

    // `stream_options` means nothing without a stream, and some vendors 400
    // on it in a non-streaming call. Usage comes back in the body regardless.
    if streaming && q.send_stream_options {
        body.insert(
            "stream_options".into(),
            serde_json::json!({ "include_usage": req.include_usage }),
        );
    }
    if let Some(v) = req.params.temperature {
        body.insert("temperature".into(), json_f64(v));
    }
    if let Some(v) = req.params.top_p {
        body.insert("top_p".into(), json_f64(v));
    }
    if let Some(v) = req.params.max_tokens {
        let field = if q.force_completion_token_field || needs_completion_token_field(&req.upstream_model) {
            "max_completion_tokens"
        } else {
            "max_tokens"
        };
        body.insert(field.into(), Value::from(v));
    }
    if let Some(stop) = &req.params.stop {
        body.insert(
            "stop".into(),
            Value::Array(stop.iter().cloned().map(Value::String).collect()),
        );
    }
    if q.supports_seed
        && let Some(seed) = req.params.seed
    {
        body.insert("seed".into(), Value::from(seed));
    }
    if let Some(v) = req.params.presence_penalty {
        body.insert("presence_penalty".into(), json_f64(v));
    }
    if let Some(v) = req.params.frequency_penalty {
        body.insert("frequency_penalty".into(), json_f64(v));
    }
    // Anything else the caller sent (tools, tool_choice, response_format,
    // top_k, reasoning_effort, logit_bias, ...) goes straight upstream. The
    // reserved set is filtered again here rather than trusted from the caller:
    // a caller must not be able to override `stream` by smuggling it through
    // `params`.
    for (k, v) in &req.params.extra {
        if !crate::router::RESERVED_PARAM_KEYS.contains(&k.as_str()) {
            body.insert(k.clone(), v.clone());
        }
    }
    body
}

fn json_f64(v: f64) -> Value {
    serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
}

/// Flatten an error and its `source` chain into one line. reqwest wraps low
/// level causes, and the top-level message alone ("builder error", "error
/// sending request") says nothing actionable.
fn describe(err: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![err.to_string()];
    let mut source = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !parts.iter().any(|p| p == &text) {
            parts.push(text);
        }
        source = cause.source();
    }
    parts.join(": ")
}

pub(crate) fn needs_completion_token_field(upstream_model: &str) -> bool {
    MAX_COMPLETION_TOKENS_MODELS
        .iter()
        .any(|prefix| upstream_model.starts_with(prefix))
}

// ---------------------------------------------------------------------------
// Transport decoding
// ---------------------------------------------------------------------------

type DataStream = Pin<Box<dyn Stream<Item = Result<String, ProviderError>> + Send>>;

/// Normalize the transport to a stream of SSE `data:` payloads. Handles real
/// SSE and, for vendors flagged as tolerant, newline-delimited JSON.
fn data_stream(resp: reqwest::Response, quirks: QuirkFlags, max_frame: usize) -> DataStream {
    // Read content-type before `bytes_stream` consumes the response.
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    let is_sse = content_type.contains("text/event-stream");
    let bytes = resp.bytes_stream();

    if is_sse {
        // `eventsource-stream` buffers an event until its blank line; bound
        // how far it can get without one.
        let bytes = bounded_events(bytes, max_frame);
        Box::pin(bytes.eventsource().map(|res| {
            res.map(|ev| ev.data).map_err(|e| match e {
                eventsource_stream::EventStreamError::Transport(BoundedError::TooLarge(max)) => {
                    too_large("event", max)
                }
                other => ProviderError::Stream(other.to_string()),
            })
        }))
    } else if quirks.accept_json_lines {
        Box::pin(line_delimited(bytes, max_frame))
    } else {
        // Not SSE and the vendor is not tolerant of a JSON-lines fallback:
        // surface the real content-type rather than a generic parse error.
        let seen = if content_type.is_empty() { "<none>" } else { &content_type };
        Box::pin(futures_util::stream::once(futures_util::future::ready(Err(
            ProviderError::Protocol(format!("expected text/event-stream, got {seen}")),
        ))))
    }
}

/// Newline-delimited JSON fallback, for self-hosted builds that ignore
/// `text/event-stream` and emit one JSON object per line.
fn line_delimited(
    bytes: impl Stream<Item = reqwest::Result<bytes::Bytes>> + Send + 'static,
    max_line: usize,
) -> impl Stream<Item = Result<String, ProviderError>> + Send {
    // Boxed so the returned stream is `Unpin` and `unfold` can poll it without
    // pinning gymnastics at every await point.
    let bytes: Pin<Box<dyn Stream<Item = reqwest::Result<bytes::Bytes>> + Send>> = Box::pin(bytes);
    futures_util::stream::unfold((bytes, Vec::<u8>::new()), move |(mut source, mut buf)| async move {
        loop {
            let newline = buf.iter().position(|b| *b == b'\n');
            if let Some(idx) = newline {
                let line: Vec<u8> = buf.drain(..=idx).collect();
                let text = String::from_utf8_lossy(&line).trim().to_string();
                if !text.is_empty() {
                    return Some((Ok(text), (source, buf)));
                }
                continue;
            }
            match source.next().await {
                None => {
                    // Flush a trailing line that arrived without a newline.
                    let rest = String::from_utf8_lossy(&buf).trim().to_string();
                    return if rest.is_empty() {
                        None
                    } else {
                        Some((Ok(rest), (source, Vec::new())))
                    };
                }
                Some(Err(e)) => return Some((Err(ProviderError::Stream(e.to_string())), (source, Vec::new()))),
                Some(Ok(chunk)) => {
                    buf.extend_from_slice(&chunk);
                    // A line is never longer than the cap: past it with no
                    // newline, the stream is refused rather than buffered.
                    if !buf.contains(&b'\n') && buf.len() > max_line {
                        return Some((Err(too_large("line", max_line)), (source, Vec::new())));
                    }
                }
            }
        }
    })
}

/// How much of an upstream error body is kept. It is only ever logged, and
/// truncated well below this.
const ERROR_BODY_MAX: usize = 16 * 1024;

fn too_large(what: &str, max: usize) -> ProviderError {
    ProviderError::Protocol(format!("upstream {what} exceeded {max} bytes"))
}

/// Read a whole response body, refusing one larger than `max` bytes. A
/// declared length over the cap is refused before anything is read.
async fn read_capped(mut resp: reqwest::Response, max: usize) -> Result<Vec<u8>, ProviderError> {
    if resp.content_length().is_some_and(|len| len > max as u64) {
        return Err(too_large("answer", max));
    }
    let mut body = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| ProviderError::Stream(describe(&e)))? {
        if body.len() + chunk.len() > max {
            return Err(too_large("answer", max));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The first `max` bytes of a body, for an error message. The rest is never
/// read; a read failure keeps what arrived.
async fn read_snippet(mut resp: reqwest::Response, max: usize) -> String {
    let mut body = Vec::new();
    while body.len() < max {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                let room = max - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            Ok(None) | Err(_) => break,
        }
    }
    String::from_utf8_lossy(&body).into_owned()
}

/// Pass SSE bytes through unchanged, failing the stream if more than `max`
/// bytes arrive without an event boundary (a blank line). `\r` is ignored,
/// so `\r\n\r\n` counts as a boundary too.
fn bounded_events(
    bytes: impl Stream<Item = reqwest::Result<bytes::Bytes>> + Send + 'static,
    max: usize,
) -> impl Stream<Item = Result<bytes::Bytes, BoundedError>> + Send {
    let mut since_boundary = 0usize;
    let mut after_newline = false;
    let mut failed = false;
    bytes.map(move |chunk| {
        if failed {
            return Err(BoundedError::Ended);
        }
        let chunk = chunk.map_err(|e| BoundedError::Transport(e.to_string()))?;
        for &b in chunk.iter() {
            match b {
                b'\n' if after_newline => since_boundary = 0,
                b'\n' => after_newline = true,
                b'\r' => {}
                _ => after_newline = false,
            }
            since_boundary += 1;
        }
        if since_boundary > max {
            failed = true;
            return Err(BoundedError::TooLarge(max));
        }
        Ok(chunk)
    })
}

/// Why [`bounded_events`] stopped a stream.
#[derive(Debug)]
enum BoundedError {
    Transport(String),
    TooLarge(usize),
    Ended,
}

impl std::fmt::Display for BoundedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BoundedError::Transport(e) => f.write_str(e),
            BoundedError::TooLarge(max) => write!(f, "upstream event exceeded {max} bytes"),
            BoundedError::Ended => f.write_str("upstream stream already failed"),
        }
    }
}

impl std::error::Error for BoundedError {}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Default, serde::Deserialize)]
struct WireChunk {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    choices: Vec<WireChoice>,
    #[serde(default)]
    usage: Option<WireUsage>,
    /// Some vendors report failures as an in-band object with HTTP 200.
    #[serde(default)]
    error: Option<WireError>,
}

#[derive(Debug, serde::Deserialize)]
struct WireChoice {
    // Providers number choices when `n > 1`. Only index 0 is meaningful here,
    // so it is accepted and ignored rather than rejected.
    #[serde(default)]
    #[allow(dead_code)]
    index: u32,
    #[serde(default)]
    delta: Option<WireDelta>,
    #[serde(default)]
    finish_reason: Option<Value>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct WireDelta {
    // An assistant role delta opens every stream. Informational only.
    #[serde(default)]
    #[allow(dead_code)]
    role: Option<String>,
    #[serde(default)]
    content: Option<Value>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<WireToolCallDelta>>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct WireToolCallDelta {
    #[serde(default)]
    index: Option<u32>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
    #[serde(default)]
    function: Option<WireFunctionDelta>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct WireFunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, Default, Clone, serde::Deserialize)]
struct WireUsage {
    #[serde(default)]
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
    #[serde(default)]
    total_tokens: u32,
    #[serde(default)]
    completion_tokens_details: Option<WireCompletionDetails>,
    #[serde(default)]
    prompt_tokens_details: Option<WirePromptDetails>,
}

#[derive(Debug, Default, Clone, serde::Deserialize)]
struct WireCompletionDetails {
    #[serde(default)]
    reasoning_tokens: Option<u32>,
}

#[derive(Debug, Default, Clone, serde::Deserialize)]
struct WirePromptDetails {
    #[serde(default)]
    cached_tokens: Option<u32>,
}

#[derive(Debug, Default, Clone, serde::Deserialize)]
struct WireError {
    #[serde(default)]
    message: Option<String>,
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default, deserialize_with = "string_or_number")]
    code: Option<String>,
}

/// Vendors disagree on the type of an error's `code`: OpenAI sends a string
/// (`"invalid_api_key"`), NVIDIA a number (`503`). Both read as text. Read
/// strictly, a numeric code made the whole error frame unparseable, and an
/// overloaded provider was reported as a non-retryable protocol error.
fn string_or_number<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    use serde::Deserialize;
    Ok(match Option::<Value>::deserialize(d)? {
        Some(Value::String(s)) => Some(s),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    })
}

/// A completed (non-streaming) response: `message` where a chunk has `delta`.
#[derive(Debug, Default, serde::Deserialize)]
struct WireCompletion {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    choices: Vec<WireMessageChoice>,
    #[serde(default)]
    usage: Option<WireUsage>,
    #[serde(default)]
    error: Option<WireError>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct WireMessageChoice {
    #[serde(default)]
    message: Option<WireMessage>,
    #[serde(default)]
    finish_reason: Option<Value>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct WireMessage {
    #[serde(default)]
    content: Option<Value>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<WireCompletedToolCall>>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct WireCompletedToolCall {
    #[serde(default)]
    id: Option<String>,
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    function: Option<WireFunctionDelta>,
}

impl From<WireCompletedToolCall> for super::ToolCall {
    fn from(c: WireCompletedToolCall) -> Self {
        let function = c.function.unwrap_or_default();
        Self {
            id: c.id.unwrap_or_default(),
            kind: c.kind.unwrap_or_else(|| "function".to_string()),
            function: super::FunctionCall {
                name: function.name.unwrap_or_default(),
                arguments: function.arguments.unwrap_or_default(),
            },
        }
    }
}

impl From<WireUsage> for Usage {
    fn from(w: WireUsage) -> Self {
        let reasoning_tokens = w.completion_tokens_details.and_then(|d| d.reasoning_tokens);
        let cached_prompt_tokens = w.prompt_tokens_details.and_then(|d| d.cached_tokens);
        let total = if w.total_tokens > 0 {
            w.total_tokens
        } else {
            w.prompt_tokens.saturating_add(w.completion_tokens)
        };
        Usage {
            prompt_tokens: w.prompt_tokens,
            completion_tokens: w.completion_tokens,
            total_tokens: total,
            reasoning_tokens,
            cached_prompt_tokens,
        }
    }
}

/// An in-band `{"error": ...}` object inside a 200: the request is dead, not
/// the connection. Shared by the stream and completion paths so the decision
/// lives in one place.
///
/// The HTTP status was 200 and the failure arrived in-band, so there is no
/// signal about whether it was transient. Optimistic: a caller that retries
/// gets a fresh attempt, and one that treats it as fatal would abandon a
/// request that may have been recoverable. The provider's own `code` is the
/// only hint, and vendors use it inconsistently.
fn in_band_error(err: WireError) -> ProviderError {
    let message = err
        .message
        .or(err.code)
        .or(err.kind)
        .unwrap_or_else(|| "upstream reported an unspecified error".to_string());
    ProviderError::Upstream {
        status: 502,
        body: message,
        retryable: true,
    }
}

/// Translate a completed JSON answer into a [`Completion`].
///
/// Lenient like the stream path: the first choice wins, absent or unreadable
/// content becomes an empty string (a tool-call-only answer has none), and a
/// missing `finish_reason` stays `None`.
fn parse_completion(value: &Value, quirks: QuirkFlags) -> Result<Completion, ProviderError> {
    let mut wire: WireCompletion = serde_json::from_value(value.clone()).map_err(|err| {
        ProviderError::Protocol(format!("{}: {err}", super::truncate(&value.to_string(), 160)))
    })?;
    if let Some(err) = wire.error.take() {
        return Err(in_band_error(err));
    }

    let mut completion = Completion {
        upstream_id: wire.id.take(),
        upstream_model: wire.model.take(),
        usage: wire.usage.take().map(Usage::from),
        ..Default::default()
    };
    if let Some(choice) = wire.choices.into_iter().next() {
        completion.finish_reason = choice.finish_reason.as_ref().and_then(finish_reason_name);
        if let Some(message) = choice.message {
            completion.content = message
                .content
                .as_ref()
                .and_then(text_from_value)
                .unwrap_or_default();
            completion.reasoning = quirks
                .reasoning_fields
                .iter()
                .find_map(|field| match *field {
                    "reasoning_content" => message.reasoning_content.clone(),
                    "reasoning" => message.reasoning.clone(),
                    _ => None,
                })
                .filter(|r| !r.is_empty());
            completion.tool_calls = message
                .tool_calls
                .unwrap_or_default()
                .into_iter()
                .map(super::ToolCall::from)
                .collect();
        }
    }
    Ok(completion)
}

// ---------------------------------------------------------------------------
// Chunk -> event translation
// ---------------------------------------------------------------------------

struct DecodeState {
    data: DataStream,
    quirks: QuirkFlags,
    pending: VecDeque<Result<StreamEvent, ProviderError>>,
    started: bool,
    eof: bool,
    finish_reason: Option<String>,
    usage: Option<Usage>,
    finish_emitted: bool,
    /// Set once an error has been surfaced, so we stop pulling.
    poisoned: bool,
    frames: u64,
}

fn decode(data: DataStream, quirks: QuirkFlags) -> ProviderStream {
    let state = DecodeState {
        data,
        quirks,
        pending: VecDeque::with_capacity(4),
        started: false,
        eof: false,
        finish_reason: None,
        usage: None,
        finish_emitted: false,
        poisoned: false,
        frames: 0,
    };
    Box::pin(futures_util::stream::unfold(state, |mut st| async move {
        let quirks = st.quirks;
        loop {
            if let Some(item) = st.pending.pop_front() {
                return Some((item, st));
            }
            if st.poisoned {
                return None;
            }
            if st.eof {
                if st.finish_emitted {
                    return None;
                }
                st.finish_emitted = true;
                let event = StreamEvent::Finished {
                    finish_reason: st.finish_reason.take(),
                    usage: st.usage.take(),
                };
                st.pending.push_back(Ok(event));
                continue;
            }

            let frame = match st.data.next().await {
                None => {
                    st.eof = true;
                    continue;
                }
                Some(Err(err)) => {
                    st.poisoned = true;
                    st.pending.push_back(Err(err));
                    continue;
                }
                Some(Ok(payload)) => payload,
            };

            let trimmed = frame.trim();
            if trimmed.is_empty() {
                continue;
            }
            // SSE terminator. Some servers close right after it, some don't.
            if trimmed == "[DONE]" {
                st.eof = true;
                continue;
            }

            st.frames += 1;
            let chunk: WireChunk = match serde_json::from_str(trimmed) {
                Ok(c) => c,
                Err(err) => {
                    st.poisoned = true;
                    st.pending.push_back(Err(ProviderError::Protocol(format!(
                        "{}: {err}",
                        super::truncate(trimmed, 160)
                    ))));
                    continue;
                }
            };

            let events = translate(&chunk, &quirks, &mut st);
            st.pending.extend(events);
        }
    }))
}

fn translate(
    chunk: &WireChunk,
    quirks: &QuirkFlags,
    st: &mut DecodeState,
) -> Vec<Result<StreamEvent, ProviderError>> {
    // An in-band error object means the request is dead, not the connection.
    if let Some(err) = &chunk.error {
        st.poisoned = true;
        return vec![Err(in_band_error(err.clone()))];
    }

    let mut out = Vec::with_capacity(2);

    if !st.started && (chunk.id.is_some() || !chunk.choices.is_empty()) {
        st.started = true;
        out.push(Ok(StreamEvent::Started {
            upstream_id: chunk.id.clone(),
            upstream_model: chunk.model.clone(),
        }));
    }

    for choice in &chunk.choices {
        if let Some(reason) = &choice.finish_reason
            && !reason.is_null()
        {
            st.finish_reason = finish_reason_name(reason);
        }
        let Some(delta) = &choice.delta else { continue };

        let reasoning = quirks
            .reasoning_fields
            .iter()
            .find_map(|field| match *field {
                "reasoning_content" => delta.reasoning_content.clone(),
                "reasoning" => delta.reasoning.clone(),
                _ => None,
            })
            .filter(|r| !r.is_empty());

        let content = delta
            .content
            .as_ref()
            .and_then(text_from_value)
            .filter(|c| !c.is_empty());

        if let Some(tool_calls) = &delta.tool_calls {
            for (position, tc) in tool_calls.iter().enumerate() {
                let function = tc.function.as_ref();
                let arguments = tc
                    .arguments
                    .clone()
                    .or_else(|| function.and_then(|f| f.arguments.clone()));
                let name = tc.name.clone().or_else(|| function.and_then(|f| f.name.clone()));
                if arguments.is_none() && name.is_none() && tc.id.is_none() {
                    continue;
                }
                out.push(Ok(StreamEvent::ToolCallDelta {
                    index: tc.index.unwrap_or(position as u32),
                    id: tc.id.clone(),
                    name,
                    arguments,
                }));
            }
        }

        if content.is_some() || reasoning.is_some() {
            out.push(Ok(StreamEvent::Delta { content, reasoning }));
        }
    }

    if let Some(usage) = chunk.usage.clone() {
        st.usage = Some(usage.into());
    }

    out
}

fn finish_reason_name(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// `content` is usually a string but OpenRouter can return an array of parts,
/// and every provider sends `null` for non-text frames.
fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Array(items) => {
            let mut out = String::new();
            for item in items {
                if let Some(s) = item.as_str() {
                    out.push_str(s);
                    continue;
                }
                if let Some(s) = item.get("text").and_then(Value::as_str) {
                    out.push_str(s);
                }
            }
            (!out.is_empty()).then_some(out)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::{HeaderList, ResolvedParams};
    use std::sync::Arc;
    use uuid::Uuid;

    /// `translate` returns `Result<StreamEvent, ProviderError>` items. Compare
    /// the success shape without needing `ProviderError: PartialEq`.
    fn assert_event(
        actual: &Result<StreamEvent, ProviderError>,
        expected: StreamEvent,
    ) {
        match actual {
            Ok(event) => assert_eq!(*event, expected),
            Err(err) => panic!("expected {expected:?}, got error {err:?}"),
        }
    }

    fn quirks() -> QuirkFlags {
        QuirkFlags::default()
    }

    fn st() -> DecodeState {
        DecodeState {
            data: Box::pin(futures_util::stream::empty::<Result<String, ProviderError>>()),
            quirks: quirks(),
            pending: VecDeque::new(),
            started: false,
            eof: false,
            finish_reason: None,
            usage: None,
            finish_emitted: false,
            poisoned: false,
            frames: 0,
        }
    }

    #[test]
    fn content_delta_maps_to_stream_event() {
        let chunk: WireChunk =
            serde_json::from_str(r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"content":"Hel"}}]}"#).unwrap();
        let mut s = st();
        let events = translate(&chunk, &quirks(), &mut s);
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Ok(StreamEvent::Started { .. })));
        assert_event(
            &events[1],
            StreamEvent::Delta { content: Some("Hel".into()), reasoning: None },
        );
    }

    #[test]
    fn null_content_is_ignored() {
        let chunk: WireChunk = serde_json::from_str(
            r#"{"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","content":null}}]}"#,
        )
        .unwrap();
        let mut s = st();
        let events = translate(&chunk, &quirks(), &mut s);
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Ok(StreamEvent::Started { .. })));
    }

    #[test]
    fn reasoning_is_kept_out_of_content() {
        let chunk: WireChunk = serde_json::from_str(
            r#"{"id":"c1","choices":[{"index":0,"delta":{"reasoning_content":"think"}}]}"#,
        )
        .unwrap();
        let mut s = st();
        let events = translate(&chunk, &quirks(), &mut s);
        let last = events.last().unwrap();
        assert_event(
            last,
            StreamEvent::Delta { content: None, reasoning: Some("think".into()) },
        );
    }

    #[test]
    fn in_band_error_object_surfaces_as_provider_error() {
        let chunk: WireChunk =
            serde_json::from_str(r#"{"error":{"message":"bad key","code":"invalid_api_key"}}"#).unwrap();
        let mut s = st();
        let events = translate(&chunk, &quirks(), &mut s);
        assert_eq!(events.len(), 1);
        match &events[0] {
            Err(ProviderError::Upstream { status, body, .. }) => {
                assert_eq!(*status, 502);
                assert_eq!(body, "bad key");
            }
            other => panic!("expected upstream error, got {other:?}"),
        }
        assert!(s.poisoned);
    }

    #[test]
    fn an_in_band_error_is_reported_as_retryable() {
        let chunk: WireChunk = serde_json::from_str(r#"{"error":{"message":"boom"}}"#).unwrap();
        let mut s = st();
        let events = translate(&chunk, &quirks(), &mut s);
        match &events[0] {
            Err(ProviderError::Upstream { status, retryable, .. }) => {
                assert_eq!(*status, 502);
                assert!(*retryable, "a 200-then-error carries no transience signal");
            }
            other => panic!("expected upstream error, got {other:?}"),
        }
    }

    #[test]
    fn an_error_with_a_numeric_code_is_an_error_not_a_protocol_failure() {
        // NVIDIA, under load, mid-stream: found by the live suite.
        let frame = r#"{"error":{"message":"Service temporarily overloaded","type":"service_unavailable","code":503}}"#;
        let chunk: WireChunk = serde_json::from_str(frame).unwrap();
        let mut s = st();
        match &translate(&chunk, &quirks(), &mut s)[0] {
            Err(ProviderError::Upstream { status, body, retryable }) => {
                assert_eq!((*status, body.as_str(), *retryable), (502, "Service temporarily overloaded", true));
            }
            other => panic!("expected a retryable upstream error, got {other:?}"),
        }
        let completion = parse_completion(&serde_json::from_str(frame).unwrap(), quirks()).unwrap_err();
        assert!(matches!(completion, ProviderError::Upstream { retryable: true, .. }), "{completion:?}");
        let bare: WireChunk = serde_json::from_str(r#"{"error":{"code":503}}"#).unwrap();
        match &translate(&bare, &quirks(), &mut st())[0] {
            Err(ProviderError::Upstream { body, .. }) => assert_eq!(body, "503"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn usage_on_a_choice_less_chunk_is_captured() {
        let chunk: WireChunk = serde_json::from_str(
            r#"{"id":"c1","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4}}"#,
        )
        .unwrap();
        let mut s = st();
        translate(&chunk, &quirks(), &mut s);
        let u = s.usage.expect("usage captured");
        assert_eq!(u.prompt_tokens, 10);
        assert_eq!(u.completion_tokens, 4);
        assert_eq!(u.total_tokens, 14);
    }

    #[test]
    fn array_content_is_flattened() {
        let chunk: WireChunk = serde_json::from_str(
            r#"{"id":"c1","choices":[{"index":0,"delta":{"content":[{"type":"text","text":"ab"},{"type":"text","text":"cd"}]}}]}"#,
        )
        .unwrap();
        let mut s = st();
        let events = translate(&chunk, &quirks(), &mut s);
        assert_event(
            events.last().unwrap(),
            StreamEvent::Delta { content: Some("abcd".into()), reasoning: None },
        );
    }

    async fn collect(frames: &[&str]) -> Vec<Result<StreamEvent, ProviderError>> {
        let payloads: Vec<Result<String, ProviderError>> = frames
            .iter()
            .map(|f| Ok((*f).to_string()))
            .collect();
        let stream = decode(
            Box::pin(futures_util::stream::iter(payloads)),
            quirks(),
        );
        futures_util::StreamExt::collect::<Vec<_>>(stream).await
    }

    #[tokio::test]
    async fn a_terminal_event_is_emitted_at_end_of_stream() {
        // Mirrors the real OpenAI shape: content deltas, then a finish_reason
        // frame, then a separate usage frame, then [DONE].
        let events = collect(&[
            r#"{"id":"c1","choices":[{"index":0,"delta":{"content":"Hi"}}]}"#,
            r#"{"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
            r#"{"id":"c1","choices":[],"usage":{"prompt_tokens":12,"completion_tokens":3}}"#,
            "[DONE]",
        ])
        .await;

        let last = events.last().expect("at least one event");
        match last {
            Ok(StreamEvent::Finished { finish_reason, usage }) => {
                assert_eq!(finish_reason.as_deref(), Some("stop"));
                let u = usage.expect("usage present");
                assert_eq!(u.prompt_tokens, 12);
                assert_eq!(u.completion_tokens, 3);
            }
            other => panic!("expected Finished, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exactly_one_terminal_event_is_emitted() {
        let events = collect(&[
            r#"{"id":"c1","choices":[{"index":0,"delta":{"content":"Hi"}}]}"#,
            r#"{"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
            "[DONE]",
        ])
        .await;
        let terminals = events
            .iter()
            .filter(|e| matches!(e, Ok(StreamEvent::Finished { .. })))
            .count();
        assert_eq!(terminals, 1, "a duplicate finish would double-count usage");
    }

    #[tokio::test]
    async fn a_stream_ending_without_done_still_finishes() {
        // Some proxies close the socket instead of sending [DONE]. The client
        // must still get a terminal event, or it waits forever.
        let events = collect(&[r#"{"id":"c1","choices":[{"index":0,"delta":{"content":"Hi"}}]}"#])
            .await;
        assert!(matches!(events.last(), Some(Ok(StreamEvent::Finished { .. }))));
    }

    #[tokio::test]
    async fn usage_on_a_choice_less_frame_reaches_the_terminal_event() {
        let events = collect(&[
            r#"{"id":"c1","choices":[{"index":0,"delta":{"content":"Hi"}}]}"#,
            r#"{"id":"c1","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":2}}"#,
            "[DONE]",
        ])
        .await;
        match events.last() {
            Some(Ok(StreamEvent::Finished { usage: Some(u), .. })) => {
                assert_eq!(u.prompt_tokens, 7);
                assert_eq!(u.completion_tokens, 2);
            }
            other => panic!("expected Finished with usage, got {other:?}"),
        }
    }

    /// A response whose body arrives as `chunks`, with no declared length.
    fn chunked(chunks: Vec<&'static [u8]>) -> reqwest::Response {
        let stream = futures_util::stream::iter(chunks.into_iter().map(|c| Ok::<_, std::io::Error>(bytes::Bytes::from_static(c))));
        reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(stream)))
    }

    fn byte_stream(chunks: Vec<Vec<u8>>) -> impl Stream<Item = reqwest::Result<bytes::Bytes>> + Send + 'static {
        futures_util::stream::iter(chunks.into_iter().map(|c| Ok(bytes::Bytes::from(c))))
    }

    #[tokio::test]
    async fn an_answer_within_the_cap_is_read_whole() {
        let body = read_capped(chunked(vec![b"{\"a\":", b"1}"]), 64).await.unwrap();
        assert_eq!(body, b"{\"a\":1}");
    }

    #[tokio::test]
    async fn an_answer_declared_over_the_cap_is_refused_before_reading() {
        let resp = reqwest::Response::from(http::Response::new(vec![b'x'; 100]));
        let err = read_capped(resp, 64).await.unwrap_err();
        assert!(matches!(err, ProviderError::Protocol(ref m) if m.contains("exceeded 64 bytes")), "{err:?}");
    }

    #[tokio::test]
    async fn an_answer_that_grows_past_the_cap_is_refused() {
        // No declared length: the cap is enforced as the chunks arrive.
        let err = read_capped(chunked(vec![&[b'x'; 40], &[b'x'; 40]]), 64).await.unwrap_err();
        assert!(matches!(err, ProviderError::Protocol(_)), "{err:?}");
        assert_eq!(err.code(), "upstream_protocol_error");
    }

    #[tokio::test]
    async fn an_error_body_is_read_only_as_far_as_it_is_shown() {
        let snippet = read_snippet(chunked(vec![b"0123456789", b"abcdef"]), 12).await;
        assert_eq!(snippet, "0123456789ab");
    }

    #[tokio::test]
    async fn many_small_events_pass_however_long_the_stream() {
        let event = b"data: {\"x\":1}\n\n".to_vec();
        let total: usize = event.len() * 50;
        let out: Vec<_> = bounded_events(byte_stream(vec![event; 50]), 32).collect().await;
        assert!(out.iter().all(Result::is_ok));
        assert_eq!(out.iter().map(|c| c.as_ref().unwrap().len()).sum::<usize>(), total);
        // CRLF line endings are boundaries too.
        let crlf = b"data: {\"x\":1}\r\n\r\n".to_vec();
        let out: Vec<_> = bounded_events(byte_stream(vec![crlf; 50]), 32).collect().await;
        assert!(out.iter().all(Result::is_ok));
    }

    #[tokio::test]
    async fn an_event_without_a_boundary_past_the_cap_fails_the_stream() {
        let chunks = vec![b"data: ".to_vec(), vec![b'x'; 40], vec![b'x'; 40]];
        let out: Vec<_> = bounded_events(byte_stream(chunks), 64).collect().await;
        assert!(out[0].is_ok() && out[1].is_ok());
        assert!(matches!(out[2], Err(BoundedError::TooLarge(64))), "{:?}", out[2]);
    }

    #[tokio::test]
    async fn an_oversized_sse_event_is_an_upstream_protocol_error() {
        let mut big = b"data: ".to_vec();
        big.extend(vec![b'x'; 200]);
        let resp = http::Response::builder()
            .header("content-type", "text/event-stream")
            .body(reqwest::Body::wrap_stream(byte_stream(vec![b"data: {}\n\n".to_vec(), big])))
            .unwrap();
        let frames: Vec<_> = data_stream(reqwest::Response::from(resp), quirks(), 64).collect().await;
        assert_eq!(frames[0].as_deref().ok(), Some("{}"));
        assert!(
            frames.iter().any(|f| matches!(f, Err(ProviderError::Protocol(m)) if m.contains("event exceeded 64 bytes"))),
            "{frames:?}"
        );
    }

    #[tokio::test]
    async fn json_lines_within_the_cap_pass_and_an_endless_line_fails() {
        let lines: Vec<_> = line_delimited(byte_stream(vec![b"{\"a\":1}\n{\"b\"".to_vec(), b":2}\n".to_vec()]), 32)
            .collect()
            .await;
        assert_eq!(lines.iter().map(|l| l.as_deref().unwrap()).collect::<Vec<_>>(), ["{\"a\":1}", "{\"b\":2}"]);
        let endless: Vec<_> = line_delimited(byte_stream(vec![vec![b'x'; 20], vec![b'x'; 20]]), 32).collect().await;
        assert!(
            matches!(endless.last(), Some(Err(ProviderError::Protocol(m))) if m.contains("line exceeded 32 bytes")),
            "{endless:?}"
        );
    }

    #[test]
    fn body_uses_completion_token_field_for_reasoning_models() {
        let req = ProviderRequest {
            request_id: Uuid::nil(),
            tenant_id: "t".into(),
            key_id: "k".into(),
            upstream_model: "o3-mini".into(),
            endpoint: None,
            messages: Arc::new(vec![]),
            params: ResolvedParams { max_tokens: Some(512), ..Default::default() },
            include_usage: true,
        };
        let spec = OpenAiCompatSpec {
            provider: "test",
            default_endpoint: "http://localhost/v1/chat/completions".into(),
            extra_headers: |_| HeaderList::new(),
            quirks: quirks(),
        };
        let body = build_body(&req, &spec, true);
        assert_eq!(body.get("max_completion_tokens"), Some(&Value::from(512)));
        assert!(!body.contains_key("max_tokens"));
    }

    #[test]
    fn body_never_forwards_reserved_keys() {
        let req = ProviderRequest {
            request_id: Uuid::nil(),
            tenant_id: "t".into(),
            key_id: "k".into(),
            upstream_model: "m".into(),
            endpoint: None,
            messages: Arc::new(vec![]),
            params: ResolvedParams {
                include_usage: true,
                extra: [("stream".to_string(), Value::Bool(false))].into_iter().collect(),
                ..Default::default()
            },
            include_usage: true,
        };
        let spec = OpenAiCompatSpec {
            provider: "test",
            default_endpoint: "http://x/v1/chat/completions".into(),
            extra_headers: |_| HeaderList::new(),
            quirks: quirks(),
        };
        let body = build_body(&req, &spec, true);
        // A caller-supplied `stream: false` must not reach the provider: the
        // gateway decides the transport per endpoint, and honouring it would
        // produce a non-SSE body on an SSE route.
        assert_eq!(body.get("stream"), Some(&Value::Bool(true)));

        // Nor can the caller turn a completion into a stream.
        let mut req = req;
        req.params.extra.insert("stream".into(), Value::Bool(true));
        let body = build_body(&req, &spec, false);
        assert_eq!(body.get("stream"), Some(&Value::Bool(false)));
    }

    #[test]
    fn a_completion_body_carries_no_stream_options() {
        let req = ProviderRequest {
            request_id: Uuid::nil(),
            tenant_id: "t".into(),
            key_id: "k".into(),
            upstream_model: "m".into(),
            endpoint: None,
            messages: Arc::new(vec![]),
            params: ResolvedParams::default(),
            include_usage: true,
        };
        let spec = OpenAiCompatSpec {
            provider: "test",
            default_endpoint: "http://x/v1/chat/completions".into(),
            extra_headers: |_| HeaderList::new(),
            quirks: quirks(),
        };
        assert!(build_body(&req, &spec, true).contains_key("stream_options"));
        assert!(!build_body(&req, &spec, false).contains_key("stream_options"));
    }

    #[test]
    fn a_completion_parses_content_reasoning_and_usage() {
        let value: Value = serde_json::from_str(
            r#"{"id":"c9","model":"m-1","choices":[{"index":0,"finish_reason":"stop",
                "message":{"role":"assistant","content":"Hello","reasoning_content":"think"}}],
                "usage":{"prompt_tokens":10,"completion_tokens":2,
                         "completion_tokens_details":{"reasoning_tokens":1}}}"#,
        )
        .unwrap();
        let c = parse_completion(&value, quirks()).unwrap();
        assert_eq!(c.upstream_id.as_deref(), Some("c9"));
        assert_eq!(c.upstream_model.as_deref(), Some("m-1"));
        assert_eq!(c.content, "Hello");
        assert_eq!(c.reasoning.as_deref(), Some("think"));
        assert_eq!(c.finish_reason.as_deref(), Some("stop"));
        let u = c.usage.unwrap();
        assert_eq!((u.prompt_tokens, u.completion_tokens, u.total_tokens), (10, 2, 12));
        assert_eq!(u.reasoning_tokens, Some(1));
    }

    #[test]
    fn a_tool_call_only_completion_has_empty_content() {
        let value: Value = serde_json::from_str(
            r#"{"choices":[{"finish_reason":"tool_calls","message":{"content":null,
                "tool_calls":[{"id":"call_1","type":"function",
                  "function":{"name":"get_weather","arguments":"{\"city\":\"NYC\"}"}}]}}]}"#,
        )
        .unwrap();
        let c = parse_completion(&value, quirks()).unwrap();
        assert_eq!(c.content, "");
        assert_eq!(c.tool_calls.len(), 1);
        assert_eq!(c.tool_calls[0].id, "call_1");
        assert_eq!(c.tool_calls[0].function.name, "get_weather");
        assert_eq!(c.tool_calls[0].function.arguments, r#"{"city":"NYC"}"#);
        assert!(c.usage.is_none(), "absent usage stays absent for the caller to estimate");
    }

    #[test]
    fn an_in_band_error_in_a_completion_is_a_retryable_upstream_refusal() {
        let value: Value = serde_json::from_str(r#"{"error":{"code":"overloaded"}}"#).unwrap();
        let err = parse_completion(&value, quirks()).unwrap_err();
        match &err {
            ProviderError::Upstream { status, body, retryable } => {
                assert_eq!((*status, body.as_str(), *retryable), (502, "overloaded", true));
            }
            other => panic!("expected upstream error, got {other:?}"),
        }
        assert!(!err.accepted_by_upstream(), "an in-band refusal costs nothing");
    }

    #[test]
    fn a_malformed_completion_is_a_protocol_error_after_acceptance() {
        let value: Value = serde_json::from_str(r#"{"choices":"not-a-list"}"#).unwrap();
        let err = parse_completion(&value, quirks()).unwrap_err();
        assert!(matches!(err, ProviderError::Protocol(_)), "{err:?}");
        assert!(err.accepted_by_upstream());
    }
}
