//! Provider adapters.
//!
//! Every provider implements one method, [`ChatProvider::stream_chat`], and
//! returns a normalized [`StreamEvent`] sequence. Quirk handling (SSE framing,
//! reasoning channels, error shapes, token-field names) lives in the adapter,
//! never in the API layer, so adding a vendor is an adapter + a registry entry
//! and no changes to the public contract.

mod groq;
mod nvidia;
mod openai_compat;
mod openrouter;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use futures_core::Stream;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use uuid::Uuid;

pub use groq::GroqAdapter;
pub use nvidia::NvidiaAdapter;
pub use openrouter::OpenRouterAdapter;
pub use openai_compat::{OpenAiCompatSpec, QuirkFlags};

// Re-exported so provider modules build their spec without naming the private module.

/// Normalized token stream. Errors are *items*, not terminal values, so the
/// gateway can report a mid-stream failure to the client and still release
/// every resource on drop.
pub type ProviderStream = Pin<Box<dyn Stream<Item = Result<StreamEvent, ProviderError>> + Send>>;

/// Undecoded upstream bytes, for passthrough relaying. The transport yields
/// chunks at TCP boundaries, which are not SSE event boundaries — a single
/// event can straddle two chunks and one chunk can hold several events.
/// Consumers must either relay verbatim or run a real incremental parser.
pub type RawStream = Pin<Box<dyn Stream<Item = Result<bytes::Bytes, ProviderError>> + Send>>;

/// Upstream models that reject `max_tokens` and require
/// `max_completion_tokens`. Matched as prefixes.
pub const MAX_COMPLETION_TOKENS_MODELS: &[&str] =
    &["o1", "o1-", "o3", "o3-", "o4", "o4-", "gpt-5", "gpt-5-"];

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// OpenAI content is either a bare string or an array of typed parts. Both
/// pass through to the provider byte-for-byte, so images, audio and future part
/// types keep working without a gateway release.
#[derive(Debug, Clone, PartialEq)]
pub enum Content {
    Text(String),
    Parts(Vec<ContentPart>),
}

impl Content {
    pub fn text(&self) -> String {
        match self {
            Content::Text(t) => t.clone(),
            Content::Parts(parts) => parts
                .iter()
                .filter_map(|p| match p {
                    ContentPart::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(""),
        }
    }

    pub fn approx_chars(&self) -> usize {
        match self {
            Content::Text(t) => t.len(),
            Content::Parts(parts) => parts
                .iter()
                .map(|p| match p {
                    ContentPart::Text { text } => text.len(),
                    // An image is a few hundred tokens, not a few dozen bytes;
                    // charge it like so for budget reservation.
                    ContentPart::ImageUrl { .. } => 1_400,
                    _ => 64,
                })
                .sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Content::Text(t) => t.is_empty(),
            Content::Parts(p) => p.is_empty(),
        }
    }
}

impl Serialize for Content {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Content::Text(t) => s.serialize_str(t),
            Content::Parts(p) => p.serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for Content {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        content_from_value(value).map_err(de::Error::custom)
    }
}

pub fn content_from_value(value: Value) -> Result<Content, String> {
    match value {
        Value::Null => Ok(Content::Text(String::new())),
        Value::String(s) => Ok(Content::Text(s)),
        Value::Array(items) => {
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                parts.push(
                    serde_json::from_value(item)
                        .map_err(|e| format!("invalid content part: {e}"))?,
                );
            }
            Ok(Content::Parts(parts))
        }
        other => Err(format!(
            "content must be a string or an array of content parts, got {}",
            json_type_name(&other)
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    Text {
        text: String,
    },
    ImageUrl {
        image_url: ImageUrl,
    },
    InputAudio {
        input_audio: Value,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImageUrl {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChatMessage {
    System { content: Content },
    User { content: Content },
    Assistant {
        content: Option<Content>,
        tool_calls: Vec<ToolCall>,
    },
    Tool {
        content: Content,
        tool_call_id: String,
        name: Option<String>,
    },
}

impl ChatMessage {
    pub fn role(&self) -> &'static str {
        match self {
            ChatMessage::System { .. } => "system",
            ChatMessage::User { .. } => "user",
            ChatMessage::Assistant { .. } => "assistant",
            ChatMessage::Tool { .. } => "tool",
        }
    }

    pub fn content(&self) -> Option<&Content> {
        match self {
            ChatMessage::System { content } | ChatMessage::User { content } => Some(content),
            ChatMessage::Assistant { content, .. } => content.as_ref(),
            ChatMessage::Tool { content, .. } => Some(content),
        }
    }

    pub fn approx_chars(&self) -> usize {
        let content = self.content().map(Content::approx_chars).unwrap_or(0);
        content
            + match self {
                ChatMessage::Tool { tool_call_id, name, .. } => {
                    tool_call_id.len() + name.as_deref().map(str::len).unwrap_or(0)
                }
                ChatMessage::Assistant { tool_calls, .. } => tool_calls
                    .iter()
                    .map(|tc| tc.function.arguments.len() + tc.function.name.len())
                    .sum(),
                _ => 0,
            }
    }
}

impl Serialize for ChatMessage {
    /// Emits exactly the OpenAI wire shape. Hand-written because the variant
    /// name is the wire `role` and internally-tagged enums are not safe to
    /// combine with our untagged `Content`.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("role", self.role())?;
        match self {
            ChatMessage::Assistant {
                content,
                tool_calls,
            } => {
                // The wire allows an explicit null content on assistant turns
                // that only carry tool calls; some providers require it.
                match content {
                    Some(c) => m.serialize_entry("content", c)?,
                    None => m.serialize_entry("content", &Value::Null)?,
                }
                if !tool_calls.is_empty() {
                    m.serialize_entry("tool_calls", tool_calls)?;
                }
            }
            ChatMessage::Tool {
                content,
                tool_call_id,
                name,
            } => {
                m.serialize_entry("content", content)?;
                m.serialize_entry("tool_call_id", tool_call_id)?;
                if let Some(name) = name {
                    m.serialize_entry("name", name)?;
                }
            }
            ChatMessage::System { content } | ChatMessage::User { content } => {
                m.serialize_entry("content", content)?;
            }
        }
        m.end()
    }
}

const SUPPORTED_ROLES: &str = "system, user, assistant, tool";

impl<'de> Deserialize<'de> for ChatMessage {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            role: String,
            #[serde(default)]
            content: Option<Value>,
            #[serde(default)]
            name: Option<String>,
            #[serde(default)]
            tool_call_id: Option<String>,
            #[serde(default)]
            tool_calls: Option<Vec<ToolCall>>,
        }
        let raw = Raw::deserialize(d)?;
        let content = match raw.content {
            None | Some(Value::Null) => None,
            Some(v) => Some(content_from_value(v).map_err(de::Error::custom)?),
        };
        // `system`, `user` and `tool` must carry content; `assistant` may omit
        // it when it is making a tool call. Typed against `D::Error` rather
        // than `serde::de::Error` so the error type matches the deserializer.
        use serde::de::Error as DeError;
        fn required<E, T>(role: &str, c: Option<T>) -> Result<T, E>
        where
            E: serde::de::Error,
        {
            c.ok_or_else(|| DeError::custom(format!("`{role}` message requires content")))
        }
        match raw.role.as_str() {
            "system" => Ok(ChatMessage::System { content: required(&raw.role, content)? }),
            "user" => Ok(ChatMessage::User { content: required(&raw.role, content)? }),
            "assistant" => Ok(ChatMessage::Assistant {
                content,
                tool_calls: raw.tool_calls.unwrap_or_default(),
            }),
            "tool" => Ok(ChatMessage::Tool {
                content: required(&raw.role, content)?,
                tool_call_id: raw.tool_call_id.ok_or_else(|| {
                    de::Error::custom("`tool` message requires `tool_call_id`")
                })?,
                name: raw.name,
            }),
            other => Err(de::Error::custom(format!(
                "unsupported message role `{other}` (expected one of {SUPPORTED_ROLES})"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: FunctionCall,
}

impl Default for ToolCall {
    fn default() -> Self {
        Self {
            id: String::new(),
            kind: "function".to_string(),
            function: FunctionCall::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    /// JSON-encoded arguments.
    pub arguments: String,
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// Effective generation parameters after merging registry defaults with the
/// request. `extra` carries anything not named here straight through to the
/// provider, which is how a new vendor parameter reaches production without a
/// gateway change.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResolvedParams {
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub max_tokens: Option<u32>,
    pub stop: Option<Vec<String>>,
    pub seed: Option<i64>,
    pub presence_penalty: Option<f64>,
    pub frequency_penalty: Option<f64>,
    pub include_usage: bool,
    pub extra: Map<String, Value>,
}

impl ResolvedParams {
    pub fn set_from_json(&mut self, key: &str, value: &Value) {
        match key {
            "temperature" => self.temperature = value.as_f64(),
            "top_p" => self.top_p = value.as_f64(),
            "max_tokens" | "max_completion_tokens" => self.max_tokens = value.as_u64().map(|v| v as u32),
            "stop" => self.stop = stop_from_value(value),
            "seed" => self.seed = value.as_i64(),
            "presence_penalty" => self.presence_penalty = value.as_f64(),
            "frequency_penalty" => self.frequency_penalty = value.as_f64(),
            "include_usage" => self.include_usage = value.as_bool().unwrap_or(false),
            _ => {
                self.extra.insert(key.to_string(), value.clone());
            }
        }
    }

    pub fn extra_keys(&self) -> Vec<&str> {
        let mut keys: Vec<&str> = self.extra.keys().map(String::as_str).collect();
        keys.sort_unstable();
        keys
    }
}

/// OpenAI accepts `stop` as a bare string or an array; we always send an array
/// and accept either inbound.
fn stop_from_value(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::String(s) => Some(vec![s.clone()]),
        Value::Array(items) => {
            let out: Vec<String> = items
                .iter()
                .filter_map(|i| i.as_str().map(str::to_string))
                .collect();
            (!out.is_empty()).then_some(out)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Stream events
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum StreamEvent {
    /// First event of a stream. Carries upstream correlation ids for support.
    Started {
        upstream_id: Option<String>,
        upstream_model: Option<String>,
    },
    Delta {
        content: Option<String>,
        /// Chain-of-thought / reasoning channel, kept strictly separate from
        /// `content` so callers cannot accidentally surface it.
        reasoning: Option<String>,
    },
    ToolCallDelta {
        index: u32,
        id: Option<String>,
        name: Option<String>,
        arguments: Option<String>,
    },
    Finished {
        finish_reason: Option<String>,
        usage: Option<Usage>,
    },
}

impl StreamEvent {
    pub fn is_terminal(&self) -> bool {
        matches!(self, StreamEvent::Finished { .. })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default, rename = "prompt_tokens")]
    pub prompt_tokens: u32,
    #[serde(default, rename = "completion_tokens")]
    pub completion_tokens: u32,
    #[serde(default, rename = "total_tokens")]
    pub total_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_prompt_tokens: Option<u32>,
}

impl Usage {
    pub fn is_empty(&self) -> bool {
        self.prompt_tokens == 0 && self.completion_tokens == 0
    }
}

// ---------------------------------------------------------------------------
// Request / errors / trait
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ProviderRequest {
    pub request_id: Uuid,
    pub tenant_id: Arc<str>,
    pub key_id: Arc<str>,
    /// Model name as the provider knows it (registry prefix stripped).
    pub upstream_model: String,
    /// Per-model endpoint override from the registry.
    pub endpoint: Option<String>,
    pub messages: Arc<Vec<ChatMessage>>,
    pub params: ResolvedParams,
    /// Ask the provider to include usage in the terminal chunk.
    pub include_usage: bool,
}

impl ProviderRequest {
    /// The URL to POST to: the per-model override from the registry, else the
    /// adapter's default. `reqwest::Url` parsing here rather than at send time,
    /// so a malformed registry entry is caught as a pre-stream error with a
    /// useful message instead of "relative URL without a base".
    pub fn url(&self, spec: &OpenAiCompatSpec) -> Result<reqwest::Url, ProviderError> {
        let raw = self
            .endpoint
            .as_deref()
            .unwrap_or(spec.default_endpoint.as_str());
        reqwest::Url::parse(raw).map_err(|e| {
            ProviderError::NotConfigured(format!(
                "upstream endpoint `{raw}` is not a valid absolute URL: {e}"
            ))
        })
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ProviderError {
    #[error("provider is not configured: {0}")]
    NotConfigured(String),
    #[error("failed to reach {provider}: {message}")]
    Connect { provider: String, message: String },
    #[error("upstream returned HTTP {status}{}", body_suffix(.body))]
    Upstream {
        status: u16,
        body: String,
        retryable: bool,
    },
    #[error("upstream stream failed: {0}")]
    Stream(String),
    #[error("upstream sent a frame the adapter could not parse: {0}")]
    Protocol(String),
    #[error("no upstream bytes within {0:?}")]
    IdleTimeout(Duration),
}

fn body_suffix(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        String::new()
    } else {
        format!(": {}", truncate(trimmed, 400))
    }
}

pub fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars).collect();
    out.push('…');
    out
}

impl ProviderError {
    /// Stable machine-readable code, surfaced in the SSE `error` frame so
    /// clients can branch without string matching.
    pub fn code(&self) -> &'static str {
        match self {
            ProviderError::NotConfigured(_) => "provider_not_configured",
            ProviderError::Connect { .. } => "upstream_connect_failed",
            ProviderError::Upstream { status, .. } => match *status {
                429 => "upstream_rate_limited",
                400..=499 => "upstream_rejected_request",
                _ => "upstream_error",
            },
            ProviderError::Stream(_) => "upstream_stream_failed",
            ProviderError::Protocol(_) => "upstream_protocol_error",
            ProviderError::IdleTimeout(_) => "upstream_idle_timeout",
        }
    }

    pub fn retryable(&self) -> bool {
        match self {
            ProviderError::Connect { .. } | ProviderError::Stream(_) | ProviderError::IdleTimeout(_) => true,
            ProviderError::Upstream { status, .. } => *status == 429 || *status >= 500,
            ProviderError::NotConfigured(_) | ProviderError::Protocol(_) => false,
        }
    }

    pub fn status_hint(&self) -> u16 {
        match self {
            ProviderError::Upstream { status, .. } => *status,
            ProviderError::NotConfigured(_) => 502,
            ProviderError::Connect { .. } | ProviderError::Stream(_) | ProviderError::Protocol(_) => 502,
            ProviderError::IdleTimeout(_) => 504,
        }
    }
}

#[async_trait::async_trait]
pub trait ChatProvider: Send + Sync + 'static {
    /// Registry key this adapter serves (`groq`, `openrouter`, `nvidia`).
    fn name(&self) -> &'static str;

    /// Open the upstream connection and return the normalized token stream.
    /// Errors returned here are *pre-stream* (auth, DNS, 4xx/5xx); everything
    /// after is delivered as `Err` items on the stream.
    async fn stream_chat(&self, req: ProviderRequest) -> Result<ProviderStream, ProviderError>;

    /// Open the upstream connection and return the response body undecoded.
    ///
    /// There is no meaningful default: producing raw bytes from a normalized
    /// stream would require re-encoding, which is the cost passthrough exists to
    /// avoid. Providers that can hand back the transport directly override this.
    async fn stream_chat_raw(&self, _req: ProviderRequest) -> Result<RawStream, ProviderError> {
        Err(ProviderError::Protocol(format!(
            "provider `{}` does not support raw passthrough",
            self.name()
        )))
    }

    /// Whether [`ChatProvider::stream_chat_raw`] is implemented.
    fn supports_passthrough(&self) -> bool {
        false
    }

    /// Cheap readiness probe target, if the provider has one.
    fn base_url(&self) -> Option<&str> {
        None
    }
}

/// Shared HTTP client. One pool per adapter so a slow Groq connection cannot
/// starve NVIDIA headroom.
pub fn build_http_client(
    connect_timeout: Duration,
    read_timeout: Duration,
    pool_max_idle_per_host: usize,
    pool_idle_timeout: Duration,
    tcp_keepalive: Duration,
    user_agent: &str,
) -> Result<reqwest::Client, ProviderError> {
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        // Per-read idle timeout so a half-open SSE stream is torn down instead
        // of pinning a socket forever. Not applied to HTTP/2, where hyper
        // rejects the combination; HTTP/2 detects a stalled stream via GOAWAY
        // and RST_STREAM instead.
        .read_timeout(read_timeout)
        .pool_max_idle_per_host(pool_max_idle_per_host)
        .pool_idle_timeout(pool_idle_timeout)
        .tcp_keepalive(tcp_keepalive)
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .user_agent(user_agent)
        .build()
        .map_err(|e| ProviderError::NotConfigured(format!("http client: {e}")))
}

/// Transport tuning, pulled from `config::UpstreamConfig` so a single
/// `reqwest::Client` per provider uses the operator's timeouts.
#[derive(Debug, Clone)]
pub struct UpstreamTuning {
    pub connect_timeout: Duration,
    pub stream_read_timeout: Duration,
    pub pool_max_idle_per_host: usize,
    pub pool_idle_timeout: Duration,
    pub tcp_keepalive: Duration,
    pub user_agent: String,
}

impl Default for UpstreamTuning {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(5_000),
            stream_read_timeout: Duration::from_millis(120_000),
            pool_max_idle_per_host: 64,
            pool_idle_timeout: Duration::from_millis(90_000),
            tcp_keepalive: Duration::from_secs(75),
            user_agent: concat!("llm-gateway/", env!("CARGO_PKG_VERSION")).to_string(),
        }
    }
}

impl From<&crate::config::UpstreamConfig> for UpstreamTuning {
    fn from(c: &crate::config::UpstreamConfig) -> Self {
        Self {
            connect_timeout: Duration::from_millis(c.connect_timeout_ms),
            stream_read_timeout: Duration::from_millis(c.stream_read_timeout_ms),
            pool_max_idle_per_host: c.pool_max_idle_per_host,
            pool_idle_timeout: Duration::from_millis(c.pool_idle_timeout_ms),
            tcp_keepalive: Duration::from_secs(c.tcp_keepalive_secs),
            user_agent: c.user_agent.clone(),
        }
    }
}

/// Build the shared upstream client.
///
/// `reqwest`'s `read_timeout` maps to hyper's per-read idle timeout, which is
/// not supported over HTTP/2 — enabling both makes client construction fail.
/// HTTP/2 already surfaces a stalled stream as a reset, and HTTP/1.1 gets the
/// read timeout, so tuning is per-protocol rather than conflicting.
pub fn tuned_client(tuning: &UpstreamTuning) -> Result<reqwest::Client, ProviderError> {
    build_http_client(
        tuning.connect_timeout,
        tuning.stream_read_timeout,
        tuning.pool_max_idle_per_host,
        tuning.pool_idle_timeout,
        tuning.tcp_keepalive,
        &tuning.user_agent,
    )
}

/// Build a spec and client from the ambient environment, for embedders that
/// construct adapters without gateway `Settings`.
/// Provider key -> secret, resolved from the environment at startup.
pub fn resolve_api_key(provider: &str) -> Result<String, ProviderError> {
    let env_name = match provider {
        "groq" => "GROQ_API_KEY",
        "openrouter" => "OPENROUTER_API_KEY",
        "nvidia" => "NVIDIA_API_KEY",
        other => {
            return Err(ProviderError::NotConfigured(format!(
                "unknown provider `{other}`"
            )));
        }
    };
    env::var(env_name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.trim().to_string())
        .ok_or_else(|| ProviderError::NotConfigured(format!("{env_name} is not set")))
}

pub fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// A provider that has no credential configured still has to exist so the
/// registry can be validated at boot; calls to it fail fast with a clear error.
pub fn credential_env_var(provider: &str) -> Option<&'static str> {
    match provider {
        "groq" => Some("GROQ_API_KEY"),
        "openrouter" => Some("OPENROUTER_API_KEY"),
        "nvidia" => Some("NVIDIA_API_KEY"),
        _ => None,
    }
}

pub type HeaderList = BTreeMap<String, String>;

/// Immutable, cheaply-cloneable set of live adapters keyed by provider name.
/// Read on every request to find the adapter for a resolved model, so it holds
/// `Arc` handles and never rebuilds.
#[derive(Clone, Default)]
pub struct ProviderPool {
    by_name: HashMap<String, Arc<dyn ChatProvider>>,
}

impl ProviderPool {
    pub fn new(adapters: Vec<Arc<dyn ChatProvider>>) -> Self {
        let mut by_name = HashMap::with_capacity(adapters.len());
        for adapter in adapters {
            by_name.insert(adapter.name().to_ascii_lowercase(), adapter);
        }
        Self { by_name }
    }

    pub fn get(&self, provider: &str) -> Option<&Arc<dyn ChatProvider>> {
        self.by_name.get(&provider.to_ascii_lowercase())
    }

    pub fn names(&self) -> HashSet<String> {
        self.by_name.keys().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_name.len()
    }
}
