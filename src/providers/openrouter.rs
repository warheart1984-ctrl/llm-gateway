//! OpenRouter adapter.
//!
//! Aggregates upstream vendors behind one OpenAI-shaped endpoint. Three
//! behaviours matter here:
//!   * `HTTP-Referer` / `X-Title` attribution headers (OpenRouter uses them for
//!     per-app leaderboards and will 400 without them on some accounts).
//!   * `usage` is always present on the terminal chunk, so cost accounting is
//!     reliable here in a way it is not on bare OpenAI.
//!   * reasoning models arrive under `reasoning`, not `reasoning_content`.

use super::{
    openai_compat::{OpenAiCompatAdapter, OpenAiCompatSpec, QuirkFlags},
    ChatProvider, HeaderList, ProviderError, ProviderRequest, ProviderStream, resolve_api_key,
    UpstreamTuning,
};

pub const DEFAULT_ENDPOINT: &str = "https://openrouter.ai/api/v1/chat/completions";

pub struct OpenRouterAdapter {
    inner: OpenAiCompatAdapter,
}

impl OpenRouterAdapter {
    pub fn from_env() -> Result<Self, ProviderError> {
        let api_key = resolve_api_key("openrouter")?;
        Self::tuned(Some(api_key), &UpstreamTuning::default())
    }

    pub fn with_key(api_key: String) -> Result<Self, ProviderError> {
        Self::tuned(Some(api_key), &UpstreamTuning::default())
    }

    /// Build with an explicit transport configuration and credential.
    pub fn tuned(api_key: Option<String>, tuning: &UpstreamTuning) -> Result<Self, ProviderError> {
        let client = super::tuned_client(tuning)?;
        Ok(Self {
            inner: OpenAiCompatAdapter::new(
                OpenAiCompatSpec {
                    provider: "openrouter",
                    default_endpoint: DEFAULT_ENDPOINT.to_string(),
                    extra_headers: openrouter_headers,
                    quirks: QuirkFlags {
                        // OpenRouter normalises reasoning onto `reasoning`, but
                        // passthrough vendors behind it may still send
                        // `reasoning_content`.
                        reasoning_fields: &["reasoning", "reasoning_content"],
                        send_stream_options: true,
                        force_completion_token_field: false,
                        supports_seed: true,
                        accept_json_lines: true,
                    },
                },
                api_key,
                client,
            ),
        })
    }
}

fn openrouter_headers(req: &ProviderRequest) -> HeaderList {
    let mut headers = HeaderList::new();
    // OpenRouter attributes requests to an app; `X-Fallback-Tenant` lets a
    // multi-tenant deployment keep its own tenancy legible upstream.
    if let Ok(referer) = std::env::var("OPENROUTER_REFERER") {
        headers.insert("HTTP-Referer".to_string(), referer);
    }
    if let Ok(title) = std::env::var("OPENROUTER_TITLE") {
        headers.insert("X-Title".to_string(), title);
    }
    headers.insert("X-Fallback-Tenant".to_string(), req.tenant_id.to_string());
    headers
}

#[async_trait::async_trait]
impl ChatProvider for OpenRouterAdapter {
    fn name(&self) -> &'static str {
        "openrouter"
    }

    fn base_url(&self) -> Option<&str> {
        Some(DEFAULT_ENDPOINT)
    }

    async fn stream_chat(&self, req: ProviderRequest) -> Result<ProviderStream, ProviderError> {
        self.inner.stream_chat(req).await
    }

    /// OpenRouter injects SSE comment keepalives, which raw relaying preserves
    /// exactly as the vendor intends.
    async fn stream_chat_raw(&self, req: ProviderRequest) -> Result<super::RawStream, ProviderError> {
        self.inner.stream_chat_raw(req).await
    }

    fn supports_passthrough(&self) -> bool {
        true
    }
}
