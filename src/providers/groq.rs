//! Groq adapter.
//!
//! Groq is the low-latency path: LPU inference, OpenAI-compatible
//! `/openai/v1/chat/completions`, bearer auth. It is strict about unknown
//! parameters and rejects `seed` for most hosted models, which the shared
//! engine's quirk flags handle.

use std::sync::Arc;

use super::{
    openai_compat::{OpenAiCompatAdapter, OpenAiCompatSpec, QuirkFlags},
    ChatProvider, HeaderList, ProviderError, ProviderRequest, ProviderStream, resolve_api_key,
    UpstreamTuning,
};

pub const DEFAULT_ENDPOINT: &str = "https://api.groq.com/openai/v1/chat/completions";

pub struct GroqAdapter {
    inner: OpenAiCompatAdapter,
}

impl GroqAdapter {
    /// `Err` when `GROQ_API_KEY` is missing, so the failure is a startup error
    /// rather than a 502 on the first request.
    pub fn from_env() -> Result<Self, ProviderError> {
        let api_key = resolve_api_key("groq")?;
        Self::tuned(Some(api_key), &UpstreamTuning::default())
    }

    /// Build with an explicit transport configuration and credential.
    pub fn tuned(api_key: Option<String>, tuning: &UpstreamTuning) -> Result<Self, ProviderError> {
        let client = super::tuned_client(tuning)?;
        Ok(Self { inner: OpenAiCompatAdapter::new(spec(), api_key, client) })
    }
}

fn spec() -> OpenAiCompatSpec {
    OpenAiCompatSpec {
        provider: "groq",
        default_endpoint: DEFAULT_ENDPOINT.to_string(),
        extra_headers: groq_headers,
        quirks: QuirkFlags {
            reasoning_fields: &["reasoning_content"],
            send_stream_options: true,
            force_completion_token_field: false,
            // Groq 400s on `seed` for most hosted models.
            supports_seed: false,
            accept_json_lines: true,
        },
    }
}

fn groq_headers(_req: &ProviderRequest) -> HeaderList {
    HeaderList::new()
}

#[async_trait::async_trait]
impl ChatProvider for GroqAdapter {
    fn name(&self) -> &'static str {
        "groq"
    }

    fn base_url(&self) -> Option<&str> {
        Some(DEFAULT_ENDPOINT)
    }

    async fn stream_chat(&self, req: ProviderRequest) -> Result<ProviderStream, ProviderError> {
        self.inner.stream_chat(req).await
    }

    async fn stream_chat_raw(&self, req: ProviderRequest) -> Result<super::RawStream, ProviderError> {
        self.inner.stream_chat_raw(req).await
    }

    fn supports_passthrough(&self) -> bool {
        true
    }
}

/// Marker so `Arc<GroqAdapter>` satisfies `ChatProvider` in the pool.
#[allow(dead_code)]
fn _assert_arc_impl(a: Arc<GroqAdapter>) -> Arc<dyn ChatProvider> {
    a
}
