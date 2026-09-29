//! NVIDIA adapter (NIM).
//!
//! Two deployments share one adapter:
//!   * **Cloud** — `https://integrate.api.nvidia.com/v1/chat/completions`,
//!     bearer `NVIDIA_API_KEY`, which bills like any other vendor.
//!   * **Local** — a self-hosted NIM container reached through the per-model
//!     `endpoint:` override in `models.yaml`. Those often run without auth, and
//!     some builds emit newline-delimited JSON instead of SSE, which
//!     [`QuirkFlags::accept_json_lines`] covers.
//!
//! Nemotron reasoning tokens arrive on `reasoning_content`; they are surfaced
//! as a separate SSE event so a caller cannot accidentally render chain of
//! thought to a user.

use super::{
    openai_compat::{OpenAiCompatAdapter, OpenAiCompatSpec, QuirkFlags},
    ChatProvider, HeaderList, ProviderError, ProviderRequest, ProviderStream, resolve_api_key,
    UpstreamTuning,
};

pub const CLOUD_ENDPOINT: &str = "https://integrate.api.nvidia.com/v1/chat/completions";

pub struct NvidiaAdapter {
    inner: OpenAiCompatAdapter,
}

impl NvidiaAdapter {
    /// Cloud mode: requires `NVIDIA_API_KEY`.
    pub fn from_env() -> Result<Self, ProviderError> {
        let api_key = resolve_api_key("nvidia")?;
        Self::tuned(Some(api_key), &UpstreamTuning::default())
    }

    pub fn with_key(api_key: String) -> Result<Self, ProviderError> {
        Self::tuned(Some(api_key), &UpstreamTuning::default())
    }

    /// Local mode: no credential. Per-model `endpoint:` in the registry points
    /// at the NIM container. A `None` key emits no `Authorization` header at
    /// all, rather than an empty bearer which some NIM builds reject.
    pub fn local() -> Result<Self, ProviderError> {
        Self::tuned(None, &UpstreamTuning::default())
    }

    pub fn tuned(api_key: Option<String>, tuning: &UpstreamTuning) -> Result<Self, ProviderError> {
        let client = super::tuned_client(tuning)?;
        Ok(Self { inner: OpenAiCompatAdapter::new(spec(), api_key, client) })
    }
}

fn spec() -> OpenAiCompatSpec {
    OpenAiCompatSpec {
        provider: "nvidia",
        default_endpoint: CLOUD_ENDPOINT.to_string(),
        extra_headers: nvidia_headers,
        quirks: QuirkFlags {
            reasoning_fields: &["reasoning_content", "reasoning"],
            send_stream_options: true,
            force_completion_token_field: false,
            supports_seed: false,
            // Self-hosted NIM builds are inconsistent about content-type.
            accept_json_lines: true,
        },
    }
}

fn nvidia_headers(_req: &ProviderRequest) -> HeaderList {
    HeaderList::new()
}

#[async_trait::async_trait]
impl ChatProvider for NvidiaAdapter {
    fn name(&self) -> &'static str {
        "nvidia"
    }

    fn base_url(&self) -> Option<&str> {
        Some(CLOUD_ENDPOINT)
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
