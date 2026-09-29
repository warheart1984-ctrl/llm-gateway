//! Structured logging and the request-summary span.
//!
//! Rule: **no per-token logging.** A 4k-token completion would otherwise emit
//! 4k log lines and turn the logger into the bottleneck. The hot path writes
//! nothing; a single `info!` fires when the stream ends, with the numbers a
//! human actually needs to debug it.
//!
//! Credentials never reach a log line: see [`redact_header`].

use std::time::Instant;

use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::config::{LogFormat, TelemetryConfig};

/// Traced per-request span. The handler creates it, drops it in the stream so
/// the span covers the whole streaming lifetime, and reads it back to build the
/// summary log line.
#[derive(Debug, Clone)]
pub struct RequestSpan {
    pub request_id: String,
    pub tenant_id: String,
    pub key_id: String,
    pub scheme: String,
    pub model: String,
    pub provider: String,
    pub upstream_model: String,
    pub started: Instant,
    pub ttft: Option<Instant>,
}

impl RequestSpan {
    pub fn new(request_id: String) -> Self {
        Self {
            request_id,
            tenant_id: String::new(),
            key_id: String::new(),
            scheme: String::new(),
            model: String::new(),
            provider: String::new(),
            upstream_model: String::new(),
            started: Instant::now(),
            ttft: None,
        }
    }

    pub fn mark_first_token(&mut self) {
        self.ttft.get_or_insert_with(Instant::now);
    }

    pub fn ttft_ms(&self) -> Option<u128> {
        self.ttft.map(|t| t.duration_since(self.started).as_millis())
    }

    pub fn elapsed_ms(&self) -> u128 {
        self.started.elapsed().as_millis()
    }
}

/// Values a stream reports back at completion. Filled in once, not per token.
#[derive(Debug, Clone, Default)]
pub struct StreamSummary {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub reasoning_tokens: u32,
    pub cached_prompt_tokens: u32,
    pub cost_micro_usd: u64,
    pub finish_reason: Option<String>,
    /// Upstream correlation ids, captured from the first provider frame.
    pub upstream_id: Option<String>,
    pub upstream_model: Option<String>,
    /// Normalized events forwarded to the client.
    pub events: u64,
    pub bytes_out: u64,
    pub aborted_by_client: bool,
    pub error_code: Option<&'static str>,
    pub error_retryable: bool,
}

pub fn init(cfg: &TelemetryConfig) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cfg.log_filter));

    let registry = tracing_subscriber::registry().with(filter);
    let installed = match cfg.log_format {
        LogFormat::Json => registry
            .with(
                fmt::layer()
                    .json()
                    .flatten_event(true)
                    .with_current_span(true)
                    .with_span_list(false)
                    .with_target(true)
                    .with_file(false)
                    .with_line_number(false),
            )
            .try_init(),
        LogFormat::Pretty => registry
            .with(
                fmt::layer()
                    .with_ansi(false)
                    .with_target(true)
                    .with_file(false)
                    .with_line_number(false),
            )
            .try_init(),
    };
    if installed.is_err() {
        // A second init in the same process (integration tests) is not fatal.
        eprintln!("tracing subscriber already installed; continuing");
    }
}

/// Header names whose values must never reach a log line.
const SENSITIVE_HEADERS: &[&str] = &["authorization", "x-api-key", "cookie", "proxy-authorization"];

/// Returns a printable value for a header, or `None` for credentials.
pub fn redact_header(name: &str, value: &str) -> Option<String> {
    if SENSITIVE_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
        None
    } else {
        Some(value.to_string())
    }
}

/// One line per finished stream, at `info`, with everything needed to
/// reconstruct the request without grepping.
pub fn log_stream_complete(span: &RequestSpan, outcome: &StreamSummary) {
    let total = outcome.prompt_tokens as u64
        + outcome.completion_tokens as u64
        + outcome.cached_prompt_tokens as u64;
    tracing::info!(
        request_id = %span.request_id,
        tenant = %span.tenant_id,
        key = %span.key_id,
        scheme = %span.scheme,
        model = %span.model,
        provider = %span.provider,
        upstream_model = %span.upstream_model,
        status = if outcome.error_code.is_some() { "error" } else { "ok" },
        finish_reason = outcome.finish_reason.as_deref().unwrap_or("-"),
        upstream_id = outcome.upstream_id.as_deref().unwrap_or("-"),
        duration_ms = span.elapsed_ms(),
        ttft_ms = span.ttft_ms().unwrap_or(0),
        prompt_tokens = outcome.prompt_tokens,
        completion_tokens = outcome.completion_tokens,
        reasoning_tokens = outcome.reasoning_tokens,
        cached_prompt_tokens = outcome.cached_prompt_tokens,
        total_tokens = total,
        cost_micro_usd = outcome.cost_micro_usd,
        events = outcome.events,
        bytes_out = outcome.bytes_out,
        client_abort = outcome.aborted_by_client,
        error_code = outcome.error_code.unwrap_or("-"),
        error_retryable = outcome.error_retryable,
        "stream complete"
    );
}

pub fn log_rejected(request_id: &str, tenant: &str, stage: &str, reason: &str) {
    tracing::warn!(
        request_id = %request_id,
        tenant = %tenant,
        stage = %stage,
        reason = %reason,
        "request rejected before streaming"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttft_is_none_until_the_first_token() {
        let mut s = RequestSpan::new("r1".into());
        assert!(s.ttft_ms().is_none());
        s.mark_first_token();
        assert!(s.ttft_ms().is_some());
    }

    #[test]
    fn credential_headers_are_not_printable() {
        assert!(redact_header("Authorization", "Bearer abc").is_none());
        assert!(redact_header("x-api-key", "gw_live_x").is_none());
        assert_eq!(
            redact_header("content-type", "application/json").as_deref(),
            Some("application/json")
        );
    }
}
