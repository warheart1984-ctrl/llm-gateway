//! Prometheus metrics.
//!
//! Hand-rolled on purpose: the counter surface is small and fixed, and a
//! metrics crate would add a dependency and a background task to a service
//! whose whole premise is a small hot path.
//!
//! Cost model: the global token/event counters are relaxed atomic increments
//! (one per token is a few nanoseconds and cannot fail), while anything keyed
//! by model or tenant is updated **once per request**, at completion. That is
//! what keeps high-cardinality work off the token path.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use dashmap::DashMap;

use super::logging::StreamSummary;

#[derive(Debug, Default)]
struct Counters {
    requests_total: AtomicU64,
    requests_rejected_total: AtomicU64,
    streams_started_total: AtomicU64,
    streams_completed_total: AtomicU64,
    streams_failed_total: AtomicU64,
    streams_aborted_total: AtomicU64,
    delta_events_total: AtomicU64,
    prompt_tokens_total: AtomicU64,
    completion_tokens_total: AtomicU64,
    cost_nano_usd_total: AtomicU64,
    bytes_to_client_total: AtomicU64,
    ttft_ms_sum: AtomicU64,
    ttft_ms_count: AtomicU64,
    duration_ms_sum: AtomicU64,
    duration_ms_count: AtomicU64,
    cost_reservation_rejected_total: AtomicU64,
    rate_limited_total: AtomicU64,
    concurrency_limited_total: AtomicU64,
    policy_denied_total: AtomicU64,
    auth_failed_total: AtomicU64,
    upstream_errors_total: AtomicU64,
}

/// Per-(provider, model) rollup. Written once per completed stream.
#[derive(Debug, Default)]
struct ModelRollup {
    streams: AtomicU64,
    streams_started: AtomicU64,
    errors: AtomicU64,
    duration_ms_sum: AtomicU64,
    prompt_tokens: AtomicU64,
    completion_tokens: AtomicU64,
    cost_nano_usd: AtomicU64,
    ttft_ms_sum: AtomicU64,
    ttft_ms_count: AtomicU64,
}

/// Per-tenant rollup, for the governed view: who is spending what.
#[derive(Debug, Default)]
struct TenantRollup {
    streams: AtomicU64,
    errors: AtomicU64,
    denied: AtomicU64,
    cost_nano_usd: AtomicU64,
    prompt_tokens: AtomicU64,
    completion_tokens: AtomicU64,
}

#[derive(Debug, Default)]
pub struct Metrics {
    global: Counters,
    by_model: DashMap<String, Arc<ModelRollup>>,
    by_tenant: DashMap<String, Arc<TenantRollup>>,
    uptime_secs: AtomicU64,
}

pub type SharedMetrics = Arc<Metrics>;

impl Metrics {
    pub fn new(start: std::time::Instant) -> SharedMetrics {
        let m = Arc::new(Self::default());
        m.uptime_secs.store(start.elapsed().as_secs(), Ordering::Relaxed);
        m
    }

    /// Refresh the uptime gauge. Called by the metrics handler, which is the
    /// only place that needs it and runs off the hot path.
    pub fn refresh_uptime(&self, start: std::time::Instant) {
        self.uptime_secs.store(start.elapsed().as_secs(), Ordering::Relaxed);
    }

    pub fn request_observed(&self) {
        self.global.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn auth_failed(&self) {
        self.global.auth_failed_total.fetch_add(1, Ordering::Relaxed);
    }

    /// A connection was opened and a stream is about to be relayed. Called only
    /// once the upstream has accepted the request, so the counter measures
    /// actual streaming work rather than attempts.
    pub fn stream_started(&self, provider: &str, model: &str) {
        self.global.streams_started_total.fetch_add(1, Ordering::Relaxed);
        self.model_rollup(provider, model).streams_started.fetch_add(1, Ordering::Relaxed);
    }

    /// The upstream refused before any bytes were streamed — a 4xx, a connect
    /// failure, a bad URL. Distinct from a mid-stream failure: no tokens were
    /// produced, but the tenant's quota slot and cost reservation were consumed,
    /// so it must be visible on the model.
    pub fn stream_failed_to_start(&self, provider: &str, model: &str) {
        self.global
            .upstream_errors_total
            .fetch_add(1, Ordering::Relaxed);
        self.model_rollup(provider, model).errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Per-token. Relaxed and infallible by design.
    pub fn delta_events(&self, n: u64) {
        self.global.delta_events_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn bytes_to_client(&self, n: u64) {
        self.global.bytes_to_client_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn rejection(&self, kind: RejectionKind) {
        self.global.requests_rejected_total.fetch_add(1, Ordering::Relaxed);
        match kind {
            RejectionKind::Auth => {
                self.global.auth_failed_total.fetch_add(1, Ordering::Relaxed);
            }
            RejectionKind::Policy => {
                self.global.policy_denied_total.fetch_add(1, Ordering::Relaxed);
            }
            RejectionKind::RateLimit => {
                self.global.rate_limited_total.fetch_add(1, Ordering::Relaxed);
            }
            RejectionKind::Concurrency => {
                self.global.concurrency_limited_total.fetch_add(1, Ordering::Relaxed);
            }
            RejectionKind::Budget => {
                self.global.cost_reservation_rejected_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            RejectionKind::Model => {}
        }
    }

    pub fn upstream_error(&self) {
        self.global.upstream_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// One call per finished stream. Everything here is deliberately a single
    /// pass over a handful of atomics.
    pub fn stream_finished(
        &self,
        provider: &str,
        model: &str,
        tenant: &str,
        summary: &StreamSummary,
        duration_ms: u64,
        ttft_ms: Option<u64>,
    ) {
        let g = &self.global;
        let failed = summary.error_code.is_some();

        if failed {
            g.streams_failed_total.fetch_add(1, Ordering::Relaxed);
            g.upstream_errors_total.fetch_add(1, Ordering::Relaxed);
        } else {
            g.streams_completed_total.fetch_add(1, Ordering::Relaxed);
        }
        if summary.aborted_by_client {
            g.streams_aborted_total.fetch_add(1, Ordering::Relaxed);
        }

        g.prompt_tokens_total
            .fetch_add(summary.prompt_tokens as u64, Ordering::Relaxed);
        g.completion_tokens_total
            .fetch_add(summary.completion_tokens as u64, Ordering::Relaxed);
        g.cost_nano_usd_total
            .fetch_add(summary.cost_nano_usd, Ordering::Relaxed);
        g.duration_ms_sum.fetch_add(duration_ms, Ordering::Relaxed);
        g.duration_ms_count.fetch_add(1, Ordering::Relaxed);
        if let Some(ttft) = ttft_ms {
            g.ttft_ms_sum.fetch_add(ttft, Ordering::Relaxed);
            g.ttft_ms_count.fetch_add(1, Ordering::Relaxed);
        }

        let m = self.model_rollup(provider, model);
        m.streams.fetch_add(1, Ordering::Relaxed);
        if failed {
            m.errors.fetch_add(1, Ordering::Relaxed);
        }
        m.duration_ms_sum.fetch_add(duration_ms, Ordering::Relaxed);
        m.prompt_tokens
            .fetch_add(summary.prompt_tokens as u64, Ordering::Relaxed);
        m.completion_tokens
            .fetch_add(summary.completion_tokens as u64, Ordering::Relaxed);
        m.cost_nano_usd
            .fetch_add(summary.cost_nano_usd, Ordering::Relaxed);
        if let Some(ttft) = ttft_ms {
            m.ttft_ms_sum.fetch_add(ttft, Ordering::Relaxed);
            m.ttft_ms_count.fetch_add(1, Ordering::Relaxed);
        }

        let t = self
            .by_tenant
            .entry(tenant.to_string())
            .or_insert_with(|| Arc::new(TenantRollup::default()))
            .clone();
        t.streams.fetch_add(1, Ordering::Relaxed);
        if failed {
            t.errors.fetch_add(1, Ordering::Relaxed);
        }
        t.cost_nano_usd
            .fetch_add(summary.cost_nano_usd, Ordering::Relaxed);
        t.prompt_tokens
            .fetch_add(summary.prompt_tokens as u64, Ordering::Relaxed);
        t.completion_tokens
            .fetch_add(summary.completion_tokens as u64, Ordering::Relaxed);
    }

    /// Fetch or create the rollup for a `(provider, model)` pair.
    fn model_rollup(&self, provider: &str, model: &str) -> Arc<ModelRollup> {
        Arc::clone(
            self.by_model
                .entry(format!("{provider}/{model}"))
                .or_insert_with(|| Arc::new(ModelRollup::default()))
                .value(),
        )
    }

    pub fn tenant_denied(&self, tenant: &str) {
        let t = self
            .by_tenant
            .entry(tenant.to_string())
            .or_insert_with(|| Arc::new(TenantRollup::default()))
            .clone();
        t.denied.fetch_add(1, Ordering::Relaxed);
    }

    pub fn tokens(&self) -> (u64, u64) {
        (
            self.global.prompt_tokens_total.load(Ordering::Relaxed),
            self.global.completion_tokens_total.load(Ordering::Relaxed),
        )
    }

    pub fn spend_nano_usd(&self) -> u64 {
        self.global.cost_nano_usd_total.load(Ordering::Relaxed)
    }

    /// Prometheus text exposition format, version 0.0.4.
    pub fn render(&self) -> String {
        let g = &self.global;
        let mut out = String::with_capacity(8 * 1024);

        metric(
            &mut out,
            "gw_requests_total",
            "counter",
            "Chat requests received.",
            &[],
            g.requests_total.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_requests_rejected_total",
            "counter",
            "Requests rejected during governance, by reason.",
            &[("reason", "auth"), ("reason", "policy"), ("reason", "rate_limit"), ("reason", "concurrency"), ("reason", "budget"), ("reason", "model")],
            g.requests_rejected_total.load(Ordering::Relaxed),
        );
        let rejections = [
            ("auth", g.auth_failed_total.load(Ordering::Relaxed)),
            ("policy", g.policy_denied_total.load(Ordering::Relaxed)),
            ("rate_limit", g.rate_limited_total.load(Ordering::Relaxed)),
            ("concurrency", g.concurrency_limited_total.load(Ordering::Relaxed)),
            ("budget", g.cost_reservation_rejected_total.load(Ordering::Relaxed)),
        ];
        for (reason, value) in rejections {
            metric(
                &mut out,
                "gw_requests_rejected_by_reason_total",
                "counter",
                "Requests rejected during governance.",
                &[("reason", reason)],
                value,
            );
        }
        metric(
            &mut out,
            "gw_streams_started_total",
            "counter",
            "Streams that reached the provider.",
            &[],
            g.streams_started_total.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_streams_completed_total",
            "counter",
            "Streams that ended with a terminal event.",
            &[],
            g.streams_completed_total.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_streams_failed_total",
            "counter",
            "Streams that ended in an error.",
            &[],
            g.streams_failed_total.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_streams_client_aborted_total",
            "counter",
            "Streams the client disconnected from before completion.",
            &[],
            g.streams_aborted_total.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_stream_events_total",
            "counter",
            "Normalized stream events produced across all providers.",
            &[],
            g.delta_events_total.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_prompt_tokens_total",
            "counter",
            "Prompt tokens billed.",
            &[],
            g.prompt_tokens_total.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_completion_tokens_total",
            "counter",
            "Completion tokens generated.",
            &[],
            g.completion_tokens_total.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_cost_nano_usd_total",
            "counter",
            "Estimated spend in nano-USD (1 USD = 1e6).",
            &[],
            g.cost_nano_usd_total.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_response_bytes_total",
            "counter",
            "SSE bytes written to clients.",
            &[],
            g.bytes_to_client_total.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_upstream_errors_total",
            "counter",
            "Errors surfaced by a provider adapter.",
            &[],
            g.upstream_errors_total.load(Ordering::Relaxed),
        );
        histogram_summary(
            &mut out,
            "gw_stream_duration_ms",
            "Wall-clock duration of a stream, from request to terminal event.",
            g.duration_ms_sum.load(Ordering::Relaxed),
            g.duration_ms_count.load(Ordering::Relaxed),
        );
        histogram_summary(
            &mut out,
            "gw_stream_ttft_ms",
            "Time to first client-visible token.",
            g.ttft_ms_sum.load(Ordering::Relaxed),
            g.ttft_ms_count.load(Ordering::Relaxed),
        );
        metric(
            &mut out,
            "gw_uptime_seconds",
            "gauge",
            "Process uptime.",
            &[],
            self.uptime_secs.load(Ordering::Relaxed),
        );

        let mut models: BTreeMap<String, Arc<ModelRollup>> = BTreeMap::new();
        for entry in self.by_model.iter() {
            models.insert(entry.key().clone(), Arc::clone(entry.value()));
        }
        for (key, m) in models {
            let (provider, model) = key.split_once('/').unwrap_or((key.as_str(), ""));
            // A model name may contain a quote or backslash. Prometheus requires
            // those escaped, and anything that cannot be escaped safely becomes
            // a fixed placeholder so the scrape stays parseable.
            let labels = [
                ("provider", safe_label(provider)),
                ("model", safe_label(model)),
            ];
            metric(&mut out, "gw_model_streams_started_total", "counter", "Connections opened per model.", &labels, m.streams_started.load(Ordering::Relaxed));
            metric(&mut out, "gw_model_streams_total", "counter", "Streams that ran per model.", &labels, m.streams.load(Ordering::Relaxed));
            metric(&mut out, "gw_model_errors_total", "counter", "Failed streams per model.", &labels, m.errors.load(Ordering::Relaxed));
            metric(&mut out, "gw_model_prompt_tokens_total", "counter", "Prompt tokens per model.", &labels, m.prompt_tokens.load(Ordering::Relaxed));
            metric(&mut out, "gw_model_completion_tokens_total", "counter", "Completion tokens per model.", &labels, m.completion_tokens.load(Ordering::Relaxed));
            metric(&mut out, "gw_model_cost_nano_usd_total", "counter", "Spend per model, nano-USD.", &labels, m.cost_nano_usd.load(Ordering::Relaxed));
            histogram_summary(&mut out, "gw_model_ttft_ms", "Time to first token per model.", m.ttft_ms_sum.load(Ordering::Relaxed), m.ttft_ms_count.load(Ordering::Relaxed));
            histogram_summary(&mut out, "gw_model_duration_ms", "Stream duration per model.", m.duration_ms_sum.load(Ordering::Relaxed), m.streams.load(Ordering::Relaxed));
        }

        let mut tenants: BTreeMap<String, Arc<TenantRollup>> = BTreeMap::new();
        for entry in self.by_tenant.iter() {
            tenants.insert(entry.key().clone(), Arc::clone(entry.value()));
        }
        for (tenant, t) in tenants {
            let labels = [("tenant", safe_label(&tenant))];
            metric(&mut out, "gw_tenant_streams_total", "counter", "Streams per tenant.", &labels, t.streams.load(Ordering::Relaxed));
            metric(&mut out, "gw_tenant_policy_denied_total", "counter", "Policy denials per tenant.", &labels, t.denied.load(Ordering::Relaxed));
            metric(&mut out, "gw_tenant_errors_total", "counter", "Failed streams per tenant.", &labels, t.errors.load(Ordering::Relaxed));
            metric(&mut out, "gw_tenant_cost_nano_usd_total", "counter", "Spend per tenant, nano-USD.", &labels, t.cost_nano_usd.load(Ordering::Relaxed));
        }

        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionKind {
    Auth,
    Policy,
    RateLimit,
    Concurrency,
    Budget,
    Model,
}

/// Emit a counter or gauge. Label values containing a `"` or `\` are dropped
/// in favour of a placeholder: a model id with a quote in it would otherwise
/// produce invalid exposition format, and a broken `/metrics` scrape takes out
/// alerting along with it.
fn metric(out: &mut String, name: &str, kind: &str, help: &str, labels: &[(&str, &str)], value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
    let _ = writeln!(out, "{name}{} {value}", render_labels(labels, &[]));
}

fn histogram_summary(out: &mut String, name: &str, help: &str, sum: u64, count: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} summary");
    let _ = writeln!(out, "{name}_sum {sum}");
    let _ = writeln!(out, "{name}_count {count}");
    if count > 0 {
        let _ = writeln!(out, "{name}_avg {:.3}", sum as f64 / count as f64);
    }
}

/// Render a label set. Emitted in the order given rather than sorted, so the
/// exposition reads predictably and a test can assert an exact string.
fn render_labels(labels: &[(&str, &str)], extra: &[(&str, &str)]) -> String {
    if labels.is_empty() && extra.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = labels
        .iter()
        .chain(extra.iter())
        .map(|(k, v)| format!("{k}=\"{}\"", escape(v)))
        .collect();
    format!("{{{}}}", parts.join(","))
}

/// Sanitize a label value. Anything that could break exposition format or
/// blow up cardinality is replaced wholesale.
fn safe_label(v: &str) -> &str {
    const MAX: usize = 128;
    if v.is_empty() || v.len() > MAX || v.contains(['"', '\\', '\n', '\r', '\t']) {
        "invalid"
    } else {
        v
    }
}

/// Escape a label value. `safe_label` has already rejected anything that could
/// need escaping, so this is a belt-and-braces pass.
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposition_includes_type_and_help() {
        let m = Metrics::new(std::time::Instant::now());
        let text = m.render();
        assert!(text.contains("# TYPE gw_streams_completed_total counter"));
        assert!(text.contains("# HELP gw_prompt_tokens_total"));
    }

    #[test]
    fn per_model_and_per_tenant_rollups_accumulate() {
        let m = Metrics::new(std::time::Instant::now());
        let summary = StreamSummary {
            prompt_tokens: 10,
            completion_tokens: 20,
            cost_nano_usd: 1234,
            events: 20,
            ..Default::default()
        };
        m.stream_finished("groq", "llama", "acme", &summary, 500, Some(80));
        m.stream_finished("groq", "llama", "acme", &summary, 700, Some(60));
        let text = m.render();
        assert!(text.contains("gw_model_streams_total{provider=\"groq\",model=\"llama\"} 2"));
        assert!(text.contains("gw_tenant_cost_nano_usd_total{tenant=\"acme\"} 2468"));
        assert_eq!(m.tokens(), (20, 40));
        assert_eq!(m.spend_nano_usd(), 2468);
    }

    #[test]
    fn a_refused_request_is_not_counted_as_a_stream() {
        // An upstream 401 produced no tokens, so it must not inflate the stream
        // counters — but it is an upstream error and belongs on the model.
        let m = Metrics::new(std::time::Instant::now());
        m.stream_failed_to_start("groq", "llama");
        let text = m.render();
        assert!(text.contains("gw_streams_started_total 0"));
        assert!(text.contains("gw_upstream_errors_total 1"));
        assert!(text.contains("gw_model_errors_total{provider=\"groq\",model=\"llama\"} 1"));
        assert!(text.contains("gw_model_streams_total{provider=\"groq\",model=\"llama\"} 0"));
    }

    #[test]
    fn started_and_completed_are_tracked_separately() {
        let m = Metrics::new(std::time::Instant::now());
        m.stream_started("groq", "llama");
        m.stream_started("groq", "llama");
        let summary = StreamSummary::default();
        m.stream_finished("groq", "llama", "acme", &summary, 10, None);
        let text = m.render();
        assert!(text.contains("gw_streams_started_total 2"));
        assert!(text.contains("gw_streams_completed_total 1"));
    }

    #[test]
    fn labels_containing_delimiters_are_replaced_not_escaped() {
        // A quote in a label would need escaping; simpler and safer to drop the
        // whole value, so a model id can never break the scrape.
        assert_eq!(safe_label("groq"), "groq");
        assert_eq!(safe_label("weird\"name"), "invalid");
        assert_eq!(safe_label(""), "invalid");
        assert_eq!(safe_label(&"x".repeat(200)), "invalid");
        assert_eq!(safe_label("with\nnewline"), "invalid");
    }

    #[test]
    fn label_rendering_keeps_declaration_order() {
        assert_eq!(
            render_labels(&[("provider", "groq"), ("model", "llama")], &[]),
            r#"{provider="groq",model="llama"}"#
        );
    }
}
