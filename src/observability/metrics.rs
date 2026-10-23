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
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(target_os = "linux")]
use std::fs;

use dashmap::DashMap;

use crate::{
    folding::{store::FoldingStore, FoldAccounting},
    receipts::{ReceiptBook, ReceiptChain},
};

use super::logging::StreamSummary;

#[derive(Debug, Default)]
struct Counters {
    requests_total: AtomicU64,
    requests_rejected_total: AtomicU64,
    streams_started_total: AtomicU64,
    streams_in_flight: AtomicU64,
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
    model_denied_total: AtomicU64,
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

/// Per-tenant folding counters. Created only for a tenant id that came from
/// configuration, and only after that tenant made a fold attempt or sent a
/// folded request.
#[derive(Debug, Default)]
struct FoldCounters {
    committed: AtomicU64,
    rejected: AtomicU64,
    failed: AtomicU64,
    unprofitable: AtomicU64,
    refused_admission: AtomicU64,
    pinned_overflow: AtomicU64,
    backed_off: AtomicU64,
    requests: AtomicU64,
    prompt_tokens_cut: AtomicU64,
    summarizer_prompt: AtomicU64,
    summarizer_completion: AtomicU64,
    /// Rejected folds that failed each check, in `checks::CHECK_NAMES` order.
    checks_failed: [AtomicU64; crate::folding::checks::CHECK_NAMES.len()],
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

pub struct Metrics {
    global: Counters,
    by_model: DashMap<String, Arc<ModelRollup>>,
    by_tenant: DashMap<String, Arc<TenantRollup>>,
    folds: DashMap<String, Arc<FoldCounters>>,
    /// Read at scrape time so the gauge is the conversations actually held.
    fold_store: Mutex<Option<Arc<FoldingStore>>>,
    uptime_secs: AtomicU64,
    /// Every receipt-enabled tenant's chain on this replica, set once at
    /// boot. Gauges are read here at scrape time; counters live in each
    /// chain's `Shared` atomics.
    receipts: Mutex<Option<Arc<ReceiptBook>>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            global: Counters::default(),
            by_model: DashMap::new(),
            by_tenant: DashMap::new(),
            folds: DashMap::new(),
            fold_store: Mutex::new(None),
            uptime_secs: AtomicU64::new(0),
            receipts: Mutex::new(None),
        }
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("uptime_secs", &self.uptime_secs.load(Ordering::Relaxed))
            .field(
                "receipts",
                &self
                    .receipts
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .as_ref()
                    .map(|book| book.tenants().len())
                    .unwrap_or(0),
            )
            .finish()
    }
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

    /// Publish the receipt book so `render` can expose per-tenant chain
    /// state at scrape time. Set once at boot; chains open lazily on first
    /// request and are read here through the same `Arc`.
    pub fn set_folding(&self, store: Arc<FoldingStore>) {
        *self.fold_store.lock().unwrap_or_else(|p| p.into_inner()) = Some(store);
    }

    fn fold_counters(&self, tenant: &str) -> Arc<FoldCounters> {
        Arc::clone(
            self.folds
                .entry(tenant.to_string())
                .or_insert_with(|| Arc::new(FoldCounters::default()))
                .value(),
        )
    }

    /// One fold attempt. `prompt_tokens` and `completion_tokens` are the
    /// summarizer's reported usage, summed across the one allowed retry.
    pub fn record_fold(
        &self,
        tenant: &str,
        outcome: FoldAccounting,
        prompt_tokens: u64,
        completion_tokens: u64,
    ) {
        let counters = self.fold_counters(tenant);
        let slot = match outcome {
            FoldAccounting::Committed => &counters.committed,
            FoldAccounting::Rejected => &counters.rejected,
            FoldAccounting::Failed => &counters.failed,
            FoldAccounting::Unprofitable => &counters.unprofitable,
            FoldAccounting::RefusedAdmission => &counters.refused_admission,
            FoldAccounting::PinnedOverflow => &counters.pinned_overflow,
            FoldAccounting::BackedOff => &counters.backed_off,
        };
        slot.fetch_add(1, Ordering::Relaxed);
        counters.summarizer_prompt.fetch_add(prompt_tokens, Ordering::Relaxed);
        counters
            .summarizer_completion
            .fetch_add(completion_tokens, Ordering::Relaxed);
    }

    /// A rejected fold's failed checks: each named check counts the fold once.
    pub fn record_fold_checks(&self, tenant: &str, failed: &BTreeMap<&'static str, u32>) {
        let counters = self.fold_counters(tenant);
        for (slot, name) in crate::folding::checks::CHECK_NAMES.iter().enumerate() {
            if failed.contains_key(name) {
                counters.checks_failed[slot].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// A main request that was sent with a summary in place of old turns.
    pub fn record_folded_request(&self, tenant: &str, prompt_tokens_cut: u64) {
        let counters = self.fold_counters(tenant);
        counters.requests.fetch_add(1, Ordering::Relaxed);
        counters.prompt_tokens_cut.fetch_add(prompt_tokens_cut, Ordering::Relaxed);
    }

    pub fn set_receipts(&self, book: Option<Arc<ReceiptBook>>) {
        *self.receipts.lock().unwrap_or_else(|p| p.into_inner()) = book;
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
        self.global.streams_in_flight.fetch_add(1, Ordering::Relaxed);
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
            RejectionKind::Model => {
                self.global.model_denied_total.fetch_add(1, Ordering::Relaxed);
            }
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
        g.streams_in_flight.fetch_sub(1, Ordering::Relaxed);
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
            "Requests rejected during governance.",
            &[],
            g.requests_rejected_total.load(Ordering::Relaxed),
        );
        let rejections = [
            ("auth", g.auth_failed_total.load(Ordering::Relaxed)),
            ("policy", g.policy_denied_total.load(Ordering::Relaxed)),
            ("rate_limit", g.rate_limited_total.load(Ordering::Relaxed)),
            ("concurrency", g.concurrency_limited_total.load(Ordering::Relaxed)),
            ("budget", g.cost_reservation_rejected_total.load(Ordering::Relaxed)),
            ("model", g.model_denied_total.load(Ordering::Relaxed)),
        ];
        let rejections: Vec<(String, u64)> = rejections
            .into_iter()
            .map(|(reason, value)| (render_labels(&[("reason", reason)], &[]), value))
            .collect();
        family(
            &mut out,
            "gw_requests_rejected_by_reason_total",
            "counter",
            "Requests rejected during governance, by reason.",
            rejections.iter().map(|(l, v)| (l.as_str(), *v)),
        );
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
            "gw_streams_in_flight",
            "gauge",
            "Streams currently relayed from an upstream provider.",
            &[],
            g.streams_in_flight.load(Ordering::Relaxed),
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
        if let Some((cpu_seconds, resident_bytes)) = process_stats() {
            metric(
                &mut out,
                "gw_process_cpu_millis_total",
                "counter",
                "Gateway process CPU time in milliseconds.",
                &[],
                cpu_seconds,
            );
            metric(
                &mut out,
                "gw_process_resident_memory_bytes",
                "gauge",
                "Gateway process resident memory.",
                &[],
                resident_bytes,
            );
        }

        let mut models: BTreeMap<String, Arc<ModelRollup>> = BTreeMap::new();
        for entry in self.by_model.iter() {
            models.insert(entry.key().clone(), Arc::clone(entry.value()));
        }
        // One family at a time: the exposition format allows a single HELP and
        // TYPE per metric name, followed by all its series. Interleaving them
        // per model (as this once did) makes the second model's HELP a parse
        // error, and one bad line fails the whole scrape.
        //
        // A model name may contain a quote or backslash. Anything that cannot
        // be escaped safely becomes a fixed placeholder so the scrape stays
        // parseable.
        let models: Vec<(String, Arc<ModelRollup>)> = models
            .into_iter()
            .map(|(key, m)| {
                let (provider, model) = key.split_once('/').unwrap_or((key.as_str(), ""));
                let labels = render_labels(
                    &[("provider", safe_label(provider)), ("model", safe_label(model))],
                    &[],
                );
                (labels, m)
            })
            .collect();
        type ModelCounter = fn(&ModelRollup) -> u64;
        let model_counters: [(&str, &str, ModelCounter); 6] = [
            ("gw_model_streams_started_total", "Connections opened per model.", |m| m.streams_started.load(Ordering::Relaxed)),
            ("gw_model_streams_total", "Streams that ran per model.", |m| m.streams.load(Ordering::Relaxed)),
            ("gw_model_errors_total", "Failed streams per model.", |m| m.errors.load(Ordering::Relaxed)),
            ("gw_model_prompt_tokens_total", "Prompt tokens per model.", |m| m.prompt_tokens.load(Ordering::Relaxed)),
            ("gw_model_completion_tokens_total", "Completion tokens per model.", |m| m.completion_tokens.load(Ordering::Relaxed)),
            ("gw_model_cost_nano_usd_total", "Spend per model, nano-USD.", |m| m.cost_nano_usd.load(Ordering::Relaxed)),
        ];
        for (name, help, value) in model_counters {
            family(&mut out, name, "counter", help, models.iter().map(|(l, m)| (l.as_str(), value(m))));
        }
        summary_family(
            &mut out,
            "gw_model_ttft_ms",
            "Time to first token per model.",
            models.iter().map(|(l, m)| (l.as_str(), m.ttft_ms_sum.load(Ordering::Relaxed), m.ttft_ms_count.load(Ordering::Relaxed))),
        );
        summary_family(
            &mut out,
            "gw_model_duration_ms",
            "Stream duration per model.",
            models.iter().map(|(l, m)| (l.as_str(), m.duration_ms_sum.load(Ordering::Relaxed), m.streams.load(Ordering::Relaxed))),
        );

        let mut tenants: BTreeMap<String, Arc<TenantRollup>> = BTreeMap::new();
        for entry in self.by_tenant.iter() {
            tenants.insert(entry.key().clone(), Arc::clone(entry.value()));
        }
        let tenants: Vec<(String, Arc<TenantRollup>)> = tenants
            .into_iter()
            .map(|(tenant, t)| (render_labels(&[("tenant", safe_label(&tenant))], &[]), t))
            .collect();
        type TenantCounter = fn(&TenantRollup) -> u64;
        let tenant_counters: [(&str, &str, TenantCounter); 4] = [
            ("gw_tenant_streams_total", "Streams per tenant.", |t| t.streams.load(Ordering::Relaxed)),
            ("gw_tenant_policy_denied_total", "Policy denials per tenant.", |t| t.denied.load(Ordering::Relaxed)),
            ("gw_tenant_errors_total", "Failed streams per tenant.", |t| t.errors.load(Ordering::Relaxed)),
            ("gw_tenant_cost_nano_usd_total", "Spend per tenant, nano-USD.", |t| t.cost_nano_usd.load(Ordering::Relaxed)),
        ];
        for (name, help, value) in tenant_counters {
            family(&mut out, name, "counter", help, tenants.iter().map(|(l, t)| (l.as_str(), value(t))));
        }

        // Receipts: one series per open tenant chain, labelled by the
        // configured tenant id (never by request input).
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|s| s.as_secs())
            .unwrap_or(0);
        if let Some(book) = self
            .receipts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(Arc::clone)
        {
            let mut series: Vec<(String, String, Arc<ReceiptChain>)> = Vec::new();
            for id in book.tenants() {
                let Some(chain) = book.open_chain(&id) else { continue };
                let label = render_labels(&[("tenant", safe_label(&id))], &[]);
                series.push((id, label, chain));
            }
            series.sort_by(|a, b| a.0.cmp(&b.0));
            if !series.is_empty() {
                family(
                    &mut out,
                    "gw_receipts_written_total",
                    "counter",
                    "receipt entries this process durably wrote",
                    series.iter().map(|(_, l, c)| (l.as_str(), c.written())),
                );
                family(
                    &mut out,
                    "gw_receipt_write_failures_total",
                    "counter",
                    "receipt jobs answered Unavailable: a failed write, or a stopped chain",
                    series.iter().map(|(_, l, c)| (l.as_str(), c.write_failures())),
                );
                family(
                    &mut out,
                    "gw_receipt_recoveries_total",
                    "counter",
                    "receipt torn tails quarantined at startup",
                    series.iter().map(|(_, l, c)| (l.as_str(), c.recoveries())),
                );
                family(
                    &mut out,
                    "gw_receipt_entries",
                    "gauge",
                    "receipt entries in the log",
                    series.iter().map(|(_, l, c)| (l.as_str(), c.len())),
                );
                family(
                    &mut out,
                    "gw_receipt_chain_stopped",
                    "gauge",
                    "the receipt chain refused further appends",
                    series.iter().map(|(_, l, c)| (l.as_str(), if c.is_failed() { 1 } else { 0 })),
                );
                let _ = writeln!(out, "# HELP gw_receipt_anchor_age_seconds seconds since the last anchor this process wrote");
                let _ = writeln!(out, "# TYPE gw_receipt_anchor_age_seconds gauge");
                for (_id, l, c) in &series {
                    let age = match c.last_anchor_secs() {
                        0 => -1i64,
                        secs => now.saturating_sub(secs) as i64,
                    };
                    let _ = writeln!(out, "gw_receipt_anchor_age_seconds{l} {age}");
                }
            }
        }

        let store_entries = self
            .fold_store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|store| store.stats().0 as u64)
            .unwrap_or(0);
        metric(
            &mut out,
            "gw_fold_store_entries",
            "gauge",
            "Conversations held in the in-memory folding store.",
            &[],
            store_entries,
        );

        let mut folds: BTreeMap<String, Arc<FoldCounters>> = BTreeMap::new();
        for entry in self.folds.iter() {
            folds.insert(entry.key().clone(), Arc::clone(entry.value()));
        }
        if !folds.is_empty() {
            let mut outcome_series = Vec::new();
            let mut request_series = Vec::new();
            let mut cut_series = Vec::new();
            let mut prompt_series = Vec::new();
            let mut completion_series = Vec::new();
            let mut check_series = Vec::new();
            for (tenant, counters) in &folds {
                let tenant_label = safe_label(tenant);
                for (outcome, value) in [
                    ("committed", counters.committed.load(Ordering::Relaxed)),
                    ("rejected", counters.rejected.load(Ordering::Relaxed)),
                    ("failed", counters.failed.load(Ordering::Relaxed)),
                    ("unprofitable", counters.unprofitable.load(Ordering::Relaxed)),
                    ("refused_admission", counters.refused_admission.load(Ordering::Relaxed)),
                    ("pinned_overflow", counters.pinned_overflow.load(Ordering::Relaxed)),
                    ("backoff", counters.backed_off.load(Ordering::Relaxed)),
                ] {
                    outcome_series.push((
                        render_labels(&[("tenant", tenant_label), ("outcome", outcome)], &[]),
                        value,
                    ));
                }
                let labels = render_labels(&[("tenant", tenant_label)], &[]);
                request_series.push((labels.clone(), counters.requests.load(Ordering::Relaxed)));
                cut_series.push((labels.clone(), counters.prompt_tokens_cut.load(Ordering::Relaxed)));
                prompt_series.push((
                    render_labels(&[("tenant", tenant_label), ("kind", "prompt")], &[]),
                    counters.summarizer_prompt.load(Ordering::Relaxed),
                ));
                completion_series.push((
                    render_labels(&[("tenant", tenant_label), ("kind", "completion")], &[]),
                    counters.summarizer_completion.load(Ordering::Relaxed),
                ));
                for (slot, check) in crate::folding::checks::CHECK_NAMES.iter().enumerate() {
                    check_series.push((
                        render_labels(&[("tenant", tenant_label), ("check", check)], &[]),
                        counters.checks_failed[slot].load(Ordering::Relaxed),
                    ));
                }
            }
            family(
                &mut out,
                "gw_folds_total",
                "counter",
                "Fold attempts by outcome.",
                outcome_series.iter().map(|(l, v)| (l.as_str(), *v)),
            );
            family(
                &mut out,
                "gw_fold_requests_total",
                "counter",
                "Main requests sent with a summary in place of older turns.",
                request_series.iter().map(|(l, v)| (l.as_str(), *v)),
            );
            family(
                &mut out,
                "gw_fold_prompt_tokens_cut_total",
                "counter",
                "Estimated prompt tokens removed from main calls.",
                cut_series.iter().map(|(l, v)| (l.as_str(), *v)),
            );
            family(
                &mut out,
                "gw_fold_summarizer_tokens_total",
                "counter",
                "Real summarizer token usage.",
                prompt_series
                    .iter()
                    .chain(completion_series.iter())
                    .map(|(l, v)| (l.as_str(), *v)),
            );
            family(
                &mut out,
                "gw_fold_checks_failed_total",
                "counter",
                "Rejected folds by summary check failed; a fold failing several counts once for each.",
                check_series.iter().map(|(l, v)| (l.as_str(), *v)),
            );
        }

        out
    }
}

/// Read process resource gauges without adding a background sampler or a
/// platform-specific dependency. On non-Linux hosts the gauges are omitted.
#[cfg(target_os = "linux")]
fn process_stats() -> Option<(u64, u64)> {
    let stat = fs::read_to_string("/proc/self/stat").ok()?;
    let fields = stat.rsplit_once(") ")?.1.split_whitespace().collect::<Vec<_>>();
    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks_per_second <= 0 {
        return None;
    }
    let cpu_millis = (user_ticks + system_ticks)
        .saturating_mul(1000)
        / ticks_per_second as u64;

    let status = fs::read_to_string("/proc/self/status").ok()?;
    let resident_kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    Some((cpu_millis, resident_kib.saturating_mul(1024)))
}

#[cfg(not(target_os = "linux"))]
fn process_stats() -> Option<(u64, u64)> {
    None
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

/// A labelled counter family: metadata once, then one series per label set.
/// Nothing is written for an empty family, so a fresh process does not
/// advertise metrics it has no series for.
fn family<'a>(
    out: &mut String,
    name: &str,
    kind: &str,
    help: &str,
    series: impl IntoIterator<Item = (&'a str, u64)>,
) {
    let mut series = series.into_iter().peekable();
    if series.peek().is_none() {
        return;
    }
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
    for (labels, value) in series {
        let _ = writeln!(out, "{name}{labels} {value}");
    }
}

/// The labelled counterpart of [`histogram_summary`].
fn summary_family<'a>(
    out: &mut String,
    name: &str,
    help: &str,
    series: impl IntoIterator<Item = (&'a str, u64, u64)>,
) {
    let series: Vec<_> = series.into_iter().collect();
    if series.is_empty() {
        return;
    }
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} summary");
    for (labels, sum, count) in &series {
        let _ = writeln!(out, "{name}_sum{labels} {sum}");
        let _ = writeln!(out, "{name}_count{labels} {count}");
    }
    // `_avg` is not part of the summary type, so it is its own family.
    if series.iter().any(|(_, _, count)| *count > 0) {
        let _ = writeln!(out, "# TYPE {name}_avg gauge");
        for (labels, sum, count) in &series {
            if *count > 0 {
                let _ = writeln!(out, "{name}_avg{labels} {:.3}", *sum as f64 / *count as f64);
            }
        }
    }
}

fn histogram_summary(out: &mut String, name: &str, help: &str, sum: u64, count: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} summary");
    let _ = writeln!(out, "{name}_sum {sum}");
    let _ = writeln!(out, "{name}_count {count}");
    if count > 0 {
        let _ = writeln!(out, "# TYPE {name}_avg gauge");
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
    use sha2::{Digest, Sha256};

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

    /// The structural rules a Prometheus text parser enforces, checked over
    /// an exposition with several models and tenants: one HELP and one TYPE
    /// per family, each before the family's samples, all of a family's
    /// samples contiguous, and no series (name plus labels) twice.
    #[test]
    fn the_exposition_is_valid_with_many_models_and_tenants() {
        let m = Metrics::new(std::time::Instant::now());
        let summary = StreamSummary { prompt_tokens: 1, completion_tokens: 1, ..Default::default() };
        for (provider, model, tenant) in [("groq", "a", "t1"), ("groq", "b", "t2"), ("nvidia", "c", "t3")] {
            m.stream_started(provider, model);
            m.stream_finished(provider, model, tenant, &summary, 10, Some(5));
        }
        m.stream_failed_to_start("groq", "b");
        let text = m.render();

        let family_of = |sample: &str| -> String {
            let name = sample.split(['{', ' ']).next().unwrap();
            for suffix in ["_sum", "_count"] {
                if let Some(base) = name.strip_suffix(suffix)
                    && text.contains(&format!("# TYPE {base} summary"))
                {
                    return base.to_string();
                }
            }
            name.to_string()
        };
        let mut helped = std::collections::HashSet::new();
        let mut typed = std::collections::HashSet::new();
        let mut closed = std::collections::HashSet::new();
        let mut series = std::collections::HashSet::new();
        let mut current = String::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let name = rest.split(' ').next().unwrap().to_string();
                assert!(helped.insert(name.clone()), "second HELP for {name}:\n{text}");
            } else if let Some(rest) = line.strip_prefix("# TYPE ") {
                let name = rest.split(' ').next().unwrap().to_string();
                assert!(typed.insert(name.clone()), "second TYPE for {name}:\n{text}");
            } else {
                let fam = family_of(line);
                if fam != current {
                    assert!(!closed.contains(&fam), "{fam} samples are not contiguous:\n{text}");
                    if !current.is_empty() {
                        closed.insert(std::mem::replace(&mut current, fam.clone()));
                    } else {
                        current = fam.clone();
                    }
                }
                assert!(typed.contains(&fam), "{fam} sampled before its TYPE:\n{text}");
                let key = line.rsplit_once(' ').unwrap().0.to_string();
                assert!(series.insert(key.clone()), "duplicate series {key}:\n{text}");
            }
        }
        assert!(text.contains("gw_model_ttft_ms_sum{provider=\"groq\",model=\"b\"} 5"), "{text}");
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

    #[test]
    fn rejected_reasons_balance_to_the_rejected_total() {
        let m = Metrics::new(std::time::Instant::now());
        for kind in [
            RejectionKind::Auth,
            RejectionKind::Policy,
            RejectionKind::RateLimit,
            RejectionKind::Concurrency,
            RejectionKind::Budget,
            RejectionKind::Model,
        ] {
            m.rejection(kind);
        }
        m.rejection(RejectionKind::Model);
        let text = m.render();
        assert!(text.contains("gw_requests_rejected_total 7"));

        // One sample line per reason, including `model`, which used to be
        // counted into the total but never surfaced by reason.
        let by_reason: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("gw_requests_rejected_by_reason_total"))
            .collect();
        assert_eq!(by_reason.len(), 6, "one sample per reason: {by_reason:?}");
        for (reason, count) in [
            ("auth", "1"),
            ("policy", "1"),
            ("rate_limit", "1"),
            ("concurrency", "1"),
            ("budget", "1"),
            ("model", "2"),
        ] {
            let needle = format!("reason=\"{reason}\"");
            assert!(
                by_reason
                    .iter()
                    .any(|l| l.contains(&needle) && l.ends_with(count)),
                "missing {needle} = {count} in {by_reason:?}"
            );
        }

        // The rollup of the labelled series equals the bare total, so nothing
        // is double-counted and no reason is silently dropped.
        let sum: u64 = by_reason
            .iter()
            .filter_map(|l| l.split_whitespace().last())
            .filter_map(|v| v.parse::<u64>().ok())
            .sum();
        assert_eq!(sum, 7);
    }

    #[test]
    fn the_rejected_total_renders_unlabeled() {
        // The old exposition put six `reason` labels on one line, which the
        // format does not allow; the total is a bare counter and the breakdown
        // lives on the by-reason series.
        let m = Metrics::new(std::time::Instant::now());
        m.rejection(RejectionKind::Auth);
        let text = m.render();
        for line in text
            .lines()
            .filter(|l| l.starts_with("gw_requests_rejected_total"))
        {
            assert!(!line.contains('{'), "total must be a bare counter line: {line}");
        }
    }

    #[test]
    fn a_torn_tail_at_startup_counts_as_a_recovery() {
        let m = Metrics::new(std::time::Instant::now());
        let dir = std::env::temp_dir().join(format!(
            "llm-gw-metrics-recovery-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let acme = dir.join("acme");
        std::fs::create_dir_all(&acme).unwrap();
        // A torn tail: a partial line with no terminating newline.
        std::fs::write(acme.join("receipts.jsonl"), "{\"version\":\"infinity.receipt.v2\"").unwrap();
        let book = Arc::new(
            crate::receipts::ReceiptBook::open(
                &dir,
                crate::receipts::ChainOptions {
                    anchor_every: 1_000_000,
                    anchor_interval: std::time::Duration::from_secs(3600),
                    startup_verify: crate::receipts::StartupVerify::Full,
                },
                ["acme"],
            )
            .unwrap(),
        );
        assert_eq!(book.chain("acme").unwrap().recoveries(), 1);
        m.set_receipts(Some(Arc::clone(&book)));
        let text = m.render();
        assert!(
            text.contains("gw_receipt_recoveries_total{tenant=\"acme\"} 1"),
            "{text}"
        );
    }

    #[test]
    fn unknown_tenant_ids_never_create_a_receipts_series() {
        let m = Metrics::new(std::time::Instant::now());
        let dir = std::env::temp_dir().join(format!(
            "llm-gw-metrics-cardinality-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let book = Arc::new(
            crate::receipts::ReceiptBook::open(
                &dir,
                crate::receipts::ChainOptions {
                    anchor_every: 1_000_000,
                    anchor_interval: std::time::Duration::from_secs(3600),
                    startup_verify: crate::receipts::StartupVerify::Full,
                },
                ["acme"],
            )
            .unwrap(),
        );
        m.set_receipts(Some(Arc::clone(&book)));
        let text = m.render();
        assert!(!text.contains("tenant=\"evil\""), "{text}");
        // acme's chain is open, so it is listed even with no entries.
        assert!(text.contains("tenant=\"acme\""), "{text}");
    }

    #[tokio::test]
    async fn two_allows_and_one_outcome_record_three_written_and_three_entries() {
        let m = Metrics::new(std::time::Instant::now());
        let dir = std::env::temp_dir().join(format!(
            "llm-gw-metrics-written-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let book = Arc::new(
            crate::receipts::ReceiptBook::open(
                &dir,
                crate::receipts::ChainOptions {
                    anchor_every: 1_000_000,
                    anchor_interval: std::time::Duration::from_secs(3600),
                    startup_verify: crate::receipts::StartupVerify::Full,
                },
                ["acme"],
            )
            .unwrap(),
        );
        let chain = book.chain("acme").unwrap();
        let salt = crate::receipts::Salt::generate().unwrap();
        let digest = crate::receipts::request_digest(&salt, b"request-bytes");
        let subject = crate::receipts::Subject {
            request_id: "req-1".into(),
            tenant_id: "acme".into(),
            key_id: "ak".into(),
            model: "mock/chat".into(),
            action: "reserve".into(),
            call_digest: Some(digest.clone()),
        };
        chain.allow(&subject).await.unwrap();
        let mut subject2 = subject.clone();
        subject2.request_id = "req-2".into();
        chain.allow(&subject2).await.unwrap();
        let open = crate::receipts::OpenReceipt {
            chain: std::sync::Arc::clone(&chain),
            decision_receipt_id: "receipt:id".into(),
            request_digest: digest,
            salt,
        };
        let response_digest = format!(
            "sha256:{:x}",
            Sha256::digest(b"the answer")
        );
        chain
            .outcome(&open, "completed", Some(response_digest))
            .await
            .unwrap();

        m.set_receipts(Some(Arc::clone(&book)));
        let text = m.render();
        assert!(
            text.contains("gw_receipts_written_total{tenant=\"acme\"} 3"),
            "{text}"
        );
        assert!(
            text.contains("gw_receipt_entries{tenant=\"acme\"} 3"),
            "{text}"
        );
    }

    fn receipt_book(name: &str, anchor_every: u64) -> Arc<crate::receipts::ReceiptBook> {
        let dir = std::env::temp_dir().join(format!("llm-gw-metrics-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Arc::new(
            crate::receipts::ReceiptBook::open(
                &dir,
                crate::receipts::ChainOptions {
                    anchor_every,
                    anchor_interval: std::time::Duration::from_secs(3600),
                    startup_verify: crate::receipts::StartupVerify::Full,
                },
                ["acme"],
            )
            .unwrap(),
        )
    }

    fn receipt_subject(request_id: &str) -> crate::receipts::Subject {
        crate::receipts::Subject {
            request_id: request_id.into(),
            tenant_id: "acme".into(),
            key_id: "ak".into(),
            model: "mock/chat".into(),
            action: "reserve".into(),
            call_digest: None,
        }
    }

    #[tokio::test]
    async fn a_stopped_chain_shows_in_the_stopped_gauge_and_the_failure_counter() {
        let m = Metrics::new(std::time::Instant::now());
        let book = receipt_book("stopped", 1_000_000);
        m.set_receipts(Some(Arc::clone(&book)));
        let chain = book.chain("acme").unwrap();
        chain.allow(&receipt_subject("req-1")).await.unwrap();
        let text = m.render();
        assert!(text.contains("gw_receipt_chain_stopped{tenant=\"acme\"} 0"), "{text}");
        assert!(text.contains("gw_receipt_write_failures_total{tenant=\"acme\"} 0"), "{text}");

        chain.fail_disk_writes_from_now(1);
        assert!(chain.allow(&receipt_subject("req-2")).await.is_err());
        let text = m.render();
        assert!(text.contains("gw_receipt_chain_stopped{tenant=\"acme\"} 1"), "{text}");
        assert!(text.contains("gw_receipt_write_failures_total{tenant=\"acme\"} 1"), "{text}");

        // Every later job the stopped chain refuses counts too.
        assert!(chain.allow(&receipt_subject("req-3")).await.is_err());
        let text = m.render();
        assert!(text.contains("gw_receipt_write_failures_total{tenant=\"acme\"} 2"), "{text}");
        assert!(text.contains("gw_receipts_written_total{tenant=\"acme\"} 1"), "{text}");
    }

    #[tokio::test]
    async fn anchor_age_is_minus_one_until_this_process_anchors() {
        let m = Metrics::new(std::time::Instant::now());
        let book = receipt_book("anchor-age", 2);
        m.set_receipts(Some(Arc::clone(&book)));
        let chain = book.chain("acme").unwrap();
        chain.allow(&receipt_subject("req-1")).await.unwrap();
        let text = m.render();
        assert!(text.contains("gw_receipt_anchor_age_seconds{tenant=\"acme\"} -1"), "{text}");

        chain.allow(&receipt_subject("req-2")).await.unwrap();
        // The writer anchors after answering; give it a moment.
        for _ in 0..200 {
            if chain.last_anchor_secs() != 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let text = m.render();
        let age: i64 = text
            .lines()
            .find_map(|line| line.strip_prefix("gw_receipt_anchor_age_seconds{tenant=\"acme\"} "))
            .unwrap_or_else(|| panic!("{text}"))
            .parse()
            .unwrap();
        assert!((0..5).contains(&age), "{text}");
    }

    #[tokio::test]
    async fn written_counts_this_process_while_entries_counts_the_log() {
        let book = receipt_book("written-vs-entries", 1_000_000);
        let chain = book.chain("acme").unwrap();
        chain.allow(&receipt_subject("req-1")).await.unwrap();
        chain.allow(&receipt_subject("req-2")).await.unwrap();
        let root = book.root().to_path_buf();
        drop(chain);
        drop(book);

        let m = Metrics::new(std::time::Instant::now());
        let book = Arc::new(
            crate::receipts::ReceiptBook::open(
                &root,
                crate::receipts::ChainOptions {
                    anchor_every: 1_000_000,
                    anchor_interval: std::time::Duration::from_secs(3600),
                    startup_verify: crate::receipts::StartupVerify::Full,
                },
                ["acme"],
            )
            .unwrap(),
        );
        m.set_receipts(Some(Arc::clone(&book)));
        book.chain("acme").unwrap().allow(&receipt_subject("req-3")).await.unwrap();
        let text = m.render();
        assert!(text.contains("gw_receipts_written_total{tenant=\"acme\"} 1"), "{text}");
        assert!(text.contains("gw_receipt_entries{tenant=\"acme\"} 3"), "{text}");
    }
}
