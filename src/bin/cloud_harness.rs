//! External-host benchmark and evidence writer for RFC-LLMGW-001.
//!
//! The existing `benches/load.rs` is the local control: it starts its own
//! provider and gateway so it can inspect every ledger row. This binary keeps
//! that behavior untouched and instead measures already-running endpoints.
//! It is deliberately conservative about governance claims: provider request
//! counts and ledger usage are optional inputs, and missing evidence is marked
//! `not_measured` rather than treated as a pass.
//!
//! Example (all hosts can be private addresses):
//!
//! ```text
//! CLOUD_GATEWAY_API_KEY=... cargo run --release --bin cloud_harness -- \
//!   --direct-url http://provider:9000/v1/chat/completions \
//!   --gateway-url http://gateway:8080/v1/chat/stream \
//!   --upstream-metrics-url http://provider:9000/metrics \
//!   --gateway-usage-url http://gateway:8080/v1/usage \
//!   --model mock/chat --upstream-model mock/chat \
//!   --concurrency 50,200,500,1000 --repetitions 3 \
//!   --warmup-secs 60 --duration-secs 180 --output target/cloud-runs/run-1
//! ```

use std::{
    collections::BTreeMap,
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::StreamExt as _;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
struct Config {
    direct_url: Option<String>,
    gateway_url: Option<String>,
    gateway_key: Option<String>,
    upstream_metrics_url: Option<String>,
    gateway_usage_url: Option<String>,
    gateway_metrics_url: Option<String>,
    output: PathBuf,
    concurrency: Vec<usize>,
    repetitions: usize,
    warmup_secs: u64,
    duration_secs: u64,
    seed: u64,
    backend: String,
    model: String,
    upstream_model: Option<String>,
    max_tokens: u64,
    prompt: String,
    max_requests: Option<usize>,
    refusal_backoff_ms: u64,
    settlement_wait_ms: u64,
}

#[derive(Clone, Debug)]
struct Variant {
    name: String,
    framing: Option<&'static str>,
    url: String,
    direct: bool,
}

#[derive(Debug, Serialize)]
struct RequestRecord {
    run_id: String,
    workload: String,
    variant: String,
    backend: String,
    framing: String,
    repetition: usize,
    concurrency: usize,
    worker: usize,
    sequence: usize,
    status: u16,
    retryable: Option<bool>,
    error_code: Option<String>,
    gateway_rejection: Option<bool>,
    outcome: String,
    ttft_ms: Option<f64>,
    latency_ms: f64,
    request_bytes: usize,
    response_bytes: usize,
    bytes_transferred: usize,
    reserved_nano_usd: Option<u64>,
    billed_nano_usd: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Percentiles {
    p50_ms: Option<f64>,
    p95_ms: Option<f64>,
    p99_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
struct WorkloadSummary {
    run_id: String,
    workload: String,
    variant: String,
    backend: String,
    framing: String,
    repetition: usize,
    concurrency: usize,
    warmup_secs: u64,
    duration_secs: u64,
    measurement_elapsed_secs: f64,
    total_requests: usize,
    successful_completions: usize,
    throughput_rps: f64,
    status_counts: BTreeMap<String, usize>,
    refusal_counts: BTreeMap<String, usize>,
    retryable_refusals: usize,
    gateway_rejections: usize,
    unclassified_gateway_errors: usize,
    upstream_error_status_counts: BTreeMap<String, usize>,
    transport_errors: usize,
    incomplete_streams: usize,
    client_observed_ttft: Percentiles,
    full_response_latency: Percentiles,
    request_bytes: usize,
    response_bytes: usize,
    bytes_transferred: usize,
    peak_concurrent_streams: usize,
    reserved_nano_usd: u64,
    billed_nano_usd: u64,
    upstream_calls_before: Option<u64>,
    upstream_calls_after: Option<u64>,
    gateway_usage_before_nano_usd: Option<u64>,
    gateway_usage_after_nano_usd: Option<u64>,
    gateway_metrics_before: Option<BTreeMap<String, u64>>,
    gateway_metrics_after: Option<BTreeMap<String, u64>>,
    settlement_before_verified: Option<bool>,
    settlement_after_verified: Option<bool>,
}

#[derive(Debug, Default)]
struct Aggregate {
    total: usize,
    successful: usize,
    status_counts: BTreeMap<String, usize>,
    refusal_counts: BTreeMap<String, usize>,
    retryable_refusals: usize,
    gateway_rejections: usize,
    unclassified_gateway_errors: usize,
    upstream_error_status_counts: BTreeMap<String, usize>,
    transport_errors: usize,
    incomplete: usize,
    ttft_ms: Vec<f64>,
    latency_ms: Vec<f64>,
    request_bytes: usize,
    response_bytes: usize,
    peak: usize,
    reserved_nano_usd: u64,
    billed_nano_usd: u64,
}

#[derive(Debug, Deserialize)]
struct ProviderCountSnapshot {
    requests: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct UsageSnapshot {
    spent_nano_usd: Option<u64>,
}

type MetricsSnapshot = BTreeMap<String, u64>;

#[derive(Debug, Serialize)]
struct GovernanceResult {
    invariant: &'static str,
    status: &'static str,
    evidence: String,
}

#[derive(Debug, Serialize)]
struct Manifest {
    run_id: String,
    started_at_unix_ms: u128,
    source_revision: String,
    rust_version: String,
    os: String,
    architecture: String,
    cpu_model: String,
    vcpus: usize,
    memory_mib: Option<u64>,
    instance_type: Option<String>,
    container_image_digest: Option<String>,
    backend: String,
    direct_url_configured: bool,
    gateway_url_configured: bool,
    gateway_metrics_url_configured: bool,
    upstream_model: Option<String>,
    source_diff_sha256: String,
    source_dirty: bool,
    concurrency: Vec<usize>,
    repetitions: usize,
    warmup_secs: u64,
    duration_secs: u64,
    max_requests: Option<usize>,
    refusal_backoff_ms: u64,
    settlement_wait_ms: u64,
    seed: u64,
    request: Value,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("cloud-harness: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cfg = Config::parse(env::args().skip(1))?;
    if cfg.direct_url.is_none() && cfg.gateway_url.is_none() {
        return Err("configure --direct-url and/or --gateway-url".into());
    }
    prepare_output(&cfg.output)?;
    let run_id = cfg
        .output
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("cloud-run")
        .to_string();
    let (source_revision, source_dirty, source_diff_sha256) = source_provenance()?;
    let request = manifest_request(&cfg);
    let manifest = Manifest {
        run_id: run_id.clone(),
        started_at_unix_ms: now_unix_ms(),
        source_revision,
        source_dirty,
        source_diff_sha256,
        rust_version: rustc_version(),
        os: env::consts::OS.into(),
        architecture: env::consts::ARCH.into(),
        cpu_model: cpu_model(),
        vcpus: std::thread::available_parallelism().map_or(1, |n| n.get()),
        memory_mib: memory_mib(),
        instance_type: env::var("CLOUD_INSTANCE_TYPE").ok(),
        container_image_digest: env::var("CLOUD_CONTAINER_IMAGE_DIGEST").ok(),
        backend: cfg.backend.clone(),
        direct_url_configured: cfg.direct_url.is_some(),
        gateway_url_configured: cfg.gateway_url.is_some(),
        gateway_metrics_url_configured: cfg.gateway_metrics_url.is_some(),
        upstream_model: cfg.upstream_model.clone(),
        concurrency: cfg.concurrency.clone(),
        repetitions: cfg.repetitions,
        warmup_secs: cfg.warmup_secs,
        duration_secs: cfg.duration_secs,
        max_requests: cfg.max_requests,
        refusal_backoff_ms: cfg.refusal_backoff_ms,
        settlement_wait_ms: cfg.settlement_wait_ms,
        seed: cfg.seed,
        request,
    };
    write_json(&cfg.output.join("manifest.json"), &manifest)?;

    let mut variants = Vec::new();
    if let Some(url) = &cfg.direct_url {
        variants.push(Variant {
            name: "direct".into(),
            framing: None,
            url: url.clone(),
            direct: true,
        });
    }
    if let Some(url) = &cfg.gateway_url {
        variants.push(Variant {
            name: "gateway-passthrough".into(),
            framing: Some("passthrough"),
            url: url.clone(),
            direct: false,
        });
        variants.push(Variant {
            name: "gateway-normalized".into(),
            framing: Some("normalized"),
            url: url.clone(),
            direct: false,
        });
    }
    shuffle(&mut variants, cfg.seed);

    let request_file = File::create(cfg.output.join("requests.jsonl"))
        .map_err(|e| format!("create requests.jsonl: {e}"))?;
    let writer = Arc::new(tokio::sync::Mutex::new(BufWriter::new(request_file)));
    let client = Client::builder()
        .pool_max_idle_per_host(1024)
        .tcp_keepalive(Duration::from_secs(30))
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(|e| format!("build HTTP client: {e}"))?;
    let mut summaries = Vec::new();
    let mut governance = Vec::new();

    for repetition in 0..cfg.repetitions {
        for concurrency in &cfg.concurrency {
            for variant in &variants {
                run_warmup(&client, &cfg, variant, *concurrency).await?;
                let settlement_before_verified = if !variant.direct {
                    wait_for_settlement(
                        &client,
                        cfg.gateway_usage_url.as_deref(),
                        cfg.gateway_metrics_url.as_deref(),
                        cfg.gateway_key.as_deref(),
                        cfg.settlement_wait_ms,
                    )
                    .await
                } else {
                    false
                };
                let upstream_before = if !variant.direct {
                    fetch_provider_count(&client, cfg.upstream_metrics_url.as_deref()).await
                } else {
                    None
                };
                let usage_before = if !variant.direct {
                    fetch_usage(
                        &client,
                        cfg.gateway_usage_url.as_deref(),
                        cfg.gateway_key.as_deref(),
                    )
                    .await
                } else {
                    None
                };
                let metrics_before = if !variant.direct {
                    fetch_gateway_metrics(
                        &client,
                        cfg.gateway_metrics_url.as_deref(),
                        cfg.gateway_key.as_deref(),
                    )
                    .await
                } else {
                    None
                };
                if let Some(metrics) = &metrics_before {
                    write_metrics_snapshot(
                        &cfg.output,
                        repetition,
                        *concurrency,
                        &variant.name,
                        "before",
                        metrics,
                    )?;
                }
                let mut measurement_cfg = cfg.clone();
                measurement_cfg.warmup_secs = 0;
                let mut summary = run_workload(
                    &client,
                    &writer,
                    &run_id,
                    &measurement_cfg,
                    variant,
                    repetition,
                    *concurrency,
                    upstream_before,
                    usage_before,
                )
                .await?;
                summary.warmup_secs = cfg.warmup_secs;
                summary.settlement_before_verified =
                    (!variant.direct).then_some(settlement_before_verified);
                if !variant.direct {
                    let settlement_after_verified = wait_for_settlement(
                        &client,
                        cfg.gateway_usage_url.as_deref(),
                        cfg.gateway_metrics_url.as_deref(),
                        cfg.gateway_key.as_deref(),
                        cfg.settlement_wait_ms,
                    )
                    .await;
                    summary.settlement_after_verified = Some(settlement_after_verified);
                }
                let upstream_after = if !variant.direct {
                    fetch_provider_count(&client, cfg.upstream_metrics_url.as_deref()).await
                } else {
                    None
                };
                let usage_after = if !variant.direct {
                    fetch_usage(
                        &client,
                        cfg.gateway_usage_url.as_deref(),
                        cfg.gateway_key.as_deref(),
                    )
                    .await
                } else {
                    None
                };
                summary.upstream_calls_after = upstream_after;
                summary.gateway_usage_after_nano_usd = usage_after;
                summary.gateway_metrics_before =
                    metrics_before.as_deref().map(parse_metrics_snapshot);
                let metrics_after = if !variant.direct {
                    fetch_gateway_metrics(
                        &client,
                        cfg.gateway_metrics_url.as_deref(),
                        cfg.gateway_key.as_deref(),
                    )
                    .await
                } else {
                    None
                };
                if let Some(metrics) = &metrics_after {
                    write_metrics_snapshot(
                        &cfg.output,
                        repetition,
                        *concurrency,
                        &variant.name,
                        "after",
                        metrics,
                    )?;
                }
                summary.gateway_metrics_after =
                    metrics_after.as_deref().map(parse_metrics_snapshot);
                governance.extend(governance_for(&summary));
                summaries.push(summary);
            }
        }
    }
    writer
        .lock()
        .await
        .flush()
        .map_err(|e| format!("flush requests.jsonl: {e}"))?;

    write_json(&cfg.output.join("summary.json"), &summaries)?;
    write_json(&cfg.output.join("governance-results.json"), &governance)?;
    write_comparison(&cfg.output.join("comparison.md"), &summaries)?;
    write_checksums(&cfg.output)?;
    println!(
        "cloud-harness: evidence written to {}",
        cfg.output.display()
    );
    Ok(())
}

impl Config {
    fn parse<I>(args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
    {
        let mut values = BTreeMap::<String, String>::new();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            if arg == "--help" || arg == "-h" {
                print_help();
                std::process::exit(0);
            }
            let key = arg
                .strip_prefix("--")
                .ok_or_else(|| format!("unexpected argument `{arg}`"))?;
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for `--{key}`"))?;
            values.insert(key.to_string(), value);
        }
        let get = |name: &str| values.get(name).cloned();
        let env_or = |name: &str, default: &str| env::var(name).unwrap_or_else(|_| default.into());
        let concurrency = get("concurrency")
            .unwrap_or_else(|| env_or("CLOUD_CONCURRENCY", "50,200,500,1000"))
            .split(',')
            .map(|v| {
                v.trim()
                    .parse::<usize>()
                    .map_err(|_| format!("invalid concurrency `{v}`"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if concurrency.is_empty() || concurrency.contains(&0) {
            return Err("concurrency must contain positive integers".into());
        }
        let parse = |name: &str, env_name: &str, default: u64| {
            get(name)
                .or_else(|| env::var(env_name).ok())
                .map_or(Ok(default), |v| {
                    v.parse::<u64>()
                        .map_err(|_| format!("invalid {name} `{v}`"))
                })
        };
        let max_requests = get("max-requests")
            .or_else(|| env::var("CLOUD_MAX_REQUESTS").ok())
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|_| format!("invalid max-requests `{value}`"))
            })
            .transpose()?;
        if max_requests == Some(0) {
            return Err("max-requests must be positive".into());
        }
        Ok(Self {
            direct_url: get("direct-url").or_else(|| env::var("CLOUD_DIRECT_URL").ok()),
            gateway_url: get("gateway-url").or_else(|| env::var("CLOUD_GATEWAY_URL").ok()),
            gateway_key: env::var("CLOUD_GATEWAY_API_KEY").ok(),
            upstream_metrics_url: get("upstream-metrics-url")
                .or_else(|| env::var("CLOUD_UPSTREAM_METRICS_URL").ok()),
            gateway_usage_url: get("gateway-usage-url")
                .or_else(|| env::var("CLOUD_GATEWAY_USAGE_URL").ok()),
            gateway_metrics_url: get("gateway-metrics-url")
                .or_else(|| env::var("CLOUD_GATEWAY_METRICS_URL").ok()),
            output: PathBuf::from(
                get("output").unwrap_or_else(|| env_or("CLOUD_OUTPUT", "target/cloud-runs/run")),
            ),
            concurrency,
            repetitions: parse("repetitions", "CLOUD_REPETITIONS", 3)? as usize,
            warmup_secs: parse("warmup-secs", "CLOUD_WARMUP_SECS", 60)?,
            duration_secs: parse("duration-secs", "CLOUD_DURATION_SECS", 180)?,
            seed: parse("seed", "CLOUD_SEED", 0x4c4c_4d47)?,
            backend: get("backend").unwrap_or_else(|| env_or("CLOUD_BACKEND", "unknown")),
            model: get("model").unwrap_or_else(|| env_or("CLOUD_MODEL", "mock/chat")),
            upstream_model: get("upstream-model").or_else(|| env::var("CLOUD_UPSTREAM_MODEL").ok()),
            max_tokens: parse("max-tokens", "CLOUD_MAX_TOKENS", 64)?,
            prompt: get("prompt")
                .unwrap_or_else(|| env_or("CLOUD_PROMPT", "cloud benchmark request")),
            max_requests,
            refusal_backoff_ms: parse("refusal-backoff-ms", "CLOUD_REFUSAL_BACKOFF_MS", 100)?,
            settlement_wait_ms: parse("settlement-wait-ms", "CLOUD_SETTLEMENT_WAIT_MS", 2000)?,
        })
    }
}

fn try_issue(issued: &AtomicUsize, max_requests: Option<usize>) -> bool {
    let Some(limit) = max_requests else {
        issued.fetch_add(1, Ordering::Relaxed);
        return true;
    };
    let mut current = issued.load(Ordering::Relaxed);
    loop {
        if current >= limit {
            return false;
        }
        match issued.compare_exchange_weak(
            current,
            current + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

async fn run_warmup(
    client: &Client,
    cfg: &Config,
    variant: &Variant,
    concurrency: usize,
) -> Result<(), String> {
    if cfg.warmup_secs == 0 {
        return Ok(());
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let issued = Arc::new(AtomicUsize::new(0));
    let end = Instant::now() + Duration::from_secs(cfg.warmup_secs);
    let handles = (0..concurrency)
        .map(|_| {
            let client = client.clone();
            let variant = variant.clone();
            let request = request_body(cfg, variant.direct);
            let key = cfg.gateway_key.clone();
            let barrier = Arc::clone(&barrier);
            let issued = Arc::clone(&issued);
            let cfg = cfg.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                while Instant::now() < end {
                    if !try_issue(&issued, cfg.max_requests) {
                        break;
                    }
                    if let Ok(record) =
                        request_once(&client, &variant, &request, key.as_deref()).await
                        && (record.status == 429 || (500..=599).contains(&record.status))
                        && cfg.refusal_backoff_ms > 0
                    {
                        tokio::time::sleep(Duration::from_millis(cfg.refusal_backoff_ms)).await;
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    barrier.wait().await;
    for handle in handles {
        handle
            .await
            .map_err(|e| format!("warmup worker join: {e}"))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_workload(
    client: &Client,
    writer: &Arc<tokio::sync::Mutex<BufWriter<File>>>,
    run_id: &str,
    cfg: &Config,
    variant: &Variant,
    repetition: usize,
    concurrency: usize,
    upstream_before: Option<u64>,
    usage_before: Option<u64>,
) -> Result<WorkloadSummary, String> {
    let (tx, mut rx) = mpsc::unbounded_channel::<RequestRecord>();
    let writer = Arc::clone(writer);
    let peak_active = Arc::new(AtomicUsize::new(0));
    let peak_active_for_writer = Arc::clone(&peak_active);
    let writer_task = tokio::spawn(async move {
        let mut aggregate = Aggregate::default();
        while let Some(record) = rx.recv().await {
            aggregate.total += 1;
            *aggregate
                .status_counts
                .entry(record.status.to_string())
                .or_default() += 1;
            match record.outcome.as_str() {
                "success" => aggregate.successful += 1,
                "incomplete_stream" => aggregate.incomplete += 1,
                outcome if outcome.starts_with("transport_error") => {
                    aggregate.transport_errors += 1
                }
                outcome if outcome.starts_with("refused_") => {
                    *aggregate
                        .refusal_counts
                        .entry(outcome.to_string())
                        .or_default() += 1;
                }
                _ => {}
            }
            if record.gateway_rejection == Some(true) {
                aggregate.gateway_rejections += 1;
            } else if record.variant != "direct"
                && record.status != 0
                && !(200..300).contains(&record.status)
                && record.gateway_rejection.is_none()
            {
                aggregate.unclassified_gateway_errors += 1;
            }
            if record.retryable == Some(true) {
                aggregate.retryable_refusals += 1;
            }
            if (500..=599).contains(&record.status) {
                *aggregate
                    .upstream_error_status_counts
                    .entry(record.status.to_string())
                    .or_default() += 1;
            }
            aggregate.request_bytes += record.request_bytes;
            aggregate.response_bytes += record.response_bytes;
            aggregate.reserved_nano_usd += record.reserved_nano_usd.unwrap_or(0);
            aggregate.billed_nano_usd += record.billed_nano_usd.unwrap_or(0);
            if record.status != 0 {
                aggregate.latency_ms.push(record.latency_ms);
            }
            if let Some(ttft) = record.ttft_ms {
                aggregate.ttft_ms.push(ttft);
            }
            let line = serde_json::to_vec(&record).map_err(|e| e.to_string())?;
            let mut file = writer.lock().await;
            file.write_all(&line).map_err(|e| e.to_string())?;
            file.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        aggregate.peak = peak_active_for_writer.load(Ordering::Relaxed);
        Ok::<_, String>(aggregate)
    });

    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let active = Arc::new(AtomicUsize::new(0));
    let issued = Arc::new(AtomicUsize::new(0));
    let warmup_end = Instant::now() + Duration::from_secs(cfg.warmup_secs);
    let handles = (0..concurrency)
        .map(|worker| {
            let client = client.clone();
            let tx = tx.clone();
            let barrier = Arc::clone(&barrier);
            let variant = variant.clone();
            let request = request_body(cfg, variant.direct);
            let key = cfg.gateway_key.clone();
            let run_id = run_id.to_string();
            let cfg = cfg.clone();
            let active = Arc::clone(&active);
            let peak_active = Arc::clone(&peak_active);
            let issued = Arc::clone(&issued);
            tokio::spawn(async move {
                let mut sequence = 0;
                while Instant::now() < warmup_end {
                    let _ = request_once(&client, &variant, &request, key.as_deref()).await;
                }
                barrier.wait().await;
                let end = Instant::now() + Duration::from_secs(cfg.duration_secs);
                while Instant::now() < end {
                    if !try_issue(&issued, cfg.max_requests) {
                        break;
                    }
                    let open = active.fetch_add(1, Ordering::Relaxed) + 1;
                    peak_active.fetch_max(open, Ordering::Relaxed);
                    let record = request_once(&client, &variant, &request, key.as_deref()).await;
                    active.fetch_sub(1, Ordering::Relaxed);
                    let record = match record {
                        Ok(mut record) => {
                            record.run_id = run_id.clone();
                            record.backend = cfg.backend.clone();
                            record.repetition = repetition;
                            record.concurrency = concurrency;
                            record.worker = worker;
                            record.sequence = sequence;
                            record
                        }
                        Err(error) => RequestRecord {
                            run_id: run_id.clone(),
                            workload: "streaming".into(),
                            variant: variant.name.clone(),
                            backend: cfg.backend.clone(),
                            framing: variant.framing.unwrap_or("direct").into(),
                            repetition,
                            concurrency,
                            worker,
                            sequence,
                            status: 0,
                            retryable: None,
                            error_code: None,
                            gateway_rejection: None,
                            outcome: format!("transport_error:{error}"),
                            ttft_ms: None,
                            latency_ms: 0.0,
                            request_bytes: serde_json::to_vec(&request).map_or(0, |v| v.len()),
                            response_bytes: 0,
                            bytes_transferred: 0,
                            reserved_nano_usd: None,
                            billed_nano_usd: None,
                        },
                    };
                    let backoff = record.status == 429 || (500..=599).contains(&record.status);
                    tx.send(record).map_err(|_| "evidence writer stopped")?;
                    if backoff && cfg.refusal_backoff_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(cfg.refusal_backoff_ms)).await;
                    }
                    sequence += 1;
                }
                Ok::<_, String>(())
            })
        })
        .collect::<Vec<_>>();
    let measurement_started = Instant::now();
    barrier.wait().await;
    for handle in handles {
        handle.await.map_err(|e| format!("worker join: {e}"))??;
    }
    drop(tx);
    let aggregate = writer_task
        .await
        .map_err(|e| format!("writer join: {e}"))??;
    let mut latency = aggregate.latency_ms;
    let mut ttft = aggregate.ttft_ms;
    latency.sort_by(f64::total_cmp);
    ttft.sort_by(f64::total_cmp);
    let elapsed = measurement_started.elapsed().as_secs_f64().max(0.001);
    Ok(WorkloadSummary {
        run_id: run_id.into(),
        workload: "streaming".into(),
        variant: variant.name.clone(),
        backend: cfg.backend.clone(),
        framing: variant.framing.unwrap_or("direct").into(),
        repetition,
        concurrency,
        warmup_secs: cfg.warmup_secs,
        duration_secs: cfg.duration_secs,
        measurement_elapsed_secs: elapsed,
        total_requests: aggregate.total,
        successful_completions: aggregate.successful,
        throughput_rps: aggregate.successful as f64 / elapsed,
        status_counts: aggregate.status_counts,
        refusal_counts: aggregate.refusal_counts,
        retryable_refusals: aggregate.retryable_refusals,
        gateway_rejections: aggregate.gateway_rejections,
        unclassified_gateway_errors: aggregate.unclassified_gateway_errors,
        upstream_error_status_counts: aggregate.upstream_error_status_counts,
        transport_errors: aggregate.transport_errors,
        incomplete_streams: aggregate.incomplete,
        client_observed_ttft: Percentiles::from(&ttft),
        full_response_latency: Percentiles::from(&latency),
        request_bytes: aggregate.request_bytes,
        response_bytes: aggregate.response_bytes,
        bytes_transferred: aggregate.request_bytes + aggregate.response_bytes,
        peak_concurrent_streams: aggregate.peak,
        reserved_nano_usd: aggregate.reserved_nano_usd,
        billed_nano_usd: aggregate.billed_nano_usd,
        upstream_calls_before: upstream_before,
        upstream_calls_after: upstream_before,
        gateway_usage_before_nano_usd: usage_before,
        gateway_usage_after_nano_usd: usage_before,
        gateway_metrics_before: None,
        gateway_metrics_after: None,
        settlement_before_verified: None,
        settlement_after_verified: None,
    })
}

impl Percentiles {
    fn from(values: &[f64]) -> Self {
        let at = |p: f64| {
            (!values.is_empty()).then(|| values[((values.len() - 1) as f64 * p).round() as usize])
        };
        Self {
            p50_ms: at(0.50),
            p95_ms: at(0.95),
            p99_ms: at(0.99),
        }
    }
}

async fn request_once(
    client: &Client,
    variant: &Variant,
    request: &Value,
    key: Option<&str>,
) -> Result<RequestRecord, String> {
    let request_bytes = serde_json::to_vec(request).map_err(|e| e.to_string())?;
    let started = Instant::now();
    let mut builder = client
        .post(&variant.url)
        .header("content-type", "application/json");
    if !variant.direct {
        if let Some(key) = key {
            builder = builder.header("x-api-key", key);
        }
        if let Some(framing) = variant.framing {
            builder = builder.header("x-gateway-framing", framing);
        }
    }
    let response = builder
        .body(request_bytes.clone())
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let retryable = response
        .headers()
        .get("x-error-retryable")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("true"));
    let reserved_nano_usd = response
        .headers()
        .get("x-reserved-nano-usd")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    let mut body = response.bytes_stream();
    let mut response_bytes = 0;
    let mut first_token = None;
    let mut buffer = Vec::new();
    let mut done_probe = Vec::new();
    let mut saw_done = false;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        response_bytes += chunk.len();
        append_scan_buffer(&mut buffer, &chunk);
        if !saw_done {
            done_probe.extend_from_slice(&chunk);
            saw_done = done_probe.windows(6).any(|window| window == b"[DONE]");
            if done_probe.len() > 5 {
                let keep_from = done_probe.len() - 5;
                done_probe.drain(..keep_from);
            }
        }
        if first_token.is_none() && token_marker(&buffer, variant) {
            first_token = Some(started.elapsed().as_secs_f64() * 1000.0);
        }
    }
    let latency = started.elapsed().as_secs_f64() * 1000.0;
    let outcome = if !status.is_success() {
        format!("refused_{}", status.as_u16())
    } else if !saw_done {
        "incomplete_stream".into()
    } else {
        "success".into()
    };
    let error_code = json_string_field(&buffer, br#""code""#);
    let gateway_rejection =
        classify_gateway_rejection(variant.direct, status.as_u16(), error_code.as_deref());
    let billed_nano_usd = number_after(&buffer, b"\"cost_nano_usd\":");
    Ok(RequestRecord {
        run_id: String::new(),
        workload: "streaming".into(),
        variant: variant.name.clone(),
        backend: String::new(),
        framing: variant.framing.unwrap_or("direct").into(),
        repetition: 0,
        concurrency: 0,
        worker: 0,
        sequence: 0,
        status: status.as_u16(),
        retryable,
        error_code,
        gateway_rejection,
        outcome,
        ttft_ms: first_token,
        latency_ms: latency,
        request_bytes: request_bytes.len(),
        response_bytes,
        bytes_transferred: request_bytes.len() + response_bytes,
        reserved_nano_usd,
        billed_nano_usd,
    })
}

fn number_after(bytes: &[u8], marker: &[u8]) -> Option<u64> {
    let start = bytes
        .windows(marker.len())
        .position(|window| window == marker)?
        + marker.len();
    let digits = bytes[start..]
        .iter()
        .skip_while(|byte| byte.is_ascii_whitespace())
        .take_while(|byte| byte.is_ascii_digit())
        .copied()
        .collect::<Vec<_>>();
    (!digits.is_empty())
        .then(|| String::from_utf8_lossy(&digits).parse().ok())
        .flatten()
}

const TOKEN_SCAN_BYTES: usize = 8192;

fn append_scan_buffer(buffer: &mut Vec<u8>, chunk: &[u8]) {
    if chunk.len() >= TOKEN_SCAN_BYTES {
        buffer.clear();
        buffer.extend_from_slice(&chunk[chunk.len() - TOKEN_SCAN_BYTES..]);
        return;
    }
    buffer.extend_from_slice(chunk);
    if buffer.len() > TOKEN_SCAN_BYTES {
        let excess = buffer.len() - TOKEN_SCAN_BYTES;
        buffer.drain(..excess);
    }
}

fn token_marker(buffer: &[u8], variant: &Variant) -> bool {
    if variant.direct || variant.framing == Some("passthrough") {
        has_nonempty_json_field(buffer, br#""content""#)
            || has_nonempty_json_field(buffer, br#""token""#)
    } else {
        has_nonempty_json_field(buffer, br#""token""#)
    }
}

fn has_nonempty_json_field(buffer: &[u8], field: &[u8]) -> bool {
    let mut search_from = 0;
    while search_from + field.len() <= buffer.len() {
        let Some(relative) = buffer[search_from..]
            .windows(field.len())
            .position(|window| window == field)
        else {
            return false;
        };
        let field_start = search_from + relative + field.len();
        let Some(colon) = buffer[field_start..].iter().position(|byte| *byte == b':') else {
            return false;
        };
        let value_start = field_start + colon + 1;
        let mut deserializer = serde_json::Deserializer::from_slice(&buffer[value_start..]);
        if let Ok(value) = Value::deserialize(&mut deserializer)
            && json_value_has_text(&value)
        {
            return true;
        }
        search_from = field_start;
    }
    false
}

fn json_string_field(buffer: &[u8], field: &[u8]) -> Option<String> {
    let mut search_from = 0;
    while search_from + field.len() <= buffer.len() {
        let relative = buffer[search_from..]
            .windows(field.len())
            .position(|window| window == field)?;
        let field_start = search_from + relative + field.len();
        let colon = buffer[field_start..]
            .iter()
            .position(|byte| *byte == b':')?;
        let value_start = field_start + colon + 1;
        let mut deserializer = serde_json::Deserializer::from_slice(&buffer[value_start..]);
        if let Ok(Value::String(value)) = Value::deserialize(&mut deserializer) {
            return Some(value);
        }
        search_from = field_start;
    }
    None
}

fn classify_gateway_rejection(direct: bool, status: u16, error_code: Option<&str>) -> Option<bool> {
    if direct || (200..300).contains(&status) {
        None
    } else {
        error_code.map(|code| !code.starts_with("upstream_"))
    }
}

fn json_value_has_text(value: &Value) -> bool {
    match value {
        Value::String(text) => !text.is_empty(),
        Value::Array(values) => values.iter().any(json_value_has_text),
        Value::Object(fields) => ["content", "text", "token"]
            .iter()
            .filter_map(|key| fields.get(*key))
            .any(json_value_has_text),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn request_body(cfg: &Config, direct: bool) -> Value {
    let model = if direct {
        cfg.upstream_model.as_deref().unwrap_or(&cfg.model)
    } else {
        &cfg.model
    };
    if direct {
        json!({"model": model, "messages":[{"role":"user","content":cfg.prompt}], "stream":true, "max_tokens":cfg.max_tokens})
    } else {
        json!({"model": model, "messages":[{"role":"user","content":cfg.prompt}], "stream":true, "params":{"max_tokens":cfg.max_tokens}})
    }
}

fn manifest_request(cfg: &Config) -> Value {
    json!({
        "model": cfg.model,
        "upstream_model": cfg.upstream_model,
        "messages": [{
            "role": "user",
            "content_sha256": hex(&Sha256::digest(cfg.prompt.as_bytes())),
            "content_bytes": cfg.prompt.len(),
        }],
        "stream": true,
        "max_tokens": cfg.max_tokens,
        "prompt_redacted": true,
    })
}

fn governance_for(summary: &WorkloadSummary) -> Vec<GovernanceResult> {
    if summary.variant == "direct" {
        return Vec::new();
    }
    let rejection_counter_delta = metric_delta(
        summary.gateway_metrics_before.as_ref(),
        summary.gateway_metrics_after.as_ref(),
        "gw_requests_rejected_total",
    );
    let refused = rejection_counter_delta.unwrap_or(summary.gateway_rejections as u64) as usize;
    let provider_delta = summary
        .upstream_calls_after
        .zip(summary.upstream_calls_before)
        .map(|(a, b)| a.saturating_sub(b));
    let expected_provider_calls = summary.total_requests.saturating_sub(refused);
    let boundary = if rejection_counter_delta.is_none() {
        GovernanceResult {
            invariant: "INV-PERF-001/002",
            status: "not_measured",
            evidence: "configure --gateway-metrics-url so gateway rejection counters can be compared with provider calls".into(),
        }
    } else if rejection_counter_delta != Some(summary.gateway_rejections as u64) {
        GovernanceResult {
            invariant: "INV-PERF-001/002",
            status: "fail",
            evidence: format!(
                "gateway rejection counter increased by {} but request metadata classified {} gateway rejections",
                rejection_counter_delta.unwrap_or_default(),
                summary.gateway_rejections
            ),
        }
    } else if summary.transport_errors > 0 || summary.unclassified_gateway_errors > 0 {
        GovernanceResult {
            invariant: "INV-PERF-001/002",
            status: "not_measured",
            evidence: format!(
                "provider boundary is ambiguous: {} transport errors and {} non-success responses lacked gateway error metadata",
                summary.transport_errors, summary.unclassified_gateway_errors
            ),
        }
    } else {
        match (refused, provider_delta) {
            (0, _) => GovernanceResult {
                invariant: "INV-PERF-001/002",
                status: "not_applicable",
                evidence: "no classified gateway refusals in this workload".into(),
            },
            (_, Some(delta)) if delta == expected_provider_calls as u64 => GovernanceResult {
                invariant: "INV-PERF-001/002",
                status: "pass",
                evidence: format!(
                    "{refused} classified gateway refusals; provider counter increased for {delta} accepted requests, including upstream failures"
                ),
            },
            (_, Some(delta)) => GovernanceResult {
                invariant: "INV-PERF-001/002",
                status: "fail",
                evidence: format!(
                    "{refused} classified gateway refusals; expected {expected_provider_calls} provider calls but counter increased by {delta}"
                ),
            },
            (_, None) => GovernanceResult {
                invariant: "INV-PERF-001/002",
                status: "not_measured",
                evidence: "configure --upstream-metrics-url for provider-boundary evidence".into(),
            },
        }
    };
    let accounting = if summary.settlement_after_verified != Some(true) {
        GovernanceResult {
            invariant: "INV-PERF-007",
            status: "not_measured",
            evidence: "settlement barrier was not verified; usage reconciliation is unverified"
                .into(),
        }
    } else if let (Some(before), Some(after)) = (
        summary.gateway_usage_before_nano_usd,
        summary.gateway_usage_after_nano_usd,
    ) {
        let usage_delta = after.saturating_sub(before);
        let expected = if summary.variant == "gateway-normalized" {
            summary.billed_nano_usd
        } else {
            summary.reserved_nano_usd
        };
        if expected == usage_delta && (expected > 0 || summary.successful_completions == 0) {
            GovernanceResult {
                invariant: "INV-PERF-007",
                status: "pass",
                evidence: format!(
                    "usage delta and per-request {} reconcile at {usage_delta} nano-USD",
                    if summary.variant == "gateway-normalized" {
                        "billed"
                    } else {
                        "reserved"
                    }
                ),
            }
        } else {
            GovernanceResult {
                invariant: "INV-PERF-007",
                status: "fail",
                evidence: format!(
                    "usage delta={usage_delta} nano-USD but per-request expected total={expected}"
                ),
            }
        }
    } else {
        GovernanceResult { invariant: "INV-PERF-007", status: "not_measured", evidence: "configure --gateway-usage-url and inspect durable ledger rows before claiming reconciliation".into() }
    };
    vec![
        boundary,
        accounting,
        GovernanceResult {
            invariant: "INV-PERF-009",
            status: "pass",
            evidence: "manifest, raw requests, summary, and SHA256SUMS emitted".into(),
        },
    ]
}

fn metric_delta(
    before: Option<&BTreeMap<String, u64>>,
    after: Option<&BTreeMap<String, u64>>,
    name: &str,
) -> Option<u64> {
    Some(after?.get(name)?.saturating_sub(*before?.get(name)?))
}

async fn fetch_gateway_metrics(
    client: &Client,
    url: Option<&str>,
    key: Option<&str>,
) -> Option<String> {
    let url = url?;
    let mut request = client.get(url);
    if let Some(key) = key {
        request = request.header("x-api-key", key);
    }
    request.send().await.ok()?.text().await.ok()
}

fn parse_metrics_snapshot(body: &str) -> MetricsSnapshot {
    body.lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (name, value) = line.split_once(' ')?;
            let value = value.trim().parse::<f64>().ok()?;
            value.is_finite().then(|| (name.to_string(), value as u64))
        })
        .collect()
}

fn write_metrics_snapshot(
    output: &Path,
    repetition: usize,
    concurrency: usize,
    variant: &str,
    phase: &str,
    body: &str,
) -> Result<(), String> {
    let path = output.join(format!(
        "gateway-metrics-r{repetition}-c{concurrency}-{variant}-{phase}.prom"
    ));
    fs::write(&path, body).map_err(|e| format!("write {}: {e}", path.display()))
}

async fn fetch_provider_count(client: &Client, url: Option<&str>) -> Option<u64> {
    let url = url?;
    let value = client
        .get(url)
        .send()
        .await
        .ok()?
        .json::<ProviderCountSnapshot>()
        .await
        .ok()?;
    value.requests
}

async fn fetch_usage(client: &Client, url: Option<&str>, key: Option<&str>) -> Option<u64> {
    let url = url?;
    let mut request = client.get(url);
    if let Some(key) = key {
        request = request.header("x-api-key", key);
    }
    request
        .send()
        .await
        .ok()?
        .json::<UsageSnapshot>()
        .await
        .ok()?
        .spent_nano_usd
}

async fn wait_for_settlement(
    client: &Client,
    usage_url: Option<&str>,
    metrics_url: Option<&str>,
    key: Option<&str>,
    max_wait_ms: u64,
) -> bool {
    if usage_url.is_none() || max_wait_ms == 0 {
        return false;
    }
    let deadline = Instant::now() + Duration::from_millis(max_wait_ms);
    let mut previous_usage = None;
    let mut stable_reads = 0;
    loop {
        let usage = fetch_usage(client, usage_url, key).await;
        let metrics = fetch_gateway_metrics(client, metrics_url, key)
            .await
            .map(|body| parse_metrics_snapshot(&body));
        let Some(metrics) = metrics else {
            stable_reads = 0;
            previous_usage = usage;
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let Some(&pending) = metrics.get("gw_ledger_pending_closings") else {
            stable_reads = 0;
            previous_usage = usage;
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let Some(&in_flight) = metrics.get("gw_streams_in_flight") else {
            stable_reads = 0;
            previous_usage = usage;
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        if usage.is_some() && usage == previous_usage && pending == 0 && in_flight == 0 {
            stable_reads += 1;
            if stable_reads >= 3 {
                return true;
            }
        } else {
            stable_reads = 0;
        }
        previous_usage = usage;
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn prepare_output(dir: &Path) -> Result<(), String> {
    match fs::metadata(dir) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(format!("output path is not a directory: {}", dir.display()));
            }
            let mut entries = fs::read_dir(dir)
                .map_err(|e| format!("inspect output directory {}: {e}", dir.display()))?;
            if entries.next().is_some() {
                return Err(format!(
                    "output directory is not empty: {}; choose a new path",
                    dir.display()
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir_all(dir)
            .map_err(|e| format!("create output directory {}: {e}", dir.display())),
        Err(error) => Err(format!(
            "inspect output directory {}: {error}",
            dir.display()
        )),
    }
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let file = File::create(path).map_err(|e| format!("create {}: {e}", path.display()))?;
    serde_json::to_writer_pretty(file, value).map_err(|e| format!("write {}: {e}", path.display()))
}

fn write_comparison(path: &Path, summaries: &[WorkloadSummary]) -> Result<(), String> {
    let mut file =
        BufWriter::new(File::create(path).map_err(|e| format!("create comparison.md: {e}"))?);
    writeln!(file, "# Cloud benchmark comparison\n\nResults are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.\n")
        .map_err(|e| e.to_string())?;
    writeln!(file, "| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |\n|---|---:|---:|---:|---:|---:|---:|---:|")
        .map_err(|e| e.to_string())?;
    for s in summaries {
        let refused: usize = s.refusal_counts.values().sum();
        writeln!(
            file,
            "| {} | {} | {} | {} | {:.2} | {} | {} | {} |",
            s.variant,
            s.concurrency,
            s.repetition,
            s.successful_completions,
            s.throughput_rps,
            display(s.client_observed_ttft.p50_ms),
            display(s.full_response_latency.p50_ms),
            refused
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn write_checksums(dir: &Path) -> Result<(), String> {
    let mut entries = fs::read_dir(dir)
        .map_err(|e| e.to_string())?
        .filter_map(Result::ok)
        .filter(|e| e.file_name() != "SHA256SUMS")
        .collect::<Vec<_>>();
    entries.sort_by_key(|e| e.file_name());
    let mut out = BufWriter::new(File::create(dir.join("SHA256SUMS")).map_err(|e| e.to_string())?);
    for entry in entries {
        if !entry.file_type().map_err(|e| e.to_string())?.is_file() {
            continue;
        }
        let bytes = fs::read(entry.path()).map_err(|e| e.to_string())?;
        let digest = Sha256::digest(bytes);
        writeln!(
            out,
            "{}  {}",
            hex(&digest),
            entry.file_name().to_string_lossy()
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn display(value: Option<f64>) -> String {
    value.map_or_else(|| "-".into(), |v| format!("{v:.3}"))
}

fn source_provenance() -> Result<(String, bool, String), String> {
    let root = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .map_err(|e| format!("inspect source checkout: {e}"))?;
    if !root.status.success() {
        return Err("benchmark must run from a Git checkout".into());
    }
    let root = String::from_utf8_lossy(&root.stdout).trim().to_string();
    let status = Command::new("git")
        .current_dir(&root)
        .args(["status", "--porcelain=v1", "--untracked-files=all"])
        .output()
        .map_err(|e| format!("inspect source checkout status: {e}"))?;
    if !status.status.success() {
        return Err("could not determine whether the source checkout is dirty".into());
    }
    let dirty = !status.stdout.is_empty();
    let revision = Command::new("git")
        .current_dir(&root)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|e| format!("read source revision: {e}"))?;
    if !revision.status.success() {
        return Err("could not read the source revision".into());
    }
    let diff = Command::new("git")
        .current_dir(&root)
        .args(["diff", "--binary", "HEAD"])
        .output()
        .map_err(|e| format!("hash source diff: {e}"))?;
    if !diff.status.success() {
        return Err("could not read the source diff".into());
    }
    let diff_sha256 = hex(&Sha256::digest(&diff.stdout));
    if dirty {
        return Err(format!(
            "source checkout is dirty; commit or remove all tracked and untracked changes before running the benchmark (diff sha256: {diff_sha256})"
        ));
    }
    Ok((
        String::from_utf8_lossy(&revision.stdout).trim().into(),
        dirty,
        diff_sha256,
    ))
}

fn rustc_version() -> String {
    let compiler = env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    Command::new(compiler)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().into())
        .filter(|version: &String| !version.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

fn cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|v| {
            v.lines()
                .find_map(|l| l.strip_prefix("model name\t: ").map(str::to_string))
        })
        .unwrap_or_else(|| "unknown".into())
}

fn memory_mib() -> Option<u64> {
    fs::read_to_string("/proc/meminfo")
        .ok()?
        .lines()
        .find_map(|l| {
            l.strip_prefix("MemTotal:")
                .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())
        })
        .map(|kb| kb / 1024)
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
}

fn shuffle<T>(values: &mut [T], mut seed: u64) {
    for i in (1..values.len()).rev() {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        values.swap(i, (seed as usize) % (i + 1));
    }
}

fn print_help() {
    println!(
        "cloud-harness options:\n  --direct-url URL\n  --gateway-url URL\n  --model GATEWAY_MODEL\n  --upstream-model PROVIDER_MODEL\n  --concurrency 50,200,500,1000\n  --repetitions 3\n  --warmup-secs 60\n  --duration-secs 180\n  --max-requests N\n  --refusal-backoff-ms 100\n  --settlement-wait-ms 2000\n  --upstream-metrics-url URL\n  --gateway-usage-url URL\n  --gateway-metrics-url URL\n  --output DIR\n\nGateway credentials come from CLOUD_GATEWAY_API_KEY and are never written to evidence.\nThe workload runs for the requested duration unless --max-requests is supplied.\nRefused and 5xx responses back off; output directories must be empty or new.\nThe settlement wait requires usage stability and explicit zero pending/in-flight metrics.\nRuns require a clean Git checkout; source commit, dirty status, and diff hash are recorded."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variant(direct: bool, framing: Option<&'static str>) -> Variant {
        Variant {
            name: "test".into(),
            framing,
            url: "http://localhost".into(),
            direct,
        }
    }

    fn config() -> Config {
        Config {
            direct_url: None,
            gateway_url: None,
            gateway_key: None,
            upstream_metrics_url: None,
            gateway_usage_url: None,
            gateway_metrics_url: None,
            output: PathBuf::from("target/test-run"),
            concurrency: vec![1],
            repetitions: 1,
            warmup_secs: 0,
            duration_secs: 1,
            seed: 1,
            backend: "test".into(),
            model: "openrouter/gpt-4.1-mini".into(),
            upstream_model: Some("openai/gpt-4.1-mini".into()),
            max_tokens: 8,
            prompt: "private prompt".into(),
            max_requests: Some(1),
            refusal_backoff_ms: 0,
            settlement_wait_ms: 1,
        }
    }

    #[test]
    fn capped_issue_slots_are_exact_and_unlimited_workloads_do_not_stop() {
        let issued = AtomicUsize::new(0);
        assert!(try_issue(&issued, Some(2)));
        assert!(try_issue(&issued, Some(2)));
        assert!(!try_issue(&issued, Some(2)));
        assert!(try_issue(&issued, None));
        assert_eq!(issued.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn ttft_requires_nonempty_content_or_token() {
        let direct = variant(true, None);
        let normalized = variant(false, Some("normalized"));
        assert!(!token_marker(
            br#"data: {"choices":[{"delta":{"role":"assistant","content":""}}]}"#,
            &direct
        ));
        assert!(token_marker(
            br#"data: {"choices":[{"delta":{"content":"hello"}}]}"#,
            &direct
        ));
        assert!(!token_marker(br#"data: {"token":""}"#, &normalized));
        assert!(token_marker(br#"data: {"token":"hello"}"#, &normalized));
    }

    #[test]
    fn ttft_scan_keeps_the_tail_after_more_than_one_buffer() {
        let variant = variant(false, Some("normalized"));
        let mut buffer = Vec::new();
        append_scan_buffer(&mut buffer, &vec![b'x'; TOKEN_SCAN_BYTES]);
        append_scan_buffer(&mut buffer, br#"data: {"token":"late"}"#);
        assert!(token_marker(&buffer, &variant));
        assert!(buffer.len() <= TOKEN_SCAN_BYTES);
    }

    #[test]
    fn direct_requests_accept_a_provider_native_model() {
        let cfg = config();
        assert_eq!(request_body(&cfg, true)["model"], "openai/gpt-4.1-mini");
        assert_eq!(
            request_body(&cfg, false)["model"],
            "openrouter/gpt-4.1-mini"
        );
    }

    #[test]
    fn manifests_hash_prompt_content_instead_of_storing_it() {
        let cfg = config();
        let request = manifest_request(&cfg);
        let serialized = serde_json::to_string(&request).expect("serialize manifest request");
        assert!(!serialized.contains("private prompt"));
        assert_eq!(request["messages"][0]["content_bytes"], 14);
        assert_eq!(
            request["messages"][0]["content_sha256"],
            hex(&Sha256::digest(b"private prompt"))
        );
        assert_eq!(request["prompt_redacted"], true);
    }

    #[test]
    fn upstream_errors_are_not_classified_as_gateway_rejections() {
        assert_eq!(
            classify_gateway_rejection(false, 502, Some("upstream_error")),
            Some(false)
        );
        assert_eq!(
            classify_gateway_rejection(false, 429, Some("rate_limited")),
            Some(true)
        );
        assert_eq!(classify_gateway_rejection(false, 502, None), None);
        assert_eq!(classify_gateway_rejection(true, 502, Some("error")), None);
    }

    #[test]
    fn missing_metric_counter_is_not_treated_as_zero() {
        let before = BTreeMap::from([(String::from("other"), 2)]);
        let after = BTreeMap::from([(String::from("other"), 3)]);
        assert_eq!(
            metric_delta(Some(&before), Some(&after), "gw_requests_rejected_total"),
            None
        );
        assert_eq!(metric_delta(None, Some(&after), "other"), None);
    }

    #[test]
    fn output_directory_must_be_empty() {
        let path = env::temp_dir().join(format!(
            "llm-gateway-harness-output-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create test output directory");
        assert!(prepare_output(&path).is_ok());
        fs::write(path.join("old.json"), b"stale").expect("write stale evidence");
        let error = prepare_output(&path).expect_err("stale output must be rejected");
        assert!(error.contains("not empty"));
        fs::remove_dir_all(path).expect("remove test output directory");
    }
}
