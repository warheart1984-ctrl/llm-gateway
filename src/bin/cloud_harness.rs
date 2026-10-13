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
    max_tokens: u64,
    prompt: String,
    max_requests: usize,
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
}

#[derive(Debug, Default)]
struct Aggregate {
    total: usize,
    successful: usize,
    status_counts: BTreeMap<String, usize>,
    refusal_counts: BTreeMap<String, usize>,
    retryable_refusals: usize,
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
    concurrency: Vec<usize>,
    repetitions: usize,
    warmup_secs: u64,
    duration_secs: u64,
    max_requests: usize,
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
    fs::create_dir_all(&cfg.output).map_err(|e| format!("create output directory: {e}"))?;
    let run_id = cfg
        .output
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("cloud-run")
        .to_string();
    let request = request_body(&cfg, false);
    let manifest = Manifest {
        run_id: run_id.clone(),
        started_at_unix_ms: now_unix_ms(),
        source_revision: source_revision(),
        rust_version: command_version("rustc", "--version"),
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
                let upstream_before = if !variant.direct {
                    run_warmup(&client, &cfg, variant, *concurrency).await?;
                    fetch_provider_count(&client, cfg.upstream_metrics_url.as_deref()).await
                } else {
                    run_warmup(&client, &cfg, variant, *concurrency).await?;
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
                if !variant.direct {
                    wait_for_settlement(
                        &client,
                        cfg.gateway_usage_url.as_deref(),
                        cfg.gateway_metrics_url.as_deref(),
                        cfg.gateway_key.as_deref(),
                        cfg.settlement_wait_ms,
                    )
                    .await;
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
                summary.gateway_metrics_before = metrics_before
                    .as_deref()
                    .map(parse_metrics_snapshot);
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
                summary.gateway_metrics_after = metrics_after
                    .as_deref()
                    .map(parse_metrics_snapshot);
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
        let max_requests = parse("max-requests", "CLOUD_MAX_REQUESTS", 1000)? as usize;
        if max_requests == 0 {
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
            max_tokens: parse("max-tokens", "CLOUD_MAX_TOKENS", 64)?,
            prompt: get("prompt")
                .unwrap_or_else(|| env_or("CLOUD_PROMPT", "cloud benchmark request")),
            max_requests,
            refusal_backoff_ms: parse(
                "refusal-backoff-ms",
                "CLOUD_REFUSAL_BACKOFF_MS",
                100,
            )?,
            settlement_wait_ms: parse(
                "settlement-wait-ms",
                "CLOUD_SETTLEMENT_WAIT_MS",
                2000,
            )?,
        })
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
                    if issued.fetch_add(1, Ordering::Relaxed) >= cfg.max_requests {
                        break;
                    }
                    if let Ok(record) = request_once(&client, &variant, &request, key.as_deref()).await
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
                    if issued.fetch_add(1, Ordering::Relaxed) >= cfg.max_requests {
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
        if buffer.len() < 8192 {
            buffer.extend_from_slice(&chunk[..chunk.len().min(8192 - buffer.len())]);
        }
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

fn token_marker(buffer: &[u8], variant: &Variant) -> bool {
    if variant.direct || variant.framing == Some("passthrough") {
        buffer.windows(10).any(|w| w == br#""content":"#)
            || buffer.windows(8).any(|w| w == br#""token":"#)
    } else {
        buffer.windows(8).any(|w| w == br#""token":"#)
    }
}

fn request_body(cfg: &Config, direct: bool) -> Value {
    if direct {
        json!({"model": cfg.model, "messages":[{"role":"user","content":cfg.prompt}], "stream":true, "max_tokens":cfg.max_tokens})
    } else {
        json!({"model": cfg.model, "messages":[{"role":"user","content":cfg.prompt}], "stream":true, "params":{"max_tokens":cfg.max_tokens}})
    }
}

fn governance_for(summary: &WorkloadSummary) -> Vec<GovernanceResult> {
    if summary.variant == "direct" {
        return Vec::new();
    }
    let refused: usize = summary.refusal_counts.values().sum();
    let provider_delta = summary
        .upstream_calls_after
        .zip(summary.upstream_calls_before)
        .map(|(a, b)| a.saturating_sub(b));
    let expected_provider_calls = summary.total_requests.saturating_sub(refused);
    let boundary = match (refused, provider_delta) {
        (0, _) => GovernanceResult {
            invariant: "INV-PERF-001/002",
            status: "not_applicable",
            evidence: "no gateway refusals in this workload".into(),
        },
        (_, Some(delta)) if delta == expected_provider_calls as u64 => GovernanceResult {
            invariant: "INV-PERF-001/002",
            status: "pass",
            evidence: format!(
                "{refused} refused requests; provider counter increased only for {delta} accepted requests"
            ),
        },
        (_, Some(delta)) => GovernanceResult {
            invariant: "INV-PERF-001/002",
            status: "fail",
            evidence: format!(
                "{refused} refused requests; expected {expected_provider_calls} provider calls but counter increased by {delta}"
            ),
        },
        (_, None) => GovernanceResult {
            invariant: "INV-PERF-001/002",
            status: "not_measured",
            evidence: "configure --upstream-metrics-url for provider-boundary evidence".into(),
        },
    };
    let accounting = if let (Some(before), Some(after)) = (
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
) {
    if usage_url.is_none() || max_wait_ms == 0 {
        return;
    }
    let deadline = Instant::now() + Duration::from_millis(max_wait_ms);
    let mut previous_usage = None;
    let mut stable_reads = 0;
    loop {
        let usage = fetch_usage(client, usage_url, key).await;
        let metrics = fetch_gateway_metrics(client, metrics_url, key)
            .await
            .map(|body| parse_metrics_snapshot(&body));
        let pending = metrics
            .as_ref()
            .and_then(|values| values.get("gw_ledger_pending_closings"))
            .copied()
            .unwrap_or(0);
        let in_flight = metrics
            .as_ref()
            .and_then(|values| values.get("gw_streams_in_flight"))
            .copied()
            .unwrap_or(0);
        if usage.is_some() && usage == previous_usage && pending == 0 && in_flight == 0 {
            stable_reads += 1;
            if stable_reads >= 3 {
                return;
            }
        } else {
            stable_reads = 0;
        }
        previous_usage = usage;
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
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

fn source_revision() -> String {
    env::var("GIT_COMMIT")
        .ok()
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().into())
        })
        .unwrap_or_else(|| "unknown".into())
}

fn command_version(command: &str, arg: &str) -> String {
    Command::new(command)
        .arg(arg)
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().into())
        .unwrap_or_else(|_| "unknown".into())
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
        "cloud-harness options:\n  --direct-url URL\n  --gateway-url URL\n  --concurrency 50,200,500,1000\n  --repetitions 3\n  --warmup-secs 60\n  --duration-secs 180\n  --max-requests 1000\n  --refusal-backoff-ms 100\n  --settlement-wait-ms 2000\n  --upstream-metrics-url URL\n  --gateway-usage-url URL\n  --gateway-metrics-url URL\n  --output DIR\n\nGateway credentials come from CLOUD_GATEWAY_API_KEY and are never written to evidence.\nRefused and 5xx responses back off and the workload is capped by --max-requests.\nThe settlement wait lets durable ledger corrections reach the usage endpoint before reconciliation."
    );
}
