//! Load harness: how much the gateway adds, and where it starts refusing.
//!
//! ```text
//! cargo bench --bench load
//! ```
//!
//! A fake upstream streams answers at a fixed pace (first token after
//! `LOAD_TTFT_MS`, then `LOAD_TOKENS` tokens `LOAD_INTERVAL_MS` apart). At
//! each concurrency level, `C` clients loop on streaming requests for
//! `LOAD_SECONDS`, first straight at the upstream, then through the gateway.
//! The difference is what the gateway costs:
//!
//! * **added TTFT**: time to the first token through the gateway minus the
//!   same percentile straight from the upstream, at the same concurrency;
//! * **ok/s**: completed streams per second through the gateway;
//! * **refused**: answers that were not a 200, by status. The gateway runs
//!   with the shipped `max_concurrent_streams_global` (512) unless
//!   `LOAD_GLOBAL_CAP` says otherwise, so levels above it show the cap
//!   refusing. A refused client waits 50 ms before its next attempt;
//! * **KiB/stream**: peak heap through the gateway minus peak heap straight
//!   to the upstream, divided by the most streams the gateway held open at
//!   once. Counted by this binary's allocator, so it is heap only.
//!
//! What this is not: production numbers. Upstream, gateway and clients share
//! one machine and its CPU (each on its own runtime), over loopback, against
//! an upstream that never slows down. Real vendors add tens to hundreds of
//! milliseconds that dwarf everything here. Read the results as the
//! gateway's own overhead and ceilings, on the machine that produced them.
//!
//! Levels are `LOAD_LEVELS` (default `50,200,500,1000`) and framing is
//! `LOAD_FRAMING` (`normalized`, the default, or `passthrough`). Each level
//! is appended to `target/load-report.jsonl`. Under `cargo test` (no
//! `--bench` flag) it runs one tiny level as a smoke check.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeMap,
    io::Write as _,
    net::SocketAddr,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{Router, body::Body, http::header, response::Response, routing::post};
use futures_util::StreamExt as _;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Heap accounting
// ---------------------------------------------------------------------------

/// The system allocator, counting live bytes and their high-water mark.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Start a new high-water mark from the current live heap; returns it.
fn reset_peak() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Plan {
    levels: Vec<usize>,
    seconds: u64,
    ttft_ms: u64,
    tokens: u32,
    interval_ms: u64,
    global_cap: usize,
    framing: String,
}

impl Plan {
    fn from_env(smoke: bool) -> Self {
        let num = |name: &str, default: u64| {
            std::env::var(name).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default)
        };
        let levels = std::env::var("LOAD_LEVELS")
            .ok()
            .map(|v| v.split(',').filter_map(|l| l.trim().parse().ok()).collect::<Vec<usize>>())
            .filter(|l| !l.is_empty());
        let framing = std::env::var("LOAD_FRAMING").unwrap_or_else(|_| "normalized".into());
        assert!(
            framing == "normalized" || framing == "passthrough",
            "LOAD_FRAMING must be `normalized` or `passthrough`"
        );
        if smoke {
            return Self {
                levels: vec![4],
                seconds: 1,
                ttft_ms: 5,
                tokens: 4,
                interval_ms: 5,
                global_cap: 512,
                framing,
            };
        }
        Self {
            levels: levels.unwrap_or_else(|| vec![50, 200, 500, 1000]),
            seconds: num("LOAD_SECONDS", 10),
            ttft_ms: num("LOAD_TTFT_MS", 50),
            tokens: num("LOAD_TOKENS", 32) as u32,
            interval_ms: num("LOAD_INTERVAL_MS", 20),
            global_cap: num("LOAD_GLOBAL_CAP", 512) as usize,
            framing,
        }
    }
}

// ---------------------------------------------------------------------------
// Fake upstream
// ---------------------------------------------------------------------------

/// Token text. Appears in no other frame either side sends (ids are hex,
/// field names have no `zq`), so finding it marks the first token.
const TOKEN: &str = "zq";

fn upstream_app(plan: &Plan) -> Router {
    let (ttft, tokens, interval) = (
        Duration::from_millis(plan.ttft_ms),
        plan.tokens,
        Duration::from_millis(plan.interval_ms),
    );
    Router::new().route(
        "/v1/chat/completions",
        post(move |_body: axum::body::Bytes| async move {
            let frames = futures_util::stream::unfold(0u32, move |i| async move {
                let frame = if i == 0 {
                    tokio::time::sleep(ttft).await;
                    chunk()
                } else if i < tokens {
                    tokio::time::sleep(interval).await;
                    chunk()
                } else if i == tokens {
                    let done = json!({
                        "id": "load", "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }],
                        "usage": { "prompt_tokens": 8, "completion_tokens": tokens, "total_tokens": 8 + tokens }
                    });
                    format!("data: {done}\n\ndata: [DONE]\n\n")
                } else {
                    return None;
                };
                Some((Ok::<_, std::io::Error>(frame), i + 1))
            });
            let mut resp = Response::new(Body::from_stream(frames));
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
            resp
        }),
    )
}

fn chunk() -> String {
    let frame = json!({ "id": "load", "model": "load-model",
                        "choices": [{ "index": 0, "delta": { "content": TOKEN } }] });
    format!("data: {frame}\n\n")
}

/// Serve `app` on its own runtime and thread, so its scheduling does not
/// compete with the clients' inside one executor.
fn serve_on_own_runtime<F, Fut>(name: &str, threads: usize, app: F) -> SocketAddr
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Router>,
{
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async move {
                let app = app().await;
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
                tx.send(listener.local_addr().unwrap()).unwrap();
                axum::serve(listener, app).await.expect("serve");
            });
        })
        .expect("spawn server thread");
    rx.recv().expect("server address")
}

// ---------------------------------------------------------------------------
// Gateway
// ---------------------------------------------------------------------------

const KEY: &str = "load-key";

fn boot_gateway(upstream: SocketAddr, plan: &Plan, threads: usize) -> SocketAddr {
    let dir = std::env::temp_dir().join(format!("llm-gateway-load-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("models.yaml"),
        format!(
            "schema_version: 1\nmodels:\n  load/chat:\n    provider: nvidia\n    endpoint: http://{upstream}/v1/chat/completions\n    \
             upstream_model: load-model\n    cost:\n      input_per_mtok_usd: 1.0\n      output_per_mtok_usd: 1.0\n"
        ),
    )
    .unwrap();
    // One tenant whose own limits never bind, so any refusal is the
    // gateway-wide cap.
    std::fs::write(
        dir.join("tenants.yaml"),
        r#"tenants:
  - tenant_id: load
    enabled: true
    credentials:
      - key_id: ak_load
        key: load-key
        scopes: [chat:stream]
    allowed_models: ["*"]
    limits:
      requests_per_minute: 100000000
      tokens_per_minute: 4000000000
      max_concurrent_streams: 1000000
      max_output_tokens: 4096
      daily_budget_nano_usd: 1000000000000000
"#,
    )
    .unwrap();
    let settings = llm_gateway::config::Settings {
        server: llm_gateway::config::ServerConfig {
            bind_addr: "127.0.0.1".into(),
            port: 0,
            max_concurrent_streams_global: plan.global_cap,
            ..Default::default()
        },
        registry: llm_gateway::config::RegistryConfig {
            models_path: dir.join("models.yaml"),
            tenants_path: dir.join("tenants.yaml"),
            hot_reload: false,
            reload_interval_ms: 60_000,
        },
        ledger: llm_gateway::config::LedgerConfig {
            backend: llm_gateway::config::LedgerBackend::Memory,
            ..Default::default()
        },
        ..Default::default()
    };
    serve_on_own_runtime("gateway", threads, move || async move {
        let state = llm_gateway::bootstrap::build(settings).await.expect("build gateway");
        llm_gateway::api::router(state)
    })
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Tally {
    ttft_us: Vec<u64>,
    total_us: Vec<u64>,
    ok: u64,
    refused: BTreeMap<u16, u64>,
    broken: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum Target {
    Upstream,
    Gateway,
}

struct Run {
    tally: Tally,
    wall: Duration,
    peak_heap: usize,
    max_open: usize,
}

async fn run_level(client: &reqwest::Client, target: Target, url: &str, plan: &Plan, concurrency: usize) -> Run {
    let tally = Arc::new(Mutex::new(Tally::default()));
    let open = Arc::new(AtomicUsize::new(0));
    let max_open = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let body = match target {
        Target::Upstream => json!({ "model": "load-model", "stream": true,
                                    "messages": [{ "role": "user", "content": "hi" }], "max_tokens": 64 }),
        Target::Gateway => json!({ "model": "load/chat", "stream": true, "framing": plan.framing,
                                   "messages": [{ "role": "user", "content": "hi" }],
                                   "params": { "max_tokens": 64 } }),
    };
    let body = Arc::new(serde_json::to_vec(&body).unwrap());

    let baseline = reset_peak();
    let started = Instant::now();
    let workers: Vec<_> = (0..concurrency)
        .map(|_| {
            let (client, url, body) = (client.clone(), url.to_string(), Arc::clone(&body));
            let (tally, open, max_open, stop) =
                (Arc::clone(&tally), Arc::clone(&open), Arc::clone(&max_open), Arc::clone(&stop));
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    one_request(&client, &url, &body, &tally, &open, &max_open).await;
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_secs(plan.seconds)).await;
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }
    let wall = started.elapsed();
    let peak_heap = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
    let tally = Arc::try_unwrap(tally).ok().unwrap().into_inner().unwrap();
    Run { tally, wall, peak_heap, max_open: max_open.load(Ordering::Relaxed) }
}

async fn one_request(
    client: &reqwest::Client,
    url: &str,
    body: &Arc<Vec<u8>>,
    tally: &Mutex<Tally>,
    open: &AtomicUsize,
    max_open: &AtomicUsize,
) {
    let sent = Instant::now();
    let reply = client
        .post(url)
        .header("x-api-key", KEY)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.as_ref().clone())
        .send()
        .await;
    let reply = match reply {
        Ok(r) => r,
        Err(_) => {
            tally.lock().unwrap().broken += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
            return;
        }
    };
    let status = reply.status().as_u16();
    if status != 200 {
        let _ = reply.bytes().await;
        *tally.lock().unwrap().refused.entry(status).or_default() += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
        return;
    }
    let now_open = open.fetch_add(1, Ordering::Relaxed) + 1;
    max_open.fetch_max(now_open, Ordering::Relaxed);

    let mut stream = reply.bytes_stream();
    let mut first: Option<Duration> = None;
    // Frames can split across reads; keep a short tail to match across them.
    let mut tail = Vec::<u8>::new();
    let mut seen_done = false;
    let mut failed = false;
    while let Some(piece) = stream.next().await {
        let Ok(piece) = piece else {
            failed = true;
            break;
        };
        tail.extend_from_slice(&piece);
        let text = String::from_utf8_lossy(&tail);
        if first.is_none() && text.contains(TOKEN) {
            first = Some(sent.elapsed());
        }
        if text.contains("event: error") {
            failed = true;
        }
        if text.contains("[DONE]") {
            seen_done = true;
        }
        let keep = tail.len().saturating_sub(32);
        tail.drain(..keep);
    }
    open.fetch_sub(1, Ordering::Relaxed);
    let total = sent.elapsed();
    let mut t = tally.lock().unwrap();
    match first {
        Some(first) if seen_done && !failed => {
            t.ok += 1;
            t.ttft_us.push(first.as_micros() as u64);
            t.total_us.push(total.as_micros() as u64);
        }
        _ => t.broken += 1,
    }
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

fn pct(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1] as f64 / 1000.0
}

fn main() {
    let smoke = !std::env::args().any(|a| a == "--bench");
    let plan = Plan::from_env(smoke);
    // The gateway logs one summary line per stream; format it, discard it,
    // so its cost is in the numbers.
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .with_writer(std::io::sink)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let share = (cores / 3).max(1);
    let upstream = serve_on_own_runtime("upstream", share, {
        let plan = plan.clone();
        move || async move { upstream_app(&plan) }
    });
    let gateway = boot_gateway(upstream, &plan, share);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(share)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(4096)
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();
        let upstream_url = format!("http://{upstream}/v1/chat/completions");
        let gateway_url = format!("http://{gateway}/v1/chat/stream");

        // Wait for the gateway to accept, then warm both paths.
        for _ in 0..100 {
            if client.get(format!("http://{gateway}/health/live")).send().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let warm = Plan { seconds: 1, ..plan.clone() };
        run_level(&client, Target::Upstream, &upstream_url, &warm, 8).await;
        run_level(&client, Target::Gateway, &gateway_url, &warm, 8).await;

        println!(
            "load: {} framing, {}s per level, upstream TTFT {} ms + {} tokens every {} ms, gateway cap {}, {} cores ({} threads each for upstream, gateway, clients)",
            plan.framing, plan.seconds, plan.ttft_ms, plan.tokens, plan.interval_ms, plan.global_cap, cores, share
        );
        println!(
            "| streams | ok/s via gateway | refused | broken | TTFT p50 / p99 direct (ms) | TTFT p50 / p99 via gateway (ms) | added p50 / p99 (ms) | heap KiB per open stream |"
        );
        println!("|---:|---:|---|---:|---|---|---|---:|");
        for &level in &plan.levels {
            let direct = run_level(&client, Target::Upstream, &upstream_url, &plan, level).await;
            let via = run_level(&client, Target::Gateway, &gateway_url, &plan, level).await;
            report(&plan, level, &direct, &via, smoke);
        }
    });
}

fn report(plan: &Plan, level: usize, direct: &Run, via: &Run, smoke: bool) {
    let mut d_ttft = direct.tally.ttft_us.clone();
    let mut g_ttft = via.tally.ttft_us.clone();
    d_ttft.sort_unstable();
    g_ttft.sort_unstable();
    let (d50, d99, g50, g99) = (pct(&d_ttft, 50.0), pct(&d_ttft, 99.0), pct(&g_ttft, 50.0), pct(&g_ttft, 99.0));
    let ok_per_s = via.tally.ok as f64 / via.wall.as_secs_f64();
    let refused: Vec<String> = via.tally.refused.iter().map(|(s, n)| format!("{n}×{s}")).collect();
    let refused = if refused.is_empty() { "0".to_string() } else { refused.join(", ") };
    let kib_per_stream = if via.max_open > 0 {
        via.peak_heap.saturating_sub(direct.peak_heap) as f64 / via.max_open as f64 / 1024.0
    } else {
        f64::NAN
    };
    println!(
        "| {level} | {ok_per_s:.0} | {refused} | {} | {d50:.1} / {d99:.1} | {g50:.1} / {g99:.1} | {:.1} / {:.1} | {kib_per_stream:.1} |",
        via.tally.broken + direct.tally.broken,
        g50 - d50,
        g99 - d99,
    );
    if smoke {
        assert!(via.tally.ok > 0 && direct.tally.ok > 0, "the smoke run completed no stream");
        return;
    }
    let line = json!({
        "at": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        "framing": plan.framing,
        "streams": level,
        "seconds": plan.seconds,
        "upstream": { "ttft_ms": plan.ttft_ms, "tokens": plan.tokens, "interval_ms": plan.interval_ms },
        "global_cap": plan.global_cap,
        "ok_per_s": ok_per_s,
        "ok": via.tally.ok,
        "refused": via.tally.refused.iter().map(|(s, n)| (s.to_string(), json!(n))).collect::<serde_json::Map<String, Value>>(),
        "broken": { "direct": direct.tally.broken, "gateway": via.tally.broken },
        "ttft_ms": { "direct_p50": d50, "direct_p99": d99, "gateway_p50": g50, "gateway_p99": g99 },
        "max_open_via_gateway": via.max_open,
        "heap_kib_per_open_stream": kib_per_stream,
    });
    let report = Path::new(env!("CARGO_MANIFEST_DIR")).join("target").join("load-report.jsonl");
    let _ = std::fs::create_dir_all(report.parent().unwrap());
    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&report) {
        let _ = writeln!(file, "{line}");
    }
}
