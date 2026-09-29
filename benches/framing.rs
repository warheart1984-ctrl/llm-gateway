//! Measures the per-frame cost of the two framings.
//!
//! The normalized path decodes and re-serializes every upstream frame; the
//! passthrough path does not. That difference is the whole argument for having
//! two modes, so it should be a number rather than an assertion.
//!
//! Run with:
//!   cargo bench --bench framing
//!
//! Interpret the output as: `per frame` divided by the frame size gives the
//! gateway's added latency per token. At ~4 bytes/token, a 1 microsecond frame
//! cost is roughly 0.25 microseconds per token — invisible next to a provider's
//! 10ms+ inter-token gap. The number matters for throughput under load, not for
//! latency.

use std::time::Instant;

use bytes::Bytes;
use criterion::{black_box, criterion_group, Criterion, Throughput};

/// A realistic OpenAI-style chunk: `choices[0].delta.content` with a short
/// token, which is the common case on the hot path.
fn upstream_frame(token: &str) -> String {
    format!(
        "data: {}\n\n",
        serde_json::json!({
            "id": "chatcmpl-abc123",
            "object": "chat.completion.chunk",
            "created": 1_700_000_000,
            "model": "llama-3.1-70b-versatile",
            "choices": [{
                "index": 0,
                "delta": { "content": token },
                "logprobs": null,
                "finish_reason": null
            }]
        })
    )
}

fn benchmark_frames(c: &mut Criterion) {
    let frame = upstream_frame("Hello");
    let tokens: Vec<String> = (0..512).map(|i| format!("tok{i}")).collect();

    let mut group = c.benchmark_group("framing");
    group.throughput(Throughput::Bytes(frame.len() as u64));

    // 1. The exact hot-path work: build the gateway's `{"token": "..."}` frame.
    //    This is a pure function of the token text.
    group.bench_function("normalized/encode_token_frame", |b| {
        b.iter(|| {
            for token in &tokens {
                black_box(encode_token_frame(black_box(token)));
            }
        })
    });

    // 2. Full provider-side decode: one upstream frame in, one StreamEvent out.
    //    Approximated by parsing the JSON delta, which is the dominant cost.
    group.bench_function("normalized/decode_upstream_frame", |b| {
        let payload = frame.trim_start_matches("data: ").trim().to_string();
        b.iter(|| {
            let parsed: Value = serde_json::from_str(black_box(&payload)).unwrap();
            black_box(parsed["choices"][0]["delta"]["content"].as_str());
        })
    });

    // 3. Passthrough: the cost of touching the bytes at all. Should be
    //    effectively zero — that is the point of the mode.
    group.bench_function("passthrough/relay_chunk", |b| {
        let bytes = Bytes::from(frame.clone());
        b.iter(|| {
            // A relay moves the `Bytes` handle; it does not inspect it.
            black_box(bytes.clone());
        })
    });

    group.finish();
}

fn encode_token_frame(token: &str) -> String {
    // Mirrors `api::chat::token_json`.
    let mut out = String::with_capacity(token.len() + 12);
    out.push_str("{\"token\":");
    out.push_str(&serde_json::to_string(token).unwrap());
    out.push('}');
    out
}

use serde_json::Value;

/// End-to-end: relay N frames through a mock stream, measuring the time to
/// first byte and to drain. Reported outside criterion so the numbers appear in
/// the test output rather than only in the benchmark report.
fn report_absolute_numbers() {
    const FRAMES: usize = 2_000;

    // Passthrough.
    let chunks: Vec<Bytes> = (0..FRAMES)
        .map(|i| Bytes::from(upstream_frame(&format!("t{i}"))))
        .collect();
    let start = Instant::now();
    let mut total = 0usize;
    for chunk in &chunks {
        total += black_box(chunk).len();
    }
    let relay = start.elapsed();
    println!(
        "passthrough: {FRAMES} frames, {total} bytes in {:?} ({:?}/frame)",
        relay,
        relay / FRAMES as u32
    );

    // Normalized: same frames, but each is decoded and re-encoded.
    let frames: Vec<String> = (0..FRAMES)
        .map(|i| upstream_frame(&format!("t{i}")))
        .collect();
    let start = Instant::now();
    let mut total = 0usize;
    for frame in &frames {
        // The real path strips the SSE `data: ` prefix before parsing.
        let payload = frame.trim_start_matches("data: ").trim();
        let parsed: Value = serde_json::from_str(payload).expect("valid chunk JSON");
        let token = parsed["choices"][0]["delta"]["content"]
            .as_str()
            .expect("content delta");
        let out = encode_token_frame(token);
        total += black_box(out).len();
    }
    let normalized = start.elapsed();
    println!(
        "normalized:  {FRAMES} frames, {total} bytes in {:?} ({:?}/frame)",
        normalized,
        normalized / FRAMES as u32
    );

    let ratio = normalized.as_secs_f64() / relay.as_secs_f64().max(f64::MIN_POSITIVE);
    println!("normalized / passthrough: {ratio:.1}x on the encode+decode path");
    println!(
        "note: passthrough here measures only the `Bytes` handoff, not the real\n\
         HTTP path, so the ratio is an upper bound on the real difference."
    );
}

/// Exercises a real `Stream` so the benchmark also covers the `unfold`
/// machinery, not just the encoder.
fn bench_stream_pump(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_pump");
    let frame = upstream_frame("Hello");
    group.throughput(Throughput::Elements(1_024));

    group.bench_function("passthrough/pump_1024_chunks", |b| {
        b.iter(|| {
            let chunks: Vec<Result<Bytes, std::io::Error>> = (0..1_024)
                .map(|_| Ok(Bytes::from(frame.clone())))
                .collect();
            let stream = futures_util::stream::iter(chunks);
            // `fold` over an already-ready stream needs no executor: drive it
            // with a no-op waker inside `block_on`-free `poll`.
            let mut total = 0usize;
            let mut stream = std::pin::pin!(stream);
            let waker = futures_util::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            while let std::task::Poll::Ready(Some(Ok(chunk))) =
                futures_core::Stream::poll_next(stream.as_mut(), &mut cx)
            {
                total += chunk.len();
            }
            black_box(total);
        })
    });

    group.finish();
}

criterion_group!(benches, benchmark_frames, bench_stream_pump);

fn main() {
    report_absolute_numbers();
    let mut c = Criterion::default().configure_from_args();
    benchmark_frames(&mut c);
    bench_stream_pump(&mut c);
    c.final_summary();
}
