# Cloud benchmark harness

This is the executable starting point for RFC-LLMGW-001. It measures a
deterministic provider directly and through an already-running gateway from
the same load-generator host. The existing `benches/load.rs` remains the
single-machine control and is intentionally unchanged.

## Topology

Use private addresses in one region:

| Node | Process | Required evidence |
|---|---|---|
| A | `cloud-harness` | client-observed timings and outcomes |
| B | `llm-gateway` | gateway configuration, `/v1/usage`, and operator metrics |
| C | `mock_upstream` | provider-side request counter at `/metrics` |
| D | PostgreSQL 16 | shared-ledger runs; record whether it is colocated |

Do not provision or load all nodes as a first action. Run the smoke command
below with one client, then compare direct, passthrough, and normalized paths.
Only after that succeeds should the full concurrency matrix be started.

## Build and start

Pin the checkout to the commit under test and use Rust 1.88. Start the
deterministic provider on C:

```bash
MOCK_ADDR=0.0.0.0:9000 MOCK_TOKEN_DELAY_MS=40 \
  cargo run --release --example mock_upstream
```

The provider's `GET /metrics` response is JSON, for example
`{"requests":42}`. It counts received chat requests, including requests that
the provider later rejects. Keep its address private; it is evidence plumbing,
not a public API.

Start B with a gateway model endpoint aimed at C. Use a durable ledger for
multi-replica work and keep the tenant key in the environment or a secret
file. No provider or production credentials belong in the benchmark bundle.

On A, configure the gateway key through the process environment and run the
smoke test:

```bash
export CLOUD_GATEWAY_API_KEY='the-isolated-benchmark-key'
cargo run --release --bin cloud_harness -- \
  --direct-url http://provider-private:9000/v1/chat/completions \
  --gateway-url http://gateway-private:8080/v1/chat/stream \
  --upstream-metrics-url http://provider-private:9000/metrics \
  --gateway-usage-url http://gateway-private:8080/v1/usage \
  --concurrency 1 --repetitions 1 --warmup-secs 0 --duration-secs 5 \
  --backend sqlite --output target/cloud-runs/smoke-1
```

For live-provider pressure, use a dedicated benchmark tenant with high but
finite policy limits. Keep authentication, admission, concurrency, budget,
ledger, framing, and evidence controls enabled; the higher limits are a
recorded benchmark policy, not a bypass. The harness also caps total attempts
per variant and backs off after `429`/`5xx` responses so immediate refusals do
not turn into an open-loop request storm:

```bash
export CLOUD_GATEWAY_API_KEY="$GATEWAY_KEY_BENCHMARK_MAIN"
cargo run --release --bin cloud_harness -- \
  --gateway-url http://gateway-private:8080/v1/chat/stream \
  --gateway-usage-url http://gateway-private:8080/v1/usage \
  --gateway-metrics-url http://gateway-private:8080/metrics \
  --model openrouter/gpt-4.1-mini --concurrency 50 --repetitions 1 \
  --warmup-secs 0 --duration-secs 5 --max-requests 500 \
  --refusal-backoff-ms 100 --output target/cloud-runs/openrouter-50
```

The metrics snapshots include gateway request/rejection counters, upstream
errors, active streams, process CPU time, and resident memory. Request records
also retain response status and retryable-error headers so gateway refusals can
be separated from upstream `5xx` signals. A provider-side `/metrics` endpoint
is still required for the refused-before-provider invariant; OpenRouter does
not provide that counter to this harness.

The runner never writes `CLOUD_GATEWAY_API_KEY`. It writes only sanitized
request metadata and outcomes. Missing provider or usage endpoints are marked
`not_measured`, not passed.

When `--max-requests` is omitted, each workload runs for the full requested
duration. Use the option only as an explicit safety cap for smoke tests or
bounded pressure runs. The `--output` directory must be new or empty; this
prevents stale metrics and evidence from being mixed into a fresh manifest.

## Full first experiment

After the smoke bundle is inspected and its `SHA256SUMS` verifies, run the
three initial repetitions with the RFC's workload settings:

```bash
cargo run --release --bin cloud_harness -- \
  --direct-url http://provider-private:9000/v1/chat/completions \
  --gateway-url http://gateway-private:8080/v1/chat/stream \
  --upstream-metrics-url http://provider-private:9000/metrics \
  --gateway-usage-url http://gateway-private:8080/v1/usage \
  --concurrency 50,200,500,1000 --repetitions 3 \
  --warmup-secs 60 --duration-secs 180 \
  --backend sqlite --output target/cloud-runs/gateway-vs-direct-1
```

Variant order is randomized from `--seed` (or `CLOUD_SEED`). Each request
record contains the variant, repetition, concurrency, status, outcome, TTFT,
full latency, response bytes, and worker identity. The manifest records the
source revision, Rust version, OS, architecture, CPU, vCPU count, memory,
configured hardware label, workload settings, and request shape.

## Evidence and governance

Every run produces:

- `manifest.json` — immutable run configuration and machine metadata;
- `requests.jsonl` — sanitized per-request raw observations;
- `summary.json` — throughput, p50/p95/p99 TTFT and latency, bytes, status
  counts, refusals, and peak active streams;
- `governance-results.json` — invariant evidence, including provider counter
  deltas and explicit `not_measured` states;
- `comparison.md` — scoped human-readable table;
- `SHA256SUMS` — hashes of every other bundle file.

The provider counter and usage baseline are sampled after each workload's
warmup and before its measured requests. A refused request can support the
authorization/budget boundary claim only when the counter endpoint is
configured and unchanged; the harness expects the counter delta to equal the
measured non-refused requests. `/v1/usage` is useful evidence for spend
deltas, but it is not a substitute for durable reservation rows; shared-ledger
reconciliation must still inspect PostgreSQL records before a run is
published.

The harness does not disable limits, bypass the ledger, or label missing data
as success. Direct, passthrough, and normalized measurements belong to
different capability classes; a transport-only comparison must not be written
up as an equivalent governed-gateway comparison.

If Docker cannot resolve package hosts, retain that result as an environment
limitation. Do not change the Dockerfile or remove TLS/checksum verification
to force a benchmark through; the direct HTTP path is useful functional
evidence but is not a passing container or three-node result.
