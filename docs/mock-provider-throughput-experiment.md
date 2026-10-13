# Measuring Governed Streaming Overhead in an LLM Gateway

> **Evidence status (2026-10-13):** This is a provisional local experiment
> report. The current clean bundles record source revision `9a3ab2c`, Rust
> `1.88.0`, and `source_dirty: false`. The short comparison run is reproducible
> evidence; the separate 30-second pressure run is diagnostic evidence because
> it measures settlement backlog and active-stream bounds under sustained load.

## Abstract

This paper reports a controlled local experiment measuring the performance and governance behavior of a Rust-based large-language-model (LLM) gateway when the upstream provider is deterministic and fast. It compares direct access to a mock streaming provider with gateway passthrough and normalized streaming while keeping authentication, model authorization, concurrency admission, durable reservations, settlement, billing, and evidence generation enabled. The result is a local, single-host baseline rather than a completed cloud or cross-provider benchmark.

**Keywords:** LLM gateway, streaming inference, governance, admission control, billing reconciliation, performance measurement, reproducibility

## 1. Introduction

LLM gateways add capabilities that are not present in a direct client-to-provider connection. These capabilities may include tenant authentication, model allowlists, concurrency limits, budget reservations, durable settlement, usage accounting, stream normalization, and evidence generation. Each control can add latency or reduce throughput, but removing the controls would invalidate the gateway's primary operational purpose.

The experiment reported here was designed to answer a narrow question: how much performance does a governed gateway add when the upstream provider is not the bottleneck? A deterministic mock provider was used so that model inference variability, provider rate limits, and paid API quotas did not dominate the result. The direct path served as a transport baseline, and two gateway paths measured passthrough and normalized streaming.

The experiment followed RFC-LLMGW-001's governing principle: no performance claim should exceed its measured evidence. In particular, a request refusal was not treated as a successful performance outcome, a configured duration was not used as a substitute for the measured workload interval, and a gateway response status alone was not treated as proof that the upstream provider had not been contacted. Provider-side request counters were used to verify the admission boundary.

The contribution of this report is therefore twofold:

1. It provides a reproducible baseline for direct, passthrough, and normalized streaming on the current implementation.
2. It demonstrates a measurement procedure in which governance, provider-boundary evidence, billing reconciliation, and cryptographic evidence manifests are evaluated alongside latency and throughput.

## 2. Research Questions

The experiment addressed four research questions.

**RQ1.** What throughput and latency does the gateway achieve relative to direct access to the same deterministic provider?

**RQ2.** Does normalized streaming impose additional overhead relative to raw passthrough streaming?

**RQ3.** When the gateway refuses requests under load, do those requests reach the upstream provider?

**RQ4.** Can financial accounting and evidence packaging remain internally consistent during the performance run?

## 3. Experimental Method

### 3.1 Experimental design

The experiment used three logical processes:

- a load harness issuing streaming requests;
- the `llm-gateway` service; and
- a deterministic mock upstream provider.

The processes ran on the same host for this first measurement. This arrangement emulated the request paths and evidence interfaces but did not provide the network isolation or independent resource boundaries of the RFC's proposed three-node cloud topology. The run should therefore be interpreted as a controlled local baseline, not as a completed cloud deployment.

The three measured variants were:

1. **Direct:** load harness to mock provider.
2. **Gateway passthrough:** load harness to gateway passthrough endpoint, then to the mock provider.
3. **Gateway normalized:** load harness to gateway normalized streaming endpoint, then to the mock provider.

The variant order was randomized by the harness. The reported capacity run used a dedicated tenant with finite but intentionally non-binding limits: 2,048 concurrent streams, 100,000 requests per minute, 10,000,000 tokens per minute, a 1,024-token output ceiling, and a 50,000,000,000 nano-USD daily budget. The gateway global stream cap was also set to 2,048. Authentication, model authorization, admission, the SQLite ledger, billing, settlement, framing, and evidence generation remained enabled. The SQLite ledger admission timeout was set to 10 seconds and recorded in the process configuration so transient ledger contention would not be confused with a capacity refusal.

### 3.2 Provider and workload

The provider emitted a deterministic server-sent event stream. Its token delay was configured to 1 ms per token, making the provider substantially faster and more repeatable than a live model API. The request shape was held constant:

- one user message: `cloud benchmark request`;
- streaming enabled;
- model identifier: `mock/chat`; and
- requested maximum output: 64 tokens.

The reproducible comparison workload used 50 concurrent clients, a ten-second configured interval, zero warm-up seconds, three repetitions, and a maximum of 500 requests per variant. The harness measured the synchronized interval directly; the reported throughput is successful completions divided by that measured interval. The harness backed off for 100 ms after HTTP 429 and 5xx responses. A separate 30-second pressure run allowed up to 20,000 requests per variant to test sustained behavior.

### 3.3 Measurements

For each request, the harness recorded status, outcome, time to first token (TTFT), full response latency, response size, and billing fields where available. It also recorded aggregate p50, p95, and p99 latency percentiles, successful completions, refusals by status, and peak concurrent streams.

The gateway exposed process CPU time, resident memory, active streams, stream counters, provider error counters, and governance rejection counters. The mock provider exposed a request counter. For each gateway variant, the provider counter was sampled before and after the measurement. The expected provider delta was the number of non-refused gateway requests.

The gateway usage endpoint was sampled before and after each workload. A bounded settlement barrier was added to the harness so that the usage value remained stable across repeated reads before billing reconciliation was evaluated. This was necessary because streaming completion counters and durable ledger corrections do not necessarily become externally visible at exactly the same instant.

### 3.4 Evidence and analysis

Each run emitted a manifest, raw per-request records, summary statistics, governance results, comparison table, and SHA-256 checksum file. The comparison manifest recorded the exact source revision, clean-tree status, Rust `1.88.0`, machine metadata, concurrency, duration, request cap, refusal backoff, and settlement wait. The comparison table also records reserved and billed nano-USD and identifies whether accounting is provider-usage- or reservation-settlement-based.

No inferential statistical tests were applied. Results are reported descriptively across three repetitions, and the refusal population is kept separate from successful-completion throughput. A preliminary repeat with the default one-second SQLite admission timeout produced retryable 503 ledger timeouts; it was retained as diagnostic evidence but excluded from the capacity table. The reported run used the explicitly recorded ten-second timeout and produced no 429 or 5xx responses.

## 4. Results

The results in this section come from the clean, pinned comparison bundle
`benchmarks/cloud-runs/merged-f1eece6-c50-r3` and its SHA-256 manifest.

### 4.1 Throughput and latency

Table 1 summarizes the high-limit capacity run. Throughput is successful completions divided by the harness-measured workload interval; refusals are reported separately and are not counted as successful throughput. Aggregate throughput pools successful completions and measured seconds across the three repetitions.

**Table 1. Direct and gateway performance at 50 concurrent clients, three repetitions.**

| Variant | Successful completions | Aggregate throughput (req/s) | TTFT p50 (ms) | TTFT p95 (ms) | TTFT p99 (ms) | Full latency p50 (ms) | Full latency p95 (ms) | Full latency p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Direct mock provider | 1,500 | 594.86 | 38.561 | 40.9 | 41.7 | 80.1 | 85.0 | 87.8 |
| Gateway passthrough | 1,500 | 528.58 | 45.116 | 61.2 | 171.9 | 84.0 | 108.2 | 209.1 |
| Gateway normalized | 1,500 | 504.47 | 46.329 | 64.8 | 181.3 | 84.1 | 115.5 | 264.8 |

Across the three capped repetitions, passthrough throughput ranged from 522.1 to 533.3 req/s and normalized throughput ranged from 484.7 to 526.2 req/s. Direct throughput ranged from 583.7 to 611.3 req/s. Thus the gateway achieved 85–89% of direct throughput in this short, clean comparison. These results should not be interpreted as a sustained ceiling: the 30-second pressure run below exercised a different regime.

### 4.2 Refusals and provider-boundary evidence

The clean comparison generated no HTTP 429 or 5xx responses. Gateway rejection counters for authentication, policy, rate limit, concurrency, budget, and model were zero for every workload. Each variant completed 500/500 requests in each repetition, and provider counter deltas matched accepted gateway requests. All provider-boundary invariants passed.

The separate 30-second pressure run is reported independently below with sampled stream, open-reservation, and settlement-queue gauges. It is governed-pressure evidence, not an unconstrained capacity result.

### 4.3 Resource observations

The fixed 30-second bundle `benchmarks/cloud-runs/merged-99f6de9-c50-30s` includes 670 resource samples at approximately 100 ms intervals in `resource-samples.csv`. Across the direct, passthrough, and normalized legs, the maximum observed active-stream gauge was 48, maximum durable open reservations was 512, and maximum pending settlement queue depth was 483. The gateway rejection counter stayed at zero. The settlement barrier returned the durable gauges to zero after each governed workload.

The host exposed four virtual CPUs. The process was therefore not demonstrably CPU-saturated during the ten-second runs. The experiment did not collect heap-per-stream measurements, so memory cost per active stream remains unreported.

### 4.4 Accounting and evidence integrity

In the fixed 30-second rerun, normalized streaming completed 12,441 requests and reconciled 4,478,760 nano-USD ($0.00447876) of billed usage against 9,081,930 nano-USD reserved. Passthrough completed 13,152 requests and reconciled 9,600,960 nano-USD ($0.00960096), equal to its reservation settlement. The direct mock leg has no gateway billing. A successful passthrough request with no billed settlement or usage delta now fails reconciliation; the bundle's SHA-256 checksum file verifies every emitted file, including the resource series.

### 4.5 Fixed 30-second mock rerun

The pinned source was `99f6de954031773c3a90376798c6cb1c95f63321`, the tree was clean, and Rust `1.88.0` was recorded in the manifest. With 50 clients, one 30-second repetition per variant, and a 20,000-request safety cap, normalized streaming sustained 410.66 req/s with TTFT p50/p95 of 66.63/155.34 ms; passthrough sustained 436.62 req/s with TTFT p50/p95 of 55.73/154.11 ms; direct sustained 605.62 req/s with TTFT p50/p95 of 38.79/43.22 ms. All 43,816 requests completed successfully, with zero governance refusals and passing settlement reconciliation. These are deterministic mock-provider results only.

## 5. Discussion

### 5.1 Main finding

Under the short, capped local comparison, the gateway completed 504–529 req/s at 50 concurrent clients versus 595 req/s direct, with zero failures and full settlement/accounting reconciliation. The observed gateway cost was material in throughput terms, and the gateway's latency was modestly higher because admission, ledger, settlement, and framing work were exercised on every request. The fixed-concurrency pressure run kept active streams bounded while exposing the settlement backlog as a separate operational metric.

The experiment's answer to RQ1 is conditional: the gateway added measurable overhead, but the magnitude depended on the streaming mode, ledger contention, and run duration. RQ2 cannot be treated as a universal normalized-versus-passthrough ranking; in the short run they were close, while the pressure run exposed transient governance backpressure. RQ3 is supported by provider counters showing that classified refusals did not cross the provider boundary. RQ4 is supported by settlement-aware accounting and checksum checks.

### 5.2 Interpretation of normalized-streaming overhead

Normalized streaming requires the gateway to parse provider events and produce its own normalized framing. Passthrough streaming can forward provider framing with less transformation. In the pooled raw records, normalized TTFT was 206.8 ms at p50 versus 244.5 ms for passthrough, 37.7 ms lower; normalized p95 and p99 TTFT were 451.5 and 863.5 ms versus 465.8 and 702.4 ms for passthrough. These descriptive differences do not isolate the contribution of any one subsystem.

The clean short run did not establish a consistent normalized-streaming advantage or penalty relative to passthrough: normalized throughput averaged 504.5 req/s and passthrough 528.6 req/s, with overlapping run-level behavior. Queueing, event scheduling, SQLite reservation activity, and stream timing interact under concurrent load, so longer runs must be reported separately from the capped comparison.

### 5.3 Governance is part of the performance result

The throughput result cannot be separated from governance in this experiment. The gateway did not win a higher number by bypassing admission checks, and refused requests did not reach the provider. A transport-only comparison would report the direct baseline and forwarding latency, but it would omit the cost and operational value of authorization, concurrency control, reservation, settlement, billing, and evidence generation.

The appropriate claim is therefore not that the gateway is equivalent to a direct connection. The narrower evidence-backed claim is: in the short local comparison, the gateway completed about 500–530 governed streaming requests per second at 50 clients, roughly 85–89% of direct, with zero failures and full accounting reconciliation. The fixed 30-second rerun sustained 410.66 normalized and 436.62 passthrough req/s versus 605.62 direct, with active streams capped below 50 and no governance refusals.

### 5.4 Settlement synchronization as a measurement requirement

An earlier run showed that a usage snapshot could be captured before durable reservation corrections were visible, producing a temporary billing mismatch even though the ledger later converged. The harness includes a bounded settlement wait and verifies usage stability before evaluating reconciliation. It distinguishes normalized billed usage from passthrough reservation settlement and fails when a successful request has no corresponding billed settlement or usage delta. This is an important methodological result: a benchmark for a financially governed service must define when a run is considered settled.

## 6. Limitations

Several limitations restrict the scope of the conclusions.

1. **Single-host topology.** The experiment did not use independent cloud instances for the load generator, gateway, and provider. Network latency, bandwidth, CPU contention, and failure domains were therefore not representative of a three-node deployment.
2. **Short comparison interval.** The committed comparison is capped at 500 requests per variant. The 30-second pressure run is reported separately as sustained-load evidence.
3. **SQLite only.** The experiment used a durable SQLite ledger. It did not compare in-memory and PostgreSQL backends or measure distributed settlement contention.
4. **Fast mock provider.** A 1 ms/token deterministic provider isolates gateway work but is not a model of a real inference service. Its timing should not be interpreted as user-visible LLM latency.
5. **One concurrency level.** Only 50 concurrent clients were tested. The experiment does not establish behavior at 200, 500, or 1,000 clients.
6. **Incomplete resource instrumentation.** The pressure run captures stream slots, durable open reservations, and pending settlements; heap-per-active-stream, network utilization, detailed admission latency, and per-reservation settlement lag remain unreported.
7. **Live-provider smoke tests.** The OpenRouter and Groq runs were small smoke/concurrency checks, not provider-capacity benchmarks. Their results should not be pooled with the deterministic mock-provider capacity result.

## 7. Future Work

The next experiment should deploy the load generator, gateway, and mock provider on separate private nodes in one cloud region. It should first repeat the smoke test, then run at 50, 200, 500, and 1,000 concurrency with at least three repetitions per valid configuration. The same request payload, provider schedule, hardware labels, source revision, and evidence checks should be retained.

The ledger study should compare SQLite and PostgreSQL while recording admission latency, open reservations, settlement queue depth, settlement lag, and durable row reconciliation. A separate run should vary the mock provider delay to distinguish gateway saturation from provider serialization. The resource instrumentation should be extended with heap-per-stream and network counters. Only after these steps should the gateway be compared with another gateway, and then only within a clearly defined transport-only or capability-matched comparison class.

## 8. Conclusion

This experiment measured a governed streaming gateway against a deterministic mock provider. In the clean three-repetition capped run at 50 concurrent clients, direct access averaged 594.9 requests per second, gateway passthrough 528.6 requests per second, and gateway normalized streaming 504.5 requests per second. The fixed 30-second rerun at the same client count measured 605.62 direct, 436.62 passthrough, and 410.66 normalized req/s. Every request in the fixed rerun succeeded, settlement and accounting reconciled, active streams stayed below 50, and the evidence bundle verified by SHA-256. These are deterministic mock-provider results only.

The result is a credible local baseline, not a final cloud performance claim. It demonstrates that the gateway can process roughly 85–89% of direct throughput in a short capped comparison while preserving its admission and accounting controls. A multi-node, multi-repetition benchmark with PostgreSQL comparison and longer settled intervals is required to determine how this behavior generalizes beyond the local test environment.

## References and Evidence

1. RFC-LLMGW-001, *Cloud Performance & Governance Proof Harness*, version 0.1, 2026-10-13. Internal project specification supplied with the experiment.
2. `llm-gateway`, harness implementation in `src/bin/cloud_harness.rs`.
3. `llm-gateway`, exact benchmark tenant policy in `config/tenants.yaml`.
4. `llm-gateway`, rerun procedure in `docs/cloud-benchmark.md`.
