# Measuring Governed Streaming Overhead in an LLM Gateway

> **Evidence status (2026-10-13):** This is a provisional experiment draft.
> The previously committed cloud bundles were removed because their manifests
> came from a dirty checkout and did not identify the source tree that ran the
> harness. The numeric observations below are retained as historical notes,
> not as reproducible benchmark results. Rerun the experiment from a clean,
> pinned commit before citing any result.

## Abstract

This paper draft describes a planned controlled experiment measuring the performance and governance behavior of a Rust-based large-language-model (LLM) gateway when the upstream provider is deterministic and fast. It compares direct access to a mock streaming provider with gateway passthrough and normalized streaming while keeping authentication, model authorization, concurrency admission, durable reservations, settlement, billing, and evidence generation enabled. Historical observations are included only to document the experiment that must be rerun; they are not currently reproducible evidence. The intended result is a local, single-host baseline rather than a completed cloud or cross-provider benchmark.

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

The workload used 50 concurrent clients, a ten-second configured interval, zero warm-up seconds, three repetitions, and a maximum of 10,000 attempts per variant. The harness measured the synchronized interval directly; the reported throughput is successful completions divided by that measured interval. The harness backed off for 100 ms after HTTP 429 and 5xx responses. This cap and backoff prevented immediate governance refusals from becoming an open-loop request storm.

### 3.3 Measurements

For each request, the harness recorded status, outcome, time to first token (TTFT), full response latency, response size, and billing fields where available. It also recorded aggregate p50, p95, and p99 latency percentiles, successful completions, refusals by status, and peak concurrent streams.

The gateway exposed process CPU time, resident memory, active streams, stream counters, provider error counters, and governance rejection counters. The mock provider exposed a request counter. For each gateway variant, the provider counter was sampled before and after the measurement. The expected provider delta was the number of non-refused gateway requests.

The gateway usage endpoint was sampled before and after each workload. A bounded settlement barrier was added to the harness so that the usage value remained stable across repeated reads before billing reconciliation was evaluated. This was necessary because streaming completion counters and durable ledger corrections do not necessarily become externally visible at exactly the same instant.

### 3.4 Evidence and analysis

Each run emitted a manifest, raw per-request records, summary statistics, governance results, comparison table, and SHA-256 checksum file. The final run's manifest recorded the source revision, machine metadata, concurrency, duration, request cap, refusal backoff, and settlement wait.

No inferential statistical tests were applied. Results are reported descriptively across three repetitions, and the refusal population is kept separate from successful-completion throughput. A preliminary repeat with the default one-second SQLite admission timeout produced retryable 503 ledger timeouts; it was retained as diagnostic evidence but excluded from the capacity table. The reported run used the explicitly recorded ten-second timeout and produced no 429 or 5xx responses.

## 4. Results

The results in this section are historical observations from the removed
bundles. They are not accepted benchmark results until a clean, pinned rerun
produces manifests containing the current harness and exact tenant policy.

### 4.1 Throughput and latency

Table 1 summarizes the high-limit capacity run. Throughput is successful completions divided by the harness-measured workload interval; refusals are reported separately and are not counted as successful throughput. Aggregate throughput pools successful completions and measured seconds across the three repetitions.

**Table 1. Direct and gateway performance at 50 concurrent clients, three repetitions.**

| Variant | Successful completions | Aggregate throughput (req/s) | TTFT p50 (ms) | TTFT p95 (ms) | TTFT p99 (ms) | Full latency p50 (ms) | Full latency p95 (ms) | Full latency p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Direct mock provider | 14,890 | 491.0 | 46.175 | 62.505 | 90.845 | 92.515 | 154.282 | 193.477 |
| Gateway passthrough | 4,859 | 152.3 | 244.534 | 465.823 | 702.449 | 295.487 | 517.294 | 733.069 |
| Gateway normalized | 5,041 | 152.7 | 206.787 | 451.499 | 863.514 | 269.270 | 535.739 | 980.481 |

Across repetitions, aggregate gateway throughput was approximately 31% of the direct baseline for passthrough and 31% for normalized streaming. Run-level throughput ranged from 128.6 to 178.4 req/s for passthrough, 148.3 to 157.3 req/s for normalized streaming, and 455.9 to 512.7 req/s for direct access. These ranges show why the earlier 20-request live runs were smoke tests rather than provider benchmarks.

### 4.2 Refusals and provider-boundary evidence

The reported high-limit run generated no HTTP 429 or 5xx responses. Gateway rejection counters for authentication, policy, rate limit, concurrency, budget, and model were all zero at every measured gateway snapshot. In workload order, provider counter deltas were 1,674 normalized, 1,641 passthrough, 1,722 normalized, 1,826 passthrough, 1,645 normalized, and 1,392 passthrough. These sum to 5,041 normalized and 4,859 passthrough completions, matching Table 1 exactly. All provider-boundary invariants passed.

The earlier policy-limited run, `mock-benchmark-50-v3`, remains a valid governance-pressure result: it recorded 681 passthrough and 773 normalized concurrency refusals, and provider counters increased only for accepted requests. It is not an unconstrained capacity result. Separating these runs prevents policy refusal capacity from being confused with gateway processing capacity.

### 4.3 Resource observations

The final gateway metrics reached approximately 117,380 ms cumulative process CPU time and 34.3 MiB resident memory by the last passthrough snapshot; peak client-observed concurrency was 50. Because variants ran sequentially in one process, these are process snapshots rather than isolated per-variant costs. The ledger pending-closing gauge returned to zero after the final settlement drain, with no backlog refusals or ledger sweep failures.

The host exposed four virtual CPUs. The process was therefore not demonstrably CPU-saturated during the ten-second runs. The experiment did not collect heap-per-stream measurements, so memory cost per active stream remains unreported.

### 4.4 Accounting and evidence integrity

All six gateway workloads passed usage-versus-per-request accounting reconciliation. The final cumulative usage was 3,754,864,000 nano-USD, and the provider counter matched every accepted gateway request. The provider-boundary, accounting, and evidence-integrity invariants all passed for both gateway variants. The SHA-256 checksum file verified every emitted bundle file, including the manifest, raw request records, summaries, metrics snapshots, and governance results.

## 5. Discussion

### 5.1 Main finding

Under the measured local high-limit configuration, the gateway sustained approximately 152 successful requests per second at 50 concurrent clients while keeping governance and evidence controls enabled. The direct mock-provider path sustained 491 successful requests per second. The observed gateway cost was material in throughput terms, and the gateway's median latency was higher because admission, ledger, settlement, and framing work were exercised on every request.

The experiment's answer to RQ1 is consequently conditional: the gateway added measurable overhead, but the magnitude depended on the streaming mode, ledger contention, and run-to-run scheduling. RQ2 cannot be answered as a stable normalized-versus-passthrough ranking from only three repetitions; their aggregate throughputs were nearly equal while their run-level variance differed. RQ3 is supported by the policy-limited provider counters and by the zero-refusal high-limit run. RQ4 is supported by the passing billing and checksum checks after settlement synchronization.

### 5.2 Interpretation of normalized-streaming overhead

Normalized streaming requires the gateway to parse provider events and produce its own normalized framing. Passthrough streaming can forward provider framing with less transformation. In the pooled raw records, normalized TTFT was 206.8 ms at p50 versus 244.5 ms for passthrough, 37.7 ms lower; normalized p95 and p99 TTFT were 451.5 and 863.5 ms versus 465.8 and 702.4 ms for passthrough. These descriptive differences do not isolate the contribution of any one subsystem.

The final run did not establish a consistent normalized-streaming advantage or penalty relative to passthrough: normalized throughput was 152.7 req/s and passthrough was 152.3 req/s in aggregate, while passthrough had a wider run-level range. Queueing, event scheduling, SQLite reservation activity, and stream timing interact under concurrent load, so a larger repeated matrix is needed to isolate framing cost.

### 5.3 Governance is part of the performance result

The throughput result cannot be separated from governance in this experiment. The gateway did not win a higher number by bypassing admission checks, and refused requests did not reach the provider. A transport-only comparison would report the direct baseline and forwarding latency, but it would omit the cost and operational value of authorization, concurrency control, reservation, settlement, billing, and evidence generation.

The appropriate claim is therefore not that the gateway is equivalent to a direct connection. The stronger and more useful claim is narrower: in this configuration, the gateway maintained its governance boundary while completing about 150 streaming requests per second against a fast deterministic provider.

### 5.4 Settlement synchronization as a measurement requirement

An earlier run showed that a usage snapshot could be captured before durable reservation corrections were visible, producing a temporary billing mismatch even though the ledger later converged. The final harness includes a bounded settlement wait and verifies usage stability before evaluating reconciliation. This is an important methodological result: a benchmark for a financially governed service must define when a run is considered settled. Sampling only transport counters is insufficient.

## 6. Limitations

Several limitations restrict the scope of the conclusions.

1. **Single-host topology.** The experiment did not use independent cloud instances for the load generator, gateway, and provider. Network latency, bandwidth, CPU contention, and failure domains were therefore not representative of a three-node deployment.
2. **Descriptive repetitions.** Three valid repetitions were run, but the sample is still too small for confidence intervals or broad claims about run-to-run variance.
3. **SQLite only.** The experiment used a durable SQLite ledger. It did not compare in-memory and PostgreSQL backends or measure distributed settlement contention.
4. **Fast mock provider.** A 1 ms/token deterministic provider isolates gateway work but is not a model of a real inference service. Its timing should not be interpreted as user-visible LLM latency.
5. **One concurrency level.** Only 50 concurrent clients were tested. The experiment does not establish behavior at 200, 500, or 1,000 clients.
6. **Incomplete resource instrumentation.** Heap-per-active-stream, network utilization, detailed admission latency, and settlement lag were not captured.
7. **Live-provider smoke tests.** The OpenRouter and Groq runs were small smoke/concurrency checks, not provider-capacity benchmarks. Their results should not be pooled with the deterministic mock-provider capacity result.

## 7. Future Work

The next experiment should deploy the load generator, gateway, and mock provider on separate private nodes in one cloud region. It should first repeat the smoke test, then run at 50, 200, 500, and 1,000 concurrency with at least three repetitions per valid configuration. The same request payload, provider schedule, hardware labels, source revision, and evidence checks should be retained.

The ledger study should compare SQLite and PostgreSQL while recording admission latency, open reservations, settlement queue depth, settlement lag, and durable row reconciliation. A separate run should vary the mock provider delay to distinguish gateway saturation from provider serialization. The resource instrumentation should be extended with heap-per-stream and network counters. Only after these steps should the gateway be compared with another gateway, and then only within a clearly defined transport-only or capability-matched comparison class.

## 8. Conclusion

This experiment measured a governed streaming gateway against a deterministic mock provider. In the three-repetition high-limit run at 50 concurrent clients, direct access completed 491.0 requests per second in aggregate, gateway passthrough completed 152.3 requests per second, and gateway normalized streaming completed 152.7 requests per second. Pooled median TTFT was 46.2 ms direct, 244.5 ms passthrough, and 206.8 ms normalized. The separate policy-limited run demonstrated refusals before the provider boundary; the high-limit run demonstrated accepted-request throughput without policy refusals. Billing reconciled after settlement synchronization, and the evidence bundles verified by SHA-256.

The result is a credible local baseline, not a final cloud performance claim. It demonstrates that the gateway can process about 150 governed streaming completions per second against a fast upstream while preserving its admission and accounting controls. A multi-node, multi-repetition benchmark with PostgreSQL comparison is required to determine how this behavior generalizes beyond the local test environment.

## References and Evidence

1. RFC-LLMGW-001, *Cloud Performance & Governance Proof Harness*, version 0.1, 2026-10-13. Internal project specification supplied with the experiment.
2. `llm-gateway`, harness implementation in `src/bin/cloud_harness.rs`.
3. `llm-gateway`, exact benchmark tenant policy in `config/tenants.yaml`.
4. `llm-gateway`, rerun procedure in `docs/cloud-benchmark.md`.
