# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-normalized | 1 | 0 | 10 | 15.06 | 45.152 | 66.213 | 0 |
| gateway-passthrough | 1 | 0 | 10 | 14.14 | 48.405 | 70.573 | 0 |
| direct | 1 | 0 | 10 | 15.19 | 42.768 | 63.323 | 0 |
