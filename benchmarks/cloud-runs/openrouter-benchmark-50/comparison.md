# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-passthrough | 50 | 0 | 36 | 7.20 | 885.992 | 320.988 | 435 |
| gateway-normalized | 50 | 0 | 36 | 7.20 | 811.651 | 290.856 | 464 |
