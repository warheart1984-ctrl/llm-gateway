# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-normalized | 50 | 0 | 1551 | 310.20 | 50.594 | 84.204 | 879 |
| gateway-passthrough | 50 | 0 | 1654 | 330.80 | 50.229 | 84.796 | 848 |
| direct | 50 | 0 | 3057 | 611.40 | 41.332 | 83.820 | 0 |
