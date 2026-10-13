# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-normalized | 50 | 0 | 500 | 264.26 | 83.885 | 122.508 | 0 |
| gateway-passthrough | 50 | 0 | 500 | 431.01 | 54.884 | 92.729 | 0 |
| direct | 50 | 0 | 500 | 596.94 | 39.158 | 80.187 | 0 |
| gateway-normalized | 50 | 1 | 500 | 293.33 | 64.943 | 104.266 | 0 |
| gateway-passthrough | 50 | 1 | 500 | 412.06 | 53.384 | 94.369 | 0 |
| direct | 50 | 1 | 500 | 617.33 | 38.195 | 80.030 | 0 |
| gateway-normalized | 50 | 2 | 500 | 361.94 | 50.249 | 88.089 | 0 |
| gateway-passthrough | 50 | 2 | 500 | 215.39 | 80.410 | 120.065 | 0 |
| direct | 50 | 2 | 500 | 590.32 | 39.152 | 80.324 | 0 |
