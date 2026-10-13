# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-normalized | 1 | 0 | 10 | 15.32 | 45.749 | 65.181 | 0 |
| gateway-passthrough | 1 | 0 | 10 | 14.97 | 45.549 | 67.402 | 0 |
| direct | 1 | 0 | 10 | 15.44 | 41.726 | 64.092 | 0 |
