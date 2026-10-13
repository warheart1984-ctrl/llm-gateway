# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-passthrough | 1 | 0 | 20 | 4.00 | 121.346 | 215.795 | 0 |
| gateway-normalized | 1 | 0 | 20 | 4.00 | - | 190.028 | 0 |
| gateway-passthrough | 10 | 0 | 20 | 4.00 | 138.277 | 234.329 | 0 |
| gateway-normalized | 10 | 0 | 20 | 4.00 | - | 241.872 | 0 |
| gateway-passthrough | 25 | 0 | 17 | 3.40 | 244.061 | 415.747 | 3 |
| gateway-normalized | 25 | 0 | 16 | 3.20 | 397.849 | 399.421 | 4 |
