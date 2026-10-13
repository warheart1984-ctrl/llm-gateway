# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-passthrough | 10 | 0 | 0 | 0.00 | 674.493 | 1172.636 | 12 |
| gateway-normalized | 10 | 0 | 42 | 8.40 | 653.300 | 1146.048 | 13 |
