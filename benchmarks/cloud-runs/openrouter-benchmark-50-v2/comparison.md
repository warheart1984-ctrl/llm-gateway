# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-passthrough | 50 | 0 | 35 | 7.00 | 867.128 | 309.513 | 458 |
| gateway-normalized | 50 | 0 | 40 | 8.00 | 907.493 | 333.847 | 432 |
