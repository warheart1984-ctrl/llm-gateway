# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-normalized | 50 | 0 | 1474 | 294.80 | 59.958 | 88.016 | 773 |
| gateway-passthrough | 50 | 0 | 1748 | 349.60 | 54.004 | 88.395 | 681 |
| direct | 50 | 0 | 3039 | 607.80 | 41.470 | 83.848 | 0 |
