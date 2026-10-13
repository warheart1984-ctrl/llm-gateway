# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-normalized | 50 | 0 | 1534 | 306.80 | 61.804 | 89.818 | 716 |
| gateway-passthrough | 50 | 0 | 1456 | 291.20 | 50.882 | 82.146 | 952 |
| direct | 50 | 0 | 3055 | 611.00 | 41.754 | 83.831 | 0 |
