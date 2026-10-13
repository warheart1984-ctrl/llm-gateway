# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-passthrough | 1 | 0 | 3 | 0.60 | 1024.582 | 1751.039 | 0 |
| gateway-normalized | 1 | 0 | 3 | 0.60 | 1706.785 | 2594.041 | 0 |
| gateway-passthrough | 10 | 0 | 17 | 3.40 | 848.278 | 1495.536 | 3 |
| gateway-normalized | 10 | 0 | 16 | 3.20 | 915.474 | 1496.088 | 4 |
| gateway-passthrough | 25 | 0 | 9 | 1.80 | 841.895 | 374.925 | 11 |
| gateway-normalized | 25 | 0 | 9 | 1.80 | 828.926 | 353.542 | 11 |
