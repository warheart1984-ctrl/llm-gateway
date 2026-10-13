# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-passthrough | 1 | 0 | 3 | 0.60 | 1546.743 | 2470.335 | 0 |
| gateway-normalized | 1 | 0 | 3 | 0.60 | 897.129 | 1440.691 | 0 |
