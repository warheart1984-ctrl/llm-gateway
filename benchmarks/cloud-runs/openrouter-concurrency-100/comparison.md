# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-passthrough | 100 | 0 | 0 | 0.00 | 788.665 | 11.369 | 26699 |
| gateway-normalized | 100 | 0 | 9 | 1.80 | 635.002 | 10.515 | 28321 |
