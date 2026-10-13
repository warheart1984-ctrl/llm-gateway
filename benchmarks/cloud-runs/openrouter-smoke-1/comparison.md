# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-passthrough | 1 | 0 | 0 | 0.00 | 923.265 | 1630.412 | 0 |
| gateway-normalized | 1 | 0 | 5 | 1.00 | 641.271 | 1193.379 | 0 |
