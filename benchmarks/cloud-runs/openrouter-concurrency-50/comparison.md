# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-passthrough | 50 | 0 | 0 | 0.00 | 867.271 | 3.289 | 25068 |
| gateway-normalized | 50 | 0 | 0 | 0.00 | - | 3.115 | 27348 |
