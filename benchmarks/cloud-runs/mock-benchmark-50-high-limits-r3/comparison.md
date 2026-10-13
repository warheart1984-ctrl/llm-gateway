# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-normalized | 50 | 0 | 1674 | 162.33 | 213.554 | 272.374 | 6 |
| gateway-passthrough | 50 | 0 | 1802 | 169.34 | 221.196 | 269.141 | 4 |
| direct | 50 | 0 | 5138 | 509.21 | 45.261 | 90.373 | 0 |
| gateway-normalized | 50 | 1 | 1577 | 147.85 | 228.698 | 298.311 | 7 |
| gateway-passthrough | 50 | 1 | 1851 | 179.53 | 206.087 | 256.120 | 6 |
| direct | 50 | 1 | 5030 | 498.07 | 46.257 | 92.206 | 0 |
| gateway-normalized | 50 | 2 | 1708 | 164.36 | 206.970 | 271.432 | 7 |
| gateway-passthrough | 50 | 2 | 1626 | 148.84 | 221.835 | 256.789 | 164 |
| direct | 50 | 2 | 5194 | 514.38 | 46.269 | 90.473 | 0 |
