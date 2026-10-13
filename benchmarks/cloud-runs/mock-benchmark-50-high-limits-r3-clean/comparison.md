# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-normalized | 50 | 0 | 1742 | 170.21 | 200.907 | 265.528 | 5 |
| gateway-passthrough | 50 | 0 | 1816 | 177.41 | 199.581 | 255.175 | 2 |
| direct | 50 | 0 | 5078 | 502.17 | 45.778 | 91.465 | 0 |
| gateway-normalized | 50 | 1 | 1599 | 153.25 | 229.577 | 301.391 | 5 |
| gateway-passthrough | 50 | 1 | 1780 | 161.56 | 214.991 | 269.002 | 5 |
| direct | 50 | 1 | 5286 | 523.83 | 45.284 | 88.162 | 0 |
| gateway-normalized | 50 | 2 | 1722 | 167.52 | 209.573 | 271.721 | 8 |
| gateway-passthrough | 50 | 2 | 1788 | 161.29 | 205.301 | 273.953 | 4 |
| direct | 50 | 2 | 5382 | 533.41 | 44.823 | 87.585 | 0 |
