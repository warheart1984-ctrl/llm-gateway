# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-normalized | 50 | 0 | 1674 | 153.01 | 212.465 | 258.806 | 0 |
| gateway-passthrough | 50 | 0 | 1641 | 151.16 | 233.324 | 283.808 | 0 |
| direct | 50 | 0 | 5096 | 504.46 | 45.588 | 89.963 | 0 |
| gateway-normalized | 50 | 1 | 1722 | 148.28 | 186.942 | 260.389 | 0 |
| gateway-passthrough | 50 | 1 | 1826 | 178.42 | 214.264 | 267.067 | 0 |
| direct | 50 | 1 | 5181 | 512.73 | 45.816 | 90.890 | 0 |
| gateway-normalized | 50 | 2 | 1645 | 157.29 | 216.045 | 286.073 | 0 |
| gateway-passthrough | 50 | 2 | 1392 | 128.61 | 307.539 | 357.220 | 0 |
| direct | 50 | 2 | 4613 | 455.93 | 47.540 | 98.680 | 0 |
