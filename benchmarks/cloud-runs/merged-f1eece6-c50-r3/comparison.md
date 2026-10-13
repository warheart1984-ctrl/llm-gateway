# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-normalized | 50 | 0 | 500 | 484.72 | 47.677 | 84.242 | 0 |
| gateway-passthrough | 50 | 0 | 500 | 530.37 | 47.467 | 84.004 | 0 |
| direct | 50 | 0 | 500 | 583.74 | 37.980 | 80.043 | 0 |
| gateway-normalized | 50 | 1 | 500 | 502.46 | 43.863 | 83.949 | 0 |
| gateway-passthrough | 50 | 1 | 500 | 522.11 | 43.931 | 83.975 | 0 |
| direct | 50 | 1 | 500 | 589.54 | 38.937 | 80.129 | 0 |
| gateway-normalized | 50 | 2 | 500 | 526.23 | 47.447 | 84.056 | 0 |
| gateway-passthrough | 50 | 2 | 500 | 533.27 | 43.949 | 83.952 | 0 |
| direct | 50 | 2 | 500 | 611.30 | 38.765 | 80.037 | 0 |
