# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|
| gateway-passthrough | 1 | 0 | 71 | 4.73 | 118.780 | 196.159 | 0 |
| gateway-normalized | 1 | 0 | 74 | 4.93 | 200.002 | 192.662 | 0 |
| gateway-passthrough | 10 | 0 | 100 | 6.67 | 139.259 | 220.351 | 0 |
| gateway-normalized | 10 | 0 | 100 | 6.67 | 277.164 | 213.012 | 0 |
| gateway-passthrough | 25 | 0 | 83 | 5.53 | 278.088 | 353.521 | 17 |
| gateway-normalized | 25 | 0 | 82 | 5.47 | 419.621 | 352.303 | 18 |
