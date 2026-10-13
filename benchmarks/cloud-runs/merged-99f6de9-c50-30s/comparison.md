# Cloud benchmark comparison

Results are scoped to the manifest and raw request records in this run. No result is a production or cross-gateway claim.

| Variant | Concurrency | Repetition | Successful | Throughput/s | TTFT p50 ms | Latency p50 ms | Reserved nano-USD | Billed nano-USD | Accounting basis | Refused |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| gateway-normalized | 50 | 0 | 12441 | 410.66 | 66.627 | 104.125 | 9081930 | 4478760 | provider-usage-billed | 0 |
| gateway-passthrough | 50 | 0 | 13152 | 436.62 | 55.732 | 93.636 | 9600960 | 9600960 | reservation-billed | 0 |
| direct | 50 | 0 | 18223 | 605.62 | 38.792 | 80.626 | 0 | 0 | - | 0 |
