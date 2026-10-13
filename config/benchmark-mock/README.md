# Deterministic mock benchmark configuration

This checked-in configuration is the exact runtime configuration for the
local deterministic-provider benchmark described in `docs/cloud-benchmark.md`.
It keeps authentication, model authorization, concurrency admission, the
SQLite ledger, billing, settlement, and metrics enabled while giving the
isolated `benchmark` tenant finite high limits. The limit is intentionally
larger than the 50-client workload so the run measures gateway processing
rather than the tenant's normal concurrency ceiling.

The model registry routes `nvidia/mock-chat` to the local `mock_upstream`
process at `127.0.0.1:9000`. The gateway listens on `8081` and its operator
metrics listener on `9091` so it does not collide with a default local
instance. The only required secret is the test tenant key supplied through
`GATEWAY_KEY_BENCHMARK_MAIN`; never commit its value.
