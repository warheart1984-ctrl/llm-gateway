# Benchmark evidence

This directory contains the harness outputs produced while developing and
testing RFC-LLMGW-001. Each run is self-contained and includes a manifest,
raw per-request records, summaries, governance results, metrics snapshots,
and a `SHA256SUMS` file.

The primary capacity result is:

- `cloud-runs/mock-benchmark-50-high-limits-r3-ledger10s`: three repetitions
  at 50 clients with a dedicated finite high-limit tenant, governance enabled,
  and a 10-second SQLite admission timeout. It has zero 429/5xx responses,
  exact provider-counter reconciliation, passing billing checks, and passing
  SHA-256 verification.

The policy-pressure result is:

- `cloud-runs/mock-benchmark-50-v3`: the governed-limit run in which 681
  passthrough and 773 normalized requests were refused before reaching the
  mock provider. It measures policy behavior, not an unconstrained gateway
  ceiling.

The other directories preserve the live OpenRouter/Groq smoke and concurrency
runs, earlier mock runs, and diagnostic timeout runs. They are retained for
reproducibility and are not all suitable for headline performance claims.

Verify every evidence bundle from the repository root with:

```bash
for run in benchmarks/cloud-runs/*; do
  (cd "$run" && sha256sum -c SHA256SUMS)
done
```

The bundles contain sanitized request metadata and outcomes only. Provider
credentials and gateway keys were supplied through environment variables and
are not part of this directory.
