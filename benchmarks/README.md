# Benchmark evidence

Committed cloud evidence bundles were removed from this PR after review found
that their manifests were produced from a dirty checkout and claimed a
revision that did not contain the harness. They must not be used as
reproducible results.

The harness now refuses to start from a dirty checkout, records the exact
commit, dirty flag, and SHA-256 hash of the Git diff, records a provider-native
`--upstream-model` separately from the gateway registry model, redacts prompt
content, and requires explicit settlement and gateway rejection metrics.

After a clean, pinned rerun, place only newly verified bundles under
`benchmarks/cloud-runs/` and verify each bundle from the repository root with:

```bash
for run in benchmarks/cloud-runs/*; do
  (cd "$run" && sha256sum -c SHA256SUMS)
done
```

The exact finite benchmark tenant policy is checked in at
`config/tenants.yaml`. Provider credentials and gateway keys must be supplied
through environment variables and must not be copied into evidence.
