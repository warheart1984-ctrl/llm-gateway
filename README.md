# llm-gateway

A streaming, multi-tenant, governed LLM chat gateway in Rust. One SSE front door
(`POST /v1/chat/stream`) routing to **Groq**, **OpenRouter**, and **NVIDIA NIM**
(cloud or self-hosted).

```
client ──▶ API (SSE framing) ──▶ governance (auth → policy → limits) ──▶ router ──▶ provider adapter ──▶ vendor
                                        all pre-flight, before the first byte
```

## What it does

- **Two framings.** `normalized` (default) decodes provider frames and
  re-emits them under a frozen v1 contract, so behaviour is identical across
  vendors and clients never see a vendor quirk. `passthrough` relays upstream
  bytes verbatim with zero per-frame work. See [Framing](#framing).
- **Governed, not just authorized.** Per-tenant rate limits, token limits,
  concurrency caps, and a day-bucketed spend budget. Cost is *reserved* before
  the upstream connection opens and settled with real usage afterwards, so a
  stream that dies at 20% still pays for its prompt.
- **Config-driven catalogue.** No model id, provider, or upstream URL is
  compiled in. `models.yaml` and `tenants.yaml` hot-reload on mtime change.
- **Versioned contract.** Frame shapes are frozen under `v1`; breaking changes
  get a new prefix, not a new meaning.
- **Quiet hot path.** No per-token logging, no per-token high-cardinality
  metrics. One summary line and one metrics update per stream.

## Quick start

```bash
# 1. Point at your config (default: ./config)
export LLM_GATEWAY_CONFIG_DIR=./config

# 2. Set tenant keys and at least one provider key
setx GATEWAY_KEY_ACME_MAIN   gwk_live_...
setx GATEWAY_KEY_JARVIS_MAIN gwk_live_...
setx GROQ_API_KEY            gsk_...

# 3. Run
cargo run --release
```

```bash
curl -N http://localhost:8080/v1/chat/stream \
  -H 'x-api-key: gwk_live_...' \
  -H 'content-type: application/json' \
  -d '{
        "model": "groq/gpt-oss-120b",
        "messages": [{"role": "user", "content": "explain SSE in one sentence"}],
        "stream": true,
        "params": {"max_tokens": 200}
      }'
```

```
event: start
data: {"request_id":"...","model":"groq/gpt-oss-120b","tenant":"acme",...}

data: {"token":"SSE"}

data: {"token":" is"}

data: {"token":" ..."}

event: end
data: {"finish_reason":"stop","usage":{"prompt_tokens":18,"completion_tokens":12,...},"cost_nano_usd":9900,...}

data: [DONE]
```

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/chat/stream` | SSE chat completion |
| `GET` | `/v1/models` | Catalogue, filtered to the caller's allowlist |
| `GET` | `/v1/usage` | Caller's own quota and spend |
| `POST` | `/v1/admin/registry/reload` | Re-read `models.yaml` / `tenants.yaml` (admin scope) |
| `GET` | `/health/live` | Liveness |
| `GET` | `/health/ready` | Readiness; 503 if routing cannot work |
| `GET` | `/metrics` | Prometheus text exposition |

## Framing

Both modes run identical governance before the first byte, and both propagate
client cancellation upstream (dropping the response body drops the reqwest
response, which closes the connection).

### `normalized` — the default

Provider frames are decoded and re-emitted under the gateway's own contract:

| Event | Payload | Notes |
|---|---|---|
| `start` | `{"request_id","model","tenant","max_output_tokens","metadata"}` | synthetic, sent immediately |
| *(unnamed)* | `{"token": "..."}` | the only unnamed event type |
| `reasoning` | `{"reasoning": "..."}` | chain of thought, never mixed into `token` |
| `tool` | `{"index","id","name","arguments"}` | tool-call deltas, forwarded intact |
| `meta` | `{"upstream_id","upstream_model"}` | upstream correlation ids |
| `error` | `{"error":{"code","message","retryable"},"request_id"}` | in-band failure after headers were sent |
| `end` | `{"finish_reason","usage","cost_nano_usd","duration_ms","ttft_ms"}` | then `data: [DONE]` |

This buys cross-provider uniformity, per-tenant billing, and a client surface
that does not change when a vendor changes their wire format. It costs a decode
and re-serialize per frame.

### `passthrough`

Upstream bytes are relayed verbatim — no text conversion, no SSE parse, no
re-serialize:

```bash
curl -N http://localhost:8080/v1/chat/stream \
  -H 'x-api-key: gwk_live_...' \
  -H 'x-gateway-framing: passthrough' \
  -d @request.json
```

The trade is real: the gateway cannot see inside the stream, so per-token
budgets are unavailable mid-stream and cost settles from the pre-flight
reservation. Comments, `event:` names, and vendor extensions survive exactly as
sent.

### The measured difference

`cargo bench --bench framing`, on this machine:

```
passthrough: 2000 frames, 416890 bytes in 5.5µs (2ns/frame)
normalized:  2000 frames,  32890 bytes in 6.5709ms (3.285µs/frame)
```

~3.3 µs per frame for normalized, of which ~2.8 µs is JSON decoding. At roughly
four bytes per token that is well under a microsecond per token — invisible next
to a provider's 10 ms+ inter-token gap. **The honest read:** the choice is not
about per-token latency at all. It is about capability. Choose `normalized`
unless you specifically need a vendor extension or a comment frame the gateway
would otherwise drop. The benchmark is in the repo so this stays a number rather
than an assertion.

## Governance

Every request passes through, in order, before the upstream connection opens:

```
authorize(tenant, model)  →  is this tenant allowed this model at this size?
check_limits(tenant, model, tokens)  →  is there quota, and can we afford it?
```

Rejections are real HTTP statuses, not errors hidden inside a 200:

| Condition | Status | `error.code` |
|---|---|---|
| Missing or invalid key | 401 | `unauthorized` |
| Model not on allowlist | 403 | `model_not_allowed` |
| Missing `chat:stream` scope | 403 | `forbidden` |
| RPM or TPM exceeded | 429 + `retry-after` | `rate_limited` |
| Concurrency cap reached | 429 | `concurrency_limited` |
| Daily budget exhausted | 402 | `budget_exhausted` |
| No `max_tokens` anywhere | 400 | `max_tokens_required` |
| Body over the limit | 413 | `payload_too_large` |
| Upstream 4xx/5xx before streaming | 502 | `upstream_*` |

`max_tokens` must be resolvable — from the request, the registry's
`default_params`, or tenant `model_params` — because the gateway reserves cost
before connecting. A model with no cap anywhere, called without one, is a 400:
an uncapped request is an unbounded one.

Parameter precedence, lowest to highest: **registry `default_params` → caller
`params` → tenant `model_params`.** Policy sits on top deliberately — a tenant
that pins `temperature: 0.2` is expressing a governance constraint, and a
constraint a caller can override is not one.

## Configuration

`config/default.toml`, overridable by `LLM_GATEWAY__SECTION__FIELD` env vars.

```toml
[server]
port = 8080
request_body_limit_bytes = 1048576
keep_alive_interval_ms = 15000

[upstream]
# Per-chunk read timeout, NOT a whole-response deadline: a 30s total limit on
# a 4000-token completion is a bug, not a safety feature.
stream_read_timeout_ms = 120000

[governance]
require_model_allowlist = true
on_max_tokens_exceeded = "clamp"   # or "reject"

[governance.default_limits]
requests_per_minute = 120
tokens_per_minute = 200000
max_concurrent_streams = 16
max_output_tokens = 8192
daily_budget_nano_usd = 5000000000   # 1 USD = 1_000_000_000 nano-USD
```

`config/models.yaml` is the only place a model, provider, or upstream URL is
defined. A per-model `endpoint:` override is what makes a self-hosted NIM
container work.

`config/tenants.yaml` holds credentials, scopes, allow/deny lists, limits, and
per-model policy. Keys are referenced by env var (`key_env`), hashed at boot,
and only the digest is retained — a memory dump yields digests, not credentials.

## Adding a provider

1. Implement `ChatProvider` in `src/providers/<name>.rs`. One method:
   `stream_chat(request) -> AsyncTokenStream`. If the vendor is
   OpenAI-compatible, `OpenAiCompatAdapter` plus a `QuirkFlags` spec is usually
   the whole adapter.
2. Register it in `src/bootstrap.rs`.
3. Add a `models.yaml` entry.

No API change, no client change.

## Deployment notes

- **Disable intermediary buffering.** The gateway sets `Cache-Control:
  no-cache, no-store, no-transform` and `X-Accel-Buffering: no`, but nginx,
  Envoy, and most CDNs need matching configuration. Verify with `curl -N`.
- **No transparent compression.** The `gzip` feature is deliberately absent
  from `reqwest`: decoding buffers before yielding, which destroys token-by-token
  flush timing. Compress in front of the gateway, with flushes enabled, or not
  at all.
- **Latency claims.** The gateway adds no measurable per-token latency in either
  mode. It cannot remove DNS/TLS/connect time, provider queueing, provider TTFT,
  model scheduling, or client-side network distance. Those dominate.
- **Per-token throughput is model- and load-dependent.** Local NIM latency
  depends on model size, quantization, batch size, KV-cache length, and GPU —
  measure it rather than assuming.

## Testing

```bash
cargo test              # 107 tests
cargo clippy --all-targets
cargo bench --bench framing
```

`tests/integration.rs` drives the real router over real HTTP against a mock
upstream with scenario-driven SSE, covering the v1 contract, reasoning
separation, tool calls, mid-stream errors, passthrough fidelity, parameter
precedence, every governance rejection, and client-disconnect accounting.

## Layout

```
src/
  config/          layered settings (file < env)
  router/          model registry, resolution, param merge, cost estimation
  providers/       per-vendor adapters hiding their quirks
    openai_compat.rs   shared SSE engine; adapters are a spec
  governance/      auth (API key, JWT) → policy → limits
  api/             HTTP surface and SSE framing
  observability/   structured logging, Prometheus metrics
  bootstrap.rs     startup wiring
```
