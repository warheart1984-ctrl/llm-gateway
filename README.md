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
  stream that dies at 20% still pays for what it used. See
  [How a request is billed](#how-a-request-is-billed).
- **Config-driven catalogue.** No model id, provider, or upstream URL is
  compiled in. `models.yaml` and `tenants.yaml` hot-reload on mtime change.
- **Versioned contract.** Frame shapes are frozen under `v1`; breaking changes
  get a new prefix, not a new meaning.
- **Quiet hot path.** No per-token logging, no per-token high-cardinality
  metrics. One summary line and one metrics update per stream.

## Try it: the demo

No provider account needed. `demo/` runs a mock provider, the gateway, and a
scripted SSE client that walks through the claims below and checks each one:
a streamed completion, per-tenant spend, a tiny budget running out with a 402
before any upstream connection, tenant isolation, a mid-stream hang-up
releasing its slot, and metrics on a separate ops port.

```bash
cd demo && docker compose up --build --abort-on-container-exit --exit-code-from client
```

CI runs exactly this, so a demo that stops passing fails the build. To run
the client against a gateway you started yourself, see the header of
`demo/client.sh`.

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
| `POST` | `/v1/chat/complete` | The same request, answered as one JSON document |
| `GET` | `/v1/models` | Catalogue, filtered to the caller's allowlist |
| `GET` | `/v1/usage` | Caller's own quota and spend |
| `POST` | `/v1/admin/registry/reload` | Re-read `models.yaml` / `tenants.yaml` (admin scope) |
| `GET` | `/health/live` | Liveness |
| `GET` | `/health/ready` | Readiness; 503 if routing cannot work |
| `GET` | `/metrics` | Prometheus text exposition (admin scope) |

With `server.ops_port` set, `/metrics` (unauthenticated) and the admin reload
move to that listener and no longer exist on the public port. See
[Security posture](#security-posture).

## Non-streaming: `/v1/chat/complete`

The request body is the one `/v1/chat/stream` takes, minus streaming:
`stream` must be `false` or absent (`true` is a 400 `stream_not_allowed`),
and `framing` is ignored. Governance is shared code, not a copy. Auth,
allowlists, parameter precedence, rate limits, concurrency and budget
reservation apply identically, and both endpoints draw on the same quotas.

```bash
curl http://localhost:8080/v1/chat/complete \
  -H 'x-api-key: gwk_live_...' -H 'content-type: application/json' \
  -d '{"model":"groq/gpt-oss-120b","messages":[{"role":"user","content":"hi"}],"params":{"max_tokens":200}}'
```

```json
{
  "request_id": "...", "model": "groq/gpt-oss-120b", "tenant": "acme",
  "content": "Hello!", "reasoning": null, "tool_calls": [],
  "finish_reason": "stop",
  "usage": {"prompt_tokens": 9, "completion_tokens": 3, "reasoning_tokens": 0, "total_tokens": 12},
  "cost_nano_usd": 3150, "duration_ms": 412,
  "upstream_id": "...", "upstream_model": "openai/gpt-oss-120b", "metadata": null
}
```

The answer carries what the stream's `start`, token, `reasoning`, `tool` and
`end` frames carry. Reasoning stays out of `content`, and a tool-call-only
answer has `content: ""`. Errors use the same statuses and error body as the
stream endpoint, and the same `x-*` response headers are set.

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
budgets are unavailable mid-stream and **the tenant is billed the full
pre-flight reservation** — prompt estimate plus the whole `max_tokens`
ceiling — however the stream ends. Refunding any of it on a guess would make
passthrough a way around the budget. Comments, `event:` names, and vendor
extensions survive exactly as sent.

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

### How a request is billed

Admission checks the budget and reserves `prompt estimate + max_tokens ×
output rate` in one atomic step, so concurrent requests cannot jointly
overshoot it. The first of these outcomes to happen decides the bill; every
later cleanup path is a no-op on the ledger:

| Outcome | Billed |
|---|---|
| Stream finishes, upstream reports usage | actual usage |
| Stream finishes or fails mid-way, no usage reported | prompt + completion estimated from the deltas actually delivered |
| Client disconnects mid-stream | same: prompt + what was delivered before the hang-up |
| Upstream refuses or is unreachable (no 2xx), or answers 200 with an in-band `{"error": ...}` | nothing — it processed no prompt |
| `passthrough` framing, any ending | the full reservation |
| Completion answered, upstream reports usage | actual usage |
| Completion answered, no usage reported | prompt + completion estimated from the returned text |
| Completion accepted, then the answer is unreadable | the prompt |
| Client disconnects while a completion is pending | the full reservation: a non-streaming upstream finishes and bills the answer anyway, and the gateway never sees its usage |

A reservation made before UTC midnight and settled after it is charged to the
day it was made in: a refund to a day that has rolled over is dropped rather
than credited to today, and an overage is charged to today.

These properties are tested under concurrency in
`src/governance/limits.rs` (every ordering of closing calls, a randomized
multi-threaded ledger property, admission races at the budget edge, tenant
isolation, rollover) and end to end in `tests/integration.rs`.

## State and restarts

Where spend lives is a choice, `[ledger] backend`:

| | `memory` (default) | `sqlite` | `postgres` |
|---|---|---|---|
| Survives a restart | no: spend resets to zero | yes | yes |
| Replicas share one budget | no: each enforces the full budget | no: use quota split (below) | yes |
| Repeated `Idempotency-Key` recognised | within one process | across restarts, on one host | across all replicas |
| Needs a database server | no | no: one local file | yes |
| Ledger unreachable | cannot happen | 503; the gateway will not start without it | 503 `ledger_unavailable`; readiness fails; no request executes |

`sqlite` is the Postgres design on a local file (`[ledger] sqlite_path`,
relative to the config directory). WAL journal with `synchronous = FULL`,
so a reservation is on disk before the provider is called, and every
transaction starts with `BEGIN IMMEDIATE`, which serializes admissions even
between processes sharing the file. Keep the file on a local disk, not a
network filesystem, and give each gateway its own.

**Quota split, for replicas without a shared database.** Set
`quota_split_replicas = N` (and optionally `quota_split_margin_percent`) on
N replicas, each on its own `sqlite` file. Each replica enforces
`budget * margin% / N` of every tenant's budget, rounded down, so together
they can never exceed it; `/v1/usage` reports the share as
`budget_scope.replica_share`. The costs are plain:

- **It strands budget.** One replica cannot spend another's unused share.
  A shared Postgres ledger does not have this cost.
- **N is fixed.** Every replica must use the same N. A replica added
  mid-day starts with a fresh share on top of what the others already
  spent, so change N only at the UTC day boundary.
- **It refuses unsafe setups.** On the `memory` ledger a restart would hand
  a replica a fresh share, and on `postgres` the replicas already share one
  budget, so the gateway will not boot with either. A tenant budget whose
  share rounds down to zero is refused outright; zero would otherwise mean
  "no ceiling".

With `postgres`, every change to money is one conditional statement, so the
database decides, not any gateway process:

- **Admission** charges the tenant's row for the day with
  `UPDATE … WHERE spent < budget AND spent + amount <= budget`. The row lock
  serializes admissions across replicas. The day comes from the database
  clock, so replicas with skewed clocks agree on midnight.
- **The reservation is committed before the provider is called.** A crash
  can leave a reservation open, but never a provider call without a record.
  A sweeper closes reservations left open longer than `sweep_after_secs`,
  billing the full reservation, because after a crash the usage is unknowable.
- **Closing** is `UPDATE … WHERE state = 'open'`, so a duplicate close from
  any process changes nothing. Closings are queued to one writer that
  retries until each one is durable, and graceful shutdown flushes the queue.
- **There is no fail-open setting.** A ledger that is unreachable at startup
  stops the gateway starting. One that is unreachable at runtime refuses
  requests with 503.

Put `sslmode=require` in the database URL outside a trusted network.

**Idempotency** works on both backends. Send `Idempotency-Key` (1 to 255
visible ASCII characters). A repeat of a finished request is not executed
again: a completion returns the stored answer (`idempotent-replayed: true`),
and a stream gets 409 `duplicate_request` naming the original request and
its bill. A repeat while the first attempt is still running gets 409
`request_in_progress`. The same key with a different request gets 422. A key
whose attempt the provider refused may be used again, since nothing was
executed or billed. Set `require_idempotency_key: true` on a tenant whose
clients retry automatically.

**Stored answers are sealed.** On the Postgres ledger, the answer kept for
replay is encrypted with AES-256-GCM before it reaches the database. It is
bound to its tenant and request, so a copy moved into another row does not
open, and anything that fails to open is never served (the caller gets 409
`duplicate_request`). Keys come from the env var named by
`[ledger] response_keys_env` (default `LLM_GATEWAY_RESPONSE_KEYS`), as
`kid:base64-of-32-bytes[,kid:...]`: the first key seals and every key opens,
so a key rotates by putting the new one first. Generate one with
`openssl rand -base64 32`. With no keys set, answers are not stored at all,
and a repeated completion gets 409 instead of a replay. A malformed key
setting stops the gateway starting.

**Still per process on both backends:** rate-limit windows and concurrency
caps. Behind N replicas, divide them by N.

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
per-model policy. Keys are referenced by env var (`key_env`) or by file
(`key_file`, e.g. a mounted secret), hashed at load, and only the digest is
retained — a memory dump yields digests, not credentials.

## Adding a provider

1. Implement `ChatProvider` in `src/providers/<name>.rs`: `stream_chat`
   for `/v1/chat/stream`, and `complete` for `/v1/chat/complete` (without
   it, that endpoint answers 502 for the provider's models). If the vendor is
   OpenAI-compatible, `OpenAiCompatAdapter` plus a `QuirkFlags` spec is
   usually the whole adapter, both methods included.
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

## Security posture

What the gateway does itself, and what it expects of the deployment around it.

| Concern | The gateway | The deployment |
|---|---|---|
| **TLS** | Speaks plain HTTP only. | Terminate TLS in front (ingress, nginx, Envoy, a cloud LB). Upstream calls to vendors are HTTPS via rustls. |
| **Client IP / trusted proxies** | Never reads `X-Forwarded-For` or the peer address. Every limit is keyed by the authenticated tenant, so there is no IP-based decision a spoofed header could influence, and no trusted-proxy list to configure. | IP allow-listing or per-IP flood control, if wanted, belongs in the proxy. |
| **Request size** | Bodies over `request_body_limit_bytes` get a 413 on the declared `content-length` alone, before any byte is read. Headers use hyper's defaults: more than 100 headers is a 431 (tested); the header block is capped at hyper's ~400 KB read buffer. | Set tighter header limits at the proxy (e.g. nginx `large_client_header_buffers`) if you need them. |
| **Operator endpoints** | `/metrics` names every tenant's spend, so on the public port it requires the `admin` scope. Set `server.ops_port` to move `/metrics` and `/v1/admin/*` to a separate listener (loopback by default); they then return 404 on the public port. The reload still requires `admin` there. | Bind the ops listener to a private interface or network and scrape it from inside. |
| **Log redaction** | Logs carry one structured summary per stream: ids and counts, never credentials or message content. An integration test captures every log line from the whole suite at `llm_gateway=trace`, including refused and malformed requests and bearer auth, and fails if a key or a prompt marker appears. | Keep the gateway's filter at `debug` or above for other crates; hyper/reqwest `trace` output is not covered by that test. |
| **Secret rotation** | `key_file` credentials are re-read on every tenant reload, so replacing the file and reloading rotates a key with no restart; the old key stops working the moment the reload lands (tested). `key_env` values, provider API keys and the JWT secret are read once at boot, and rotating them means a restart. There is no dual-secret window for JWTs. | Rotate tenant keys without a gap by adding the new credential alongside the old, moving clients, then removing the old one. |
| **Metric cardinality** | Every label value comes from configuration — provider, registry model id, tenant id from `tenants.yaml` — never from request input. A series is created only after authentication and routing succeed. Labels with delimiter characters or over 128 bytes become `invalid`. Tested with unknown models, unknown keys and junk metadata. | Cardinality is bounded by the size of the model and tenant files; size scrape limits accordingly. |
| **Request ids** | A client-supplied `x-request-id` (≤ 128 visible ASCII chars) is echoed and logged for correlation. It is not an identity and is never trusted for anything else. | Overwrite it at the proxy if clients must not choose it. |

## Testing

```bash
cargo test --locked --all-targets                  # 245 tests
cargo clippy --locked --all-targets -- -D warnings
cargo bench --bench framing                        # add `-- --quick` for a fast pass
```

CI (`.github/workflows/ci.yml`) runs that gate from a clean checkout on Linux
and Windows: the full suite three times over, the money tests five more times,
clippy with warnings as errors, a build on the declared `rust-version`, the
benchmark in quick mode, and the docker compose demo end to end.

`tests/pressure.rs` is the boundary pressure suite: one test per attack on
the claim that an unauthorised or financially inadmissible request never
reaches the provider, checked by counting what the provider actually
received. It includes tests that pin today's known gaps (restart, replicas,
replay) as failing open. The claim → test → result table is
[docs/boundary-pressure-tests.md](docs/boundary-pressure-tests.md).

`tests/integration.rs` drives the real router over real HTTP against a mock
upstream with scenario-driven SSE, covering the v1 contract, reasoning
separation, tool calls, mid-stream errors, passthrough fidelity, parameter
precedence, every governance rejection, client-disconnect accounting, billing
for each way a stream can end, concurrent admissions at the budget edge, log
redaction, header limits, and metric-label cardinality.

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
examples/
  mock_upstream.rs a stand-in provider for the demo
demo/              docker compose demo: config, secrets, scripted client
```
