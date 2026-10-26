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
| `POST` | `/v1/chat/completions` | OpenAI Chat Completions format, streamed or not, for OpenAI SDKs |
| `GET` | `/v1/models` | Catalogue, filtered to the caller's allowlist |
| `GET` | `/v1/usage` | Caller's own quota and spend |
| `GET` | `/v1/holds/{id}` | A held request's state, for the tenant that made it |
| `GET` | `/v1/receipts/log`, `/anchor`, `/key` | The caller's own tenant's receipt evidence (`receipts:read`) |
| `POST` | `/v1/admin/registry/reload` | Re-read `models.yaml` / `tenants.yaml` (admin scope) |
| `GET` | `/v1/admin/decisions` | The decision record (admin scope) |
| `GET` | `/v1/admin/holds` | Held requests; `?state=pending` is the approval queue (`approve:holds` or admin) |
| `POST` | `/v1/admin/holds/{id}/approve` | Approve a held request (`approve:holds`) |
| `POST` | `/v1/admin/holds/{id}/deny` | Deny it, or revoke an approval not yet used (`approve:holds`) |
| `GET` | `/health/live` | Liveness |
| `GET` | `/health/ready` | Readiness; 503 if no model has a usable provider route |
| `GET` | `/metrics` | Prometheus text exposition (admin scope) |

Readiness evaluates each model route, including provider credentials and the
explicit local endpoint used by self-hosted NVIDIA NIM. A deployment remains
ready when at least one route is usable; the response lists unavailable model
routes in `unavailable_models` so a fallback does not hide a degraded catalog.

With `server.ops_port` set, `/metrics` (unauthenticated) and every
`/v1/admin/*` route move to that listener and no longer exist on the public
port. See
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

## OpenAI-compatible: `/v1/chat/completions`

An application written for OpenAI's API switches to the gateway by changing
only its base URL:

```python
from openai import OpenAI
client = OpenAI(base_url="https://gateway.example.com/v1", api_key="gwk_live_...")
client.chat.completions.create(model="groq/gpt-oss-120b", messages=[...], max_tokens=200)
```

The request is OpenAI's Chat Completions body; `"stream": true` streams it.
The answer is OpenAI's shape: a `chat.completion` document, or
`chat.completion.chunk` data lines ending in `data: [DONE]`, with a final
usage chunk when `stream_options.include_usage` is set. Tool calls come back
as OpenAI `tool_calls`; reasoning, when a model sends it, as
`reasoning_content`. Errors use the gateway's error body, which OpenAI SDKs
read as their own (`AuthenticationError` for 401, `BadRequestError` for 400,
and so on); a failure after streaming began is a `data: {"error": ...}` line.

Underneath it is the same code as `/v1/chat/stream` and `/v1/chat/complete`:
the key (`Authorization: Bearer` works), the allowlist, limits, budget
reservation, holds and receipts all apply unchanged, and `x-receipt-id` /
`x-receipt-salt` come back in the response headers. Differences to know:

- `max_tokens` (or `max_completion_tokens`) must be resolvable, from the
  request or the registry's `default_params`, as on every endpoint: the
  gateway reserves cost before calling the model. Set a default per model if
  your clients leave it out.
- `stream`, `stream_options` and `n` are checked, not coerced: a wrong type
  (`"stream": "true"`, a non-boolean `include_usage`, `stream_options`
  without `"stream": true`) is `400 invalid_parameter`, as on OpenAI's API;
  `null` means not set.
- `n` above 1 is refused (`400 unsupported_parameter`). `store` and
  `metadata`, OpenAI's own storage options, are not forwarded. Every other
  field is forwarded as a parameter.
- A held request answers `202` with the hold, as on the other endpoints; an
  OpenAI SDK does not know that shape, so tenants with holds need a client
  that handles it.
- An `Idempotency-Key` is scoped to the format, so a key reused across
  `/v1/chat/complete` and `/v1/chat/completions` is never answered with the
  other format's stored reply.
- For a receipt's reply digest, `content` is the concatenated
  `delta.content`, `reasoning` the concatenated `delta.reasoning_content`,
  and tool calls are assembled per `index` as for the native stream.

The receipts demo drives this endpoint with the official OpenAI Python SDK.

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
| Held for approval (not a refusal) | 202 | `status: "held"` |
| Presented hold still pending | 409 + `retry-after` | `hold_pending` |
| Presented hold denied | 403 | `hold_denied` |
| Presented hold expired | 410 | `hold_expired` |
| Presented hold already used | 409 | `hold_consumed` |
| Not the request that was approved | 422 | `hold_mismatch` |
| Too many holds waiting | 429 | `holds_pending_limit` |
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

Where spend lives is a choice, `[ledger] backend`. It is durable by default:

| | `sqlite` (default) | `postgres` | `memory` |
|---|---|---|---|
| Survives a restart | yes | yes | no: spend resets to zero |
| Replicas share one budget | no: use quota split (below) | yes | no: each enforces the full budget |
| Repeated `Idempotency-Key` recognised | across restarts, on one host | across all replicas | within one process |
| Needs a database server | no: one local file | yes | no |
| Ledger unreachable | 503; the gateway will not start without it | 503 `ledger_unavailable`; readiness fails; no request executes | cannot happen |

`memory` is for tests and development. Choose it explicitly; the gateway
logs a warning at boot whenever it is in use. `GET /health/ready` states
what the ledger in use guarantees, so nobody has to infer it from a name:

```json
"ledger": { "backend": "sqlite", "durability": "persistent", "replica_mode": "single_instance_only" }
```

`replica_mode` is `single_instance_only`, `shared` (Postgres),
`single_process` (memory), or the quota split's replica count and margin.

> **Upgrading from an earlier release.** A config with no `[ledger]`
> section used the in-memory ledger; it now uses `sqlite`, writing
> `ledger.sqlite3` next to the config (`sqlite_path` changes where). That
> directory must be writable. If it is not, for example a read-only config
> mount, the gateway refuses to start and says why. Point `sqlite_path` at a
> writable volume, or set `backend = "memory"` to keep the old behaviour.

<details><summary>The same table, as it was before this release</summary>

| | `memory` (old default) | `sqlite` | `postgres` |
|---|---|---|---|
| Survives a restart | no: spend resets to zero | yes | yes |
| Replicas share one budget | no: each enforces the full budget | no: use quota split (below) | yes |
| Repeated `Idempotency-Key` recognised | within one process | across restarts, on one host | across all replicas |
| Needs a database server | no | no: one local file | yes |
| Ledger unreachable | cannot happen | 503; the gateway will not start without it | 503 `ledger_unavailable`; readiness fails; no request executes |

</details>

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
  Every reservation holds a **lease** (`lease_secs`, default 60). The
  gateway renews the leases it holds in one statement every third of that,
  until each reservation's closing is durable. A crashed process renews
  nothing, so its leases lapse, and the sweeper closes reservations whose
  lease has lapsed, billing the full reservation, because after a crash the
  usage is unknowable. A stream that simply runs long keeps renewing and is
  never swept while live. (`sweep_after_secs`, the older name for the
  setting, is still accepted.)
- **A refused request is never billed.** `timeout_ms` bounds deciding an
  admission; running out rolls the transaction back, so nothing is held.
  The commit gets what is left, but never less than a quarter of
  `timeout_ms`, so under overload an admission can take up to 1.25 times
  it. A commit still running past that is refused with 503
  like any timeout, but left to finish rather than abandoned: if it lands,
  the reservation is released at once, at no charge, written straight to
  the database rather than queued behind settlements
  (`gw_ledger_late_commits_released_total`). The SQLite ledger does the
  same. Abandoning it would leave a reservation nothing closes, holding a
  concurrency slot and its idempotency key until the sweeper billed it in
  full.
- **Upkeep is on `/metrics`.** `gw_ledger_sweep_failures_total` counts
  sweeper steps that failed: while they fail, lapsed reservations stay open
  and hold their tenant's money. `gw_ledger_swept_reservations_total`
  counts reservations billed in full because their lease lapsed; outside a
  crash it should stay at zero, so alert on it.
- **A connection is never reused mid-transaction.** An operation cut off by
  its deadline while starting a transaction can leave the database's
  transaction open with nothing in the gateway to roll it back. Every
  ledger connection is checked as it returns to the pool, and one still
  inside a transaction is closed, which ends it
  (`gw_ledger_stuck_connections_closed_total`). Reused, it stopped the
  whole SQLite ledger under overload (it holds the write lock), and on
  Postgres it held a tenant's day-row lock, refusing that tenant's
  admissions.
- **Closing** is `UPDATE … WHERE state = 'open'`, so a duplicate close from
  any process changes nothing. Closings are queued to one writer that
  retries until each one is durable, and graceful shutdown flushes the queue.
- **The settlement queue is bounded at the door.** Closings carry money, so
  none is ever dropped. Instead, once `max_pending_closings` (10,000) are
  waiting to be written, new admissions are refused with a retryable 503
  until the writer catches up. `gw_ledger_pending_closings` shows the
  backlog, and `gw_ledger_backlog_refusals_total` counts the refusals.
- **Retention.** A closed reservation (one per admitted request, with its
  bill) is deleted after `reservation_retention_days` (30), in batches, by
  the sweeper (`gw_ledger_reservations_pruned_total`); `spend_days` keeps
  the daily totals regardless. A stored answer is cleared once its
  idempotency key expires (`idempotency_retention_secs`), since nothing can
  replay it after that. The gateway will not start with a reservation
  retention shorter than the idempotency window. 0 keeps reservations
  forever.
- **There is no fail-open setting.** A ledger that is unreachable at startup
  stops the gateway starting. One that is unreachable at runtime refuses
  requests with 503.

Put `sslmode=require` in the database URL outside a trusted network.

**Idempotency** works on every backend. Send `Idempotency-Key` (1 to 255
visible ASCII characters). A repeat of a finished request is not executed
again: a completion returns the stored answer (`idempotent-replayed: true`),
and a stream gets 409 `duplicate_request` naming the original request and
its bill. A repeat while the first attempt is still running gets 409
`request_in_progress`. The same key with a different request gets 422. A key
whose attempt the provider refused may be used again, since nothing was
executed or billed. Set `require_idempotency_key: true` on a tenant whose
clients retry automatically.

**Request fingerprints are keyed.** An idempotency record keeps a
fingerprint of its request, to tell a retry from a reused key. A plain hash
of a prompt would let anyone who can read the ledger confirm a guess at it,
so the fingerprint is an HMAC-SHA256 keyed from the env var named by
`[ledger] fingerprint_keys_env` (default `LLM_GATEWAY_FINGERPRINT_KEYS`),
as `kid:base64-of-32+-bytes[,kid:...]`. The first key fingerprints new
records and every key matches old ones, so keys rotate like the sealing keys.
Records made before keys were configured still match until they expire.
Without keys, fingerprints stay plain SHA-256, with a warning at boot.

**Stored answers are sealed.** On the durable ledgers, the answer kept for
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

**Rate limits and concurrency are shared too,** on `sqlite` and `postgres`,
decided in the same admission transaction as the budget:

- **Concurrency** is the tenant's count of open reservations with a live
  lease, counted under the tenant's day-row lock. Across replicas it is one
  cap. A crashed replica's reservations keep their slots until their leases
  lapse, which errs toward refusing.
- **Requests and tokens per minute** are counters per tenant per aligned UTC
  minute, charged with a conditional `UPDATE`. A refusal rolls back and
  consumes nothing. Completion tokens count in the minute they settle in.
- **It is a fixed window, not a sliding one.** It is exact and shared, but a
  tenant can use a full minute's quota at 12:00:59 and another at 12:01:00:
  up to twice the rate across a boundary. The in-memory ledger keeps its
  per-process sliding window.
- With quota split, each replica enforces its share of these limits as well,
  and a limit whose share rounds down to zero refuses outright.
- `/v1/usage`'s `requests_last_minute`, `tokens_last_minute` and
  `streams_in_flight` are this process's view. The shared counters decide.

### The decision record

Reservations record admitted, billable work. The decision record covers the
rest: every refusal (4xx) and failure (5xx) a known caller received, and
every operator action. `GET /v1/admin/decisions?tenant=&since=&limit=`
(admin scope, and on the ops listener when one is configured) returns
request id, tenant, key id, kind, endpoint, model, code, reason and time,
newest first.

- **Only authenticated callers.** Anonymous 401s stay in logs and metrics;
  persisting them would let anyone write to the database.
- **No prompts or answers.** A reason is a code and a short sentence. For a
  malformed body it is a fixed sentence, because a parser's error can quote
  the offending value.
- **Routine records never cost availability.** Refusals and failures go
  through a bounded queue. If it is full or the store fails, the record is
  dropped and counted in `gw_decisions_dropped_total`; the caller still gets
  the refusal.
- **Operator actions are recorded before they happen.** A registry reload is
  written synchronously first. If that write fails, the reload does not
  happen and the operator gets 503.
- Kept `decision_retention_days` (default 30), then deleted by the sweeper.
  Durable on `sqlite` and `postgres`; the last 10,000 in memory on `memory`.

## Conversation folding

A chat client resends the whole conversation on every request, so the prompt grows with every turn. A tenant that opts in can have the older turns replaced by a rolling summary before the request is forwarded. Folding is off unless that tenant's `folding.enabled` is true. On one 14-call run, 5-fold-engine cut main-path prompt tokens from 18,977 to 4,311, and the summarizer still spent more than the cut saved (net −$0.0057) because it was an expensive reasoning model. A summary can also drop facts that changed during the conversation.

**Folding saves money, and keeps changed facts correct by refusing to fold what it cannot verify.** In the benchmark below, the fixed scenario kept every fact with the final version, at a 47% saving (51–55% in earlier versions, one of which lost a corrected budget). Facts that change mid-conversation are kept in a gateway-owned [fact ledger](#the-fact-ledger): the summarizer proposes updates, and the gateway verifies each against the conversation before it becomes current. With `gpt-4.1-mini` summarizing and `amortization_requests: 16`, two runs full of corrections kept every fact (889 / 889 each) at a 54–55% saving, through the fold cap, a correction after it, and a re-fold that carried the correction. With `gpt-4.1-nano` summarizing, the gateway refused almost every fold of those conversations, so they kept every fact but saved nothing (about 3% extra, with the growing back-off). The summarizer has to be able to report facts reliably, and `amortization_requests` has to reflect how many requests a summary really serves. Earlier designs that let the summarizer write the facts lost 2–14% of changed facts; see [what each round showed](#changing-facts-what-the-safeguards-fixed-and-what-they-did-not).

The summary is a `user` message, never `system` or `developer`: it is model-written from the tenant's own turns, and a user turn that says "ignore your rules" must not come back with system authority. Some providers reject two `user` messages in a row. The OpenAI-compatible providers this gateway calls accept it.

`fold_mode` is `off`, `shadow`, or `on`. It defaults to `off`. A tenant that still sets `enabled: true` and no `fold_mode` is `on`, so existing configs do not change. `shadow` runs the same fold — summarizer, fact ledger, pin and stale-fact checks, back-off, amortization, and the single-flight lock — and records what it would have done, then forwards the original messages. The shadow ledger is a separate store: switching that tenant to `on` starts fresh. The response matches `off` except for `x-gateway-fold-shadow`, which carries the outcome and the estimated net saving and never the summary. A shadow fold is receipted as `fold_shadow`, not `fold`. Summarizer spend counts against the tenant budget. If the summarizer fails, times out, or the budget cannot cover both calls, the client request still succeeds unfolded.

Shadow mode spends real summarizer money to produce its estimates. A tenant in shadow pays for those fold attempts and does not get the token savings, because the upstream request stays unfolded.

A shadow fold is counted under its own metrics, `gw_shadow_folds_total`, `gw_shadow_fold_summarizer_tokens_total`, `gw_shadow_fold_checks_failed_total` and `gw_shadow_fold_corrective_retries_total`, never under `gw_folds_total`: nothing was folded for that tenant. With `[folding] background = true` the summarizer runs after the request has logged its decision, so a background shadow fold logs a `shadow fold finished` line of its own with what the summarizer cost. `python tools/shadow_report.py <gateway.log>` totals a log per tenant: requests, the share that would have gone out folded, each fold outcome and reason, and the net saving after summarizer cost, background folds included.

A folded response carries `x-gateway-folded: <turns replaced>`. The decision log records counts and hashes only, never the summary or the message text. The summarizer call is a separate governed request: a model the tenant is allowed to call, with its own reservation and settlement. If the fold fails, is refused, does not pay, or cannot be admitted, the client request still succeeds and goes out with the last good summary, or unfolded.

Folding works with `receipts: true`: the receipts bind the summary the model was actually sent, and the summarizer call gets its own receipt (see "Folded calls" under Receipts). For such a tenant `summary_chars` is at most 4,000, because the summary comes back in a header, and `[folding] background = true` is refused at load, because a background fold has no caller to hand the salt and summary to.

```yaml
folding:
  fold_mode: on                        # off (default) | shadow | on; enabled: true still means on
  summarizer_model: groq/summarizer   # must be in this tenant's allowlist
  keep_last: 4                         # newest turns sent verbatim
  summary_chars: 1200
  summarizer_max_tokens: 512          # one retry at double this budget
  max_folds: 8
  refold_threshold: 8
  min_new_turns: 4
  min_new_tokens: 400
  amortization_requests: 4            # requests a summary is expected to serve; set near real reuse
  min_saving_nano_usd: 0
  backoff_turns: 8                     # after a rejected fold, wait this many more turns
  fold_tool_turns: false               # fold tool calls and results too; see "Tool turns"
```

`[folding]` in `config/default.toml` bounds the in-memory store (`max_entries`, `max_total_chars`) and sets `background`. The default runs the fold before the request is forwarded. `background = true` answers with the summary already committed and folds afterwards, which spends the tenant's budget after the response.

What a fold costs and keeps:

- **The summarizer call takes a rate slot.** It is admitted like any request, so a fold only runs when the tenant's rate window has room for it and for the request it serves. Near the limit, the request goes out unfolded rather than being refused. A request that is refused afterwards for budget or a repeated `Idempotency-Key` has still paid for its fold; the summary it made is kept for the next request.
- **It is billed like a main call.** Summarizer prompt tokens are billed at no less than the gateway's estimate of what was sent, and a summarizer that reports no usage is billed the estimated output. `gw_fold_summarizer_tokens_total` counts what was billed.
- **The gateway keeps the facts; the summarizer does not write them.** Each fold, the summarizer returns JSON: fact updates from the new turns, each a `field`, a `value` and the `evidence` sentence it came from, plus a short `context` note. The gateway applies an update only if:
  - its evidence is one sentence from the new turns (ignoring case, spacing and a closing full stop), not a whole turn that states several facts;
  - its value is in that sentence, and the sentence names the fact: at least one word of its name that says which fact it is (`staging` in `staging_port`; words for the kind of value, such as port, date or budget, do not count);
  - its value is not the one that sentence itself replaces ("Noor replaces Mira" can make Noor the owner, never Mira);
  - it is not a second name for a fact already kept: a name whose words all belong to a kept fact's name, or the other way round (`release` beside `release_date`, `project_owner` beside `owner`), a kept value under a new name, or a value another fact replaced;
  - when its evidence names a fact already kept ("staging now listens on 8444" and `staging_port`, "the ceiling is now 1800 USD" and `spending_ceiling_usd`), it goes to that fact, not to another or to a new one.

  Standing instructions are kept the same way, as their own kind of entry: the reply's `instructions` each give the exact sentence from the new turns ("Standing rule: never mention BIRCH-204 …") and, for a rule change, the instruction it `replaces`. The gateway stores the sentence as the conversation worded it, not the summarizer's copy; one that says it changes a rule ("rule change", "instead") must name the rule it replaces, which is then retired. Every sentence of the new turns that reads as an instruction ("standing rule", "from now on", "every answer", "always", "never") must end up kept or replaced (`instruction_missing`), so a rule cannot survive only as a paraphrase in the context note. Instructions are shown first, under `Standing instructions from the user, still in force:`, and like facts are never cut.

  After the updates apply, no kept fact may still hold a value the new turns replace ("2400 USD is obsolete" while `spending_ceiling_usd` is 2400): `ledger_stale`.

  Updates apply in the order the conversation made them, whatever order the reply lists them in, and an update from an earlier turn than a fact's current value never replaces it. Each fact keeps the sentence and turn it came from and its last 8 replaced values. Facts a later fold does not mention carry forward unchanged, so the summarizer cannot drop or flip them. The gateway writes the summary itself: a `Current facts:` block, one line per field with the value it replaced as `(was …)`, then the context note cut to what `summary_chars` leaves. Facts are never cut; a conversation whose facts alone pass `summary_chars` is not folded. One update that fails any of these fails the summary, which then gets its corrective retry; an update that cannot be verified never becomes current. The ledger is held with the summary in the store and counts towards `max_total_chars`.
- **Values that must survive are pinned.** Times, dates, hyphenated or underscored identifiers, ticket ids, short quoted strings, amounts, and explicit updates ("moved to 15:30", "actually use Tromso instead of Oslo") must appear in the clipped summary, or the fold is refused and the previous summary stays. System messages are not pinned: they go out verbatim on every request. Ordinary hyphenated words ("well-known", "follow-up", "e-mail", "re-run") are not values; anything with a digit, an underscore or a capital still is. So is a bare number of three or more digits ("port 9443", "2400 USD"), unless it is part of an id, date, time, decimal or grouped number, follows `$` or `#`, or counts something ("note 101", "step 300", "No. 123"). A value the conversation replaced (below) is not required: a summary may keep it as `(was …)` history or drop it.
- **An update that names its field pins the pair.** "owner is now Ines", "Noor replaces Mira as owner", "delivery moves to Tromso", "release slipped to 2027-03-04", "the new city is Bergen" and "staging now listens on 8444" pin `owner: Ines` and so on, the latest value per field. Some kept fact must have that value, with the field in its name or in its evidence sentence (`budget_usd: 1250 USD` from "the ceiling is now 1250 USD" states `ceiling: 1250`), so a fold that keeps "Ines" but not as the owner is refused. Only the turns being folded are read for these; an update that does not name its field ("Noor replaces Mira, and …") pins only the value.
- **A replaced value must not be stated as current.** "Noor replaces Mira", "Bergen instead of Oslo", "8444, not 8443", "Tromso, superseding Bergen" and "2400 USD is void" mark the old value as replaced, as do a field update for that field's previous value ("staging now listens on 9443", later "… on 8443 again") and the prior summary's own `(was …)` notes; a later update that names the value again takes it back. A restore in words none of these patterns know ("we are back on 8443") leaves the value replaced, so the right summary is rejected: the fold is lost, not the fact. No line of the `Current facts:` block (the whole summary if it has none) may contain a replaced value before its `(was`. This catches the summarizer reading "Noor replaces Mira" backwards, which a value pin cannot: "owner: Mira (was Noor)" still contains "Noor". Only values that start with a digit or a capital count.
- **A fold that cannot pass is not paid for.** If the pinned values alone would take more than half of `summary_chars`, the fold is skipped before the summarizer is admitted (`gw_folds_total{outcome="pinned_overflow"}`). Raise `summary_chars` for conversations heavy in ids and codes.
- **A summary that fails its checks gets one corrective retry.** The summarizer is sent its rejected reply and what was wrong with it ("It leaves out '9443'", "Update rejected: 'Mira' for owner is the value its own evidence replaces", "It is not the JSON object asked for"), and answers again. Only the summarizer, which has seen the conversation, gets those values. If the retry fails too, the fold is rejected. `gw_fold_corrective_retries_total{result="committed"|"rejected"}` counts how retries end, and the decision log's `corrected_checks` names what the first summary failed.
- **A rejected summary says which checks it failed.** Its decision log entry carries `failed_checks`, each check's name and how many errors it gave (never the values it found missing), and `gw_fold_checks_failed_total{check=…}` counts failing summaries per check, a corrective retry's included: `format`, `instruction_evidence`, `instruction_replaces`, `instruction_missing`, `ledger_field`, `ledger_evidence`, `ledger_backwards`, `ledger_misfiled`, `ledger_duplicate`, `ledger_stale`, `tool_fact_evidence`, `tool_exchange_split`, `pinned_facts_present`, `field_value_present`, `superseded_not_current`, `within_length_bound`, `non_empty`, `covers_something`.
- **A rejected fold backs off, longer each time.** After a summary fails its checks, comes back empty, or stops at the length limit, that conversation is not folded again until `backoff_turns` more turns have arrived (`outcome="backoff"`), and each further rejection in a row doubles the wait, up to 16 times `backoff_turns`. A fold that commits resets it. A conversation whose summaries keep failing then pays for a few attempts, not one every `backoff_turns` turns. A provider error does not back off; it is usually transient.
- **One fold per conversation at a time.** A request that finds a fold running for its conversation goes out with the summary already committed.
- **A conversation is keyed by tenant, API key, system messages and first turn.** Sessions behind different keys never share a summary.
- **The store is shared by all tenants.** One busy tenant can evict others' summaries, which then pay to fold again. Size `max_entries` and `max_total_chars` for every folding tenant together.

### Folding: measured

`tools/fold_retention.py` sends every call twice: once to a tenant without folding and once to a tenant with it. Both use the same model, the same messages and `temperature: 0`. It asks for ten facts that the conversation states, corrects and confuses with a second project (5-fold-engine's scenarios). Answers are scored by strict typed equality. A fact counts towards retention only if:
- it was last stated inside the turns the summary replaced;
- and the unfolded tenant got it right on the same call.

Spend is each tenant's own ledger, so summarizer calls and failed folds are included. Run it with `python tools/run_fold_retention_local.py --provider openrouter --scenario fixed --profile economics`. The result files are in `benchmark_results/`.

OpenRouter, 2026-10-23. Main model `openai/gpt-4.1-mini` ($0.40 / $1.60 per Mtok), summarizer `openai/gpt-4.1-nano` ($0.10 / $0.40). Each run is 50 calls:

| Run | Facts kept | Accuracy, unfolded / folded | Spend, unfolded → folded | Saving | Folds committed |
|---|---|---|---|---|---|
| fixed, lifecycle | 356 / 356 | 100% / 100% | $0.136 → $0.088 | 36% | 4 (46 refused as unprofitable) |
| mutating, lifecycle | 322 / 322 | 100% / 100% | $0.128 → $0.083 | 35% | 4 (3 rejected, 43 unprofitable) |
| fixed, economics | 356 / 356 | 100% / 100% | $0.127 → $0.076 | 41% | 3 (3 unprofitable) |

How to read these:

- **Only the economics run measures cost.** The lifecycle runs lower the fold thresholds (`min_new_turns: 1`, `min_new_tokens: 0`) to exercise folding in a short run, so most of their fold attempts are cost checks that refuse a fold of one new turn.
- **No run reached the fold cap.** None got to the frozen or re-fold stage. The longer runs below do.
- **The scenario is synthetic:** ten facts, one second project to confuse them with. It shows that a cheap summarizer kept every fact here, not that summaries keep facts in general.

Groq (`openai/gpt-oss-120b` main, `openai/gpt-oss-20b` summarizer, 2026-10-23) finished only the fixed lifecycle run. That run kept 182 of 182 facts and saved 5%, but committed just 1 fold of 69 attempts: 46 unprofitable, 9 rejected, 13 failed. The summarizer is a reasoning model priced at half the main model. The other two runs stopped at the account's 200,000-tokens-a-day limit for `gpt-oss-120b`. Their incomplete results are kept and exit `2`.

#### Longer runs, with prompt caching

The gateway bills every prompt token at the full input price. OpenAI, however, serves a repeated prompt prefix from its cache at a quarter of that price ($0.10 per Mtok for `gpt-4.1-mini`). An unfolded conversation only grows at the end, so most of it is cached. A folded one changes near the start at every fold, so little of it is. To see whether that erases the saving, the harness now:
- records each call's cached prompt tokens (the OpenAI-compatible endpoint returns `usage.prompt_tokens_details.cached_tokens`);
- prices them at `cached_input_per_mtok_usd` from the models file;
- reports that as `aggregates.caching.provider_cost_usd`, next to the gateway's own billing.

Summarizer calls are priced with no cache discount. The two arms take turns going first on alternate calls. Each run below was done twice (OpenRouter, 2026-10-23, same models):

| Run | Reached | Facts kept | Folded accuracy | Billed saving | Cached share, unfolded / folded | Provider cost with caching, unfolded → folded | Saving |
|---|---|---|---|---|---|---|---|
| fixed, economics, 200 calls | frozen, 8 folds | 1492 / 1492 | 100% | 48% | 59% / 19% | $0.259 → $0.120 | 54% |
| (repeat) | frozen, 8 folds | 1492 / 1492 | 100% | 51% | 64% / 20% | $0.243 → $0.118 | 51% |
| mutating, lifecycle, 120 calls | re-folded, 6 folds | 772 / 816 | 96.3% | 53% | 57% / 15% | $0.136 → $0.066 | 52% |
| (repeat) | re-folded, 8 folds | 836 / 867 | 97.4% | 45% | 65% / 5% | $0.123 → $0.060 | 51% |

What this shows:

- **Caching does not erase the saving.** The unfolded tenant gets 57–65% of its prompt tokens from the cache, but its prompt is still several times larger. Priced the way the provider charges, folding saved 51–54% in all four runs. The billed saving, which ignores caching, varied more between repeats (45–53%).
- **The economics profile froze at 200 calls and never re-folded.** It reached the fold cap with 8 folds. Its gateway logs show a re-fold attempted every 8 calls from call 141, and every one failed the summary checks: the whole history no longer fit `summary_chars` with every pinned value. It kept every fact.
- **The mutating runs lost a fact.** In both runs the folded tenant lost the latest `owner` right after a fold and never got it back. Run 1 lost "Noor" from call 31 (fold 3) and was wrong on 44 calls. Run 2 lost "Ines", the correction sent at the cap, from call 89 (its first re-fold) and was wrong on 31 calls. Every other field was kept. Pinned values did not prevent it. Both names were pinned ("Noor replaces Mira" and "owner is now Ines" both match a pin pattern), so each fold had to keep the name somewhere in the summary, yet the answers were still wrong: a pin keeps a value, not which field it belongs to. The result files do not record the wrong answers themselves. Both runs exit `1`, the harness's regression code.
- **Repeats agree on the shape but not the details:** which fold drops the fact and when differ between repeats, even at `temperature: 0`.

#### Changing facts: what the safeguards fixed and what they did not

The runs above lost the latest owner. Their summaries showed why: a chronological story that stated the old owner first and the correction later, clipped at `summary_chars` where the newest corrections sat. Each safeguard in "What a fold costs and keeps" came from one of these rounds. Each round is two mutating, lifecycle runs of 120 calls on OpenRouter (2026-10-23), with `gpt-4.1-mini` answering and `gpt-4.1-nano` summarizing:

| Added | Facts kept | Folds committed | Saving with caching | What the result files showed |
|---|---|---|---|---|
| Current-facts summary, field/value pins | 667 / 667, 705 / 818 | 4, 4 | 42%, 45% | Run 2's first summary read "Noor replaces Mira" backwards: `owner: Mira (was Noor)` |
| A replaced value must not be current | 640 / 647, 695 / 695 | 4, 5 | 36%, 47% | The owner was kept; a summary dropped `production port: 9443`, which nothing pinned |
| Bare numbers pinned, rejection reasons logged | 452 / 452, 727 / 727 | 1, 2 | 22%, 37% | Nothing lost, but most folds rejected (`pinned_facts_present`) for old values the prior summary kept as history |
| Replaced values no longer required | 488 / 488, 510 / 510 | 3, 5 | 27%, 31% | Later folds passed; the first fold took 8 attempts, rejected for backwards readings (`superseded_not_current`) |
| One corrective retry; a field update replaces the field's last value | 793 / 811, 662 / 749 | 6, 6 | 48%, 46% | Folding flowed and reached the re-fold, and facts were lost again: two lines for one fact (`owner: Ines` and `project owner: Noor`), or a fact named only by the conversation's word (`ceiling: 1250` for the budget) |

"Facts kept" counts only facts last stated inside the folded turns, so it differs between runs. The trade-off is plain in the table: with checks strict enough to lose nothing, folding mostly stopped and the saving fell to 22–37%; once a retry let folds through, the summarizer's mistakes got through with them. The checks infer each fact's current value from wording, and a first statement ("Production listens on port 9443") is no update, so a later correction has no recorded value to replace.

One more round, with every safeguard above and `gpt-4.1-mini` summarizing instead of `gpt-4.1-nano` (`--summarizer openrouter/gpt-4.1-mini`), kept 715 / 715 and 720 / 720 facts. Its first fold passed on the first attempt, no fold was rejected, one corrective retry per run committed, and its summaries held one line per fact with every correction the right way round. But at four times nano's price the cost check refused 118 of 120 fold attempts in each run as unprofitable, so it committed 2 folds per run, saved 43% and 40% with caching, and never reached the fold cap or the re-fold. A stronger summarizer removes most of the mistakes here; it does not show that corrections survive repeated re-folds.

#### The fact ledger

What fixed it is a gateway that owns each fact's current value instead of inferring it from the summary: the summarizer compresses history, and the gateway keeps the facts (see "The gateway keeps the facts" above). Rounds with the ledger, on the same scenarios (OpenRouter, 2026-10-23, `gpt-4.1-nano` summarizing unless noted):

| Ledger version | Run | Facts kept | Folds committed / rejected | Saving with caching | What it showed |
|---|---|---|---|---|---|
| First | fixed, economics, 200 calls | 1441 / 1441 | 5 / 11 | 54% | Stable facts as before |
| First | mutating ×2 | 625 / 706; no folds | 4 / 6; 0 / 15 | 49%; −15% | A value taken from a turn stating two facts (`staging port: 9443`); one fact under two names (`staging` and `staging port`) |
| One-sentence evidence that names its fact; same-fact names | mutating ×2 | 417 / 417, 421 / 421 | 1 / 13, 1 / 12 | 22%, 21% | Nothing wrong; most folds refused for their evidence |
| same, `gpt-4.1-mini` | mutating | 710 / 710 | 2 / 0 | 41% | Nothing refused; the cost check let 2 folds through |
| same | fixed, economics, 200 calls | 1223 / 1347 | 7 / 7 | 55% | The budget correction filed as a new `ceiling: 1800` beside the kept `spending ceiling usd: 2400` |
| + stale facts refused; a correction cannot start a new name | fixed, economics, 200 calls | 1417 / 1417 | 4 / 14 | 47% | The stale check refused that fold, and its retry committed the right one |
| same | mutating ×2 | no folds | 0 / 15, 0 / 15 | −14%, −16% | Nothing wrong, nothing folded, and the summarizer paid every 8 turns |
| + growing back-off | mutating ×2 | no folds | 0 / 4, 0 / 4 | −3%, −3% | Four attempts instead of fifteen |

Then, with every rule above and `gpt-4.1-mini` summarizing, the cost check's `amortization_requests` (how many requests a fold's saving is counted over) was varied. In these runs each committed fold was in fact reused for about 21 calls:

| `amortization_requests` | Folds | Facts kept | Saving with caching | Reached |
|---|---|---|---|---|
| 4 (the bench profiles' setting) | 2 | 710 / 710 | 41% | warm |
| 8 | 4 | 863 / 863 | 49% | warm |
| 16 | 6 | 889 / 889 | 55% | re-folded |
| 16 (repeat) | 6 | 889 / 889 | 54% | re-folded |

At 16 both runs reached the fold cap at call 85, were sent "owner is now Ines" at call 86, and re-folded at call 106; the owner was right on every counted call after it, and no fold was refused. That is the correction-then-re-fold path, in a real run, twice.

Each fault a round found became a rule with its own test. What the rounds show: the ledger turns the summarizer's mistakes into refused folds rather than wrong answers, and the summarizer's quality then decides how much a conversation with corrections saves. With `gpt-4.1-nano` that is nothing; with `gpt-4.1-mini` it is 54–55%, once `amortization_requests` is set near how often a summary is really reused. A summarizer that costs as much as the main model needs a higher value than the default; one that costs a quarter of it, a lower one.

#### Task success: standing rules

Fact recall is not the whole job: a summary can keep every fact and still drop "never mention BIRCH-204". `tools/fold_tasks.py` runs the same paired calls on the mutating conversation, with five standing rules given in its user turns:
- answer in at most three `- ` bullets;
- never mention BIRCH-204 or Ana;
- end with `Ref: CEDAR-731`;
- from call 15, dates as DD.MM.YYYY;
- at call 60, a change to `Ref: CEDAR-731/B`.

Calls alternate between a status update and "which projects and budgets have come up?". A deterministic checker scores every answer against every rule in force. A call succeeds when it obeys all of them. A rule counts against folding only where it was folded away and the unfolded tenant obeyed it. The run exits `1` if the folded tenant broke such a rule, or, with `--max-success-drop`, if its success rate fell further than that below the unfolded tenant's. Run it with `python tools/run_fold_retention_local.py --provider openrouter --scenario constraints --profile lifecycle --max-calls 120`.

OpenRouter, 2026-10-24, `gpt-4.1-mini` answering, 120 calls each:

| Summarizer | Folds | Task success, unfolded → folded | Bullets rule, unfolded / folded | Footer and date rules | Saving with caching |
|---|---|---|---|---|---|
| `gpt-4.1-nano` | none | 34% → 31% | 82% / 79% | kept in both | −3% |
| `gpt-4.1-mini`, `amortization_requests: 16` | 7, re-folded | 32% → 13% | 82% / 59%; broken 30 times after it was folded away | kept in both | 58% |
| same, repeat | 8, re-folded | 40% → 53% | 87% / 91%; broken 10 times | kept in both | 54% |

What this shows:

- **The bullets rule is the one folding lost.** It was the rule the summarizer kept only as a sentence in the context note ("Standing rules include answers limited to three bullet points"). The footer and the date format, which it filed as ledger facts (`answer ending line: Ref: CEDAR-731/B`, `date format rule: DD.MM.YYYY`), were obeyed on every call, including after the footer changed.
- **The two mini runs disagree by 31 points of success.** The cost saving repeats; compliance with a format rule does not, at temperature 0. Judge from more than one run.
- **Even unfolded, the main model breaks "never mention BIRCH-204" whenever the question asks which projects came up**: 50% compliance on both tenants. Those calls never count against folding, and the low baseline success rate (32–40%) is mostly theirs. Compare the tenants with each other, not with 100%.

With standing instructions kept word for word in the ledger (see "The gateway keeps the facts" above), the same runs (2026-10-24):

| Summarizer | Folds | Task success, unfolded → folded | Bullets rule, unfolded / folded | Saving with caching |
|---|---|---|---|---|
| `gpt-4.1-nano` | none | 38% → 36% | 83% / 85% | −6% |
| `gpt-4.1-mini`, `amortization_requests: 16` | 5, frozen | 38% → **60%** | 85% / 98%; broken once after it was folded away | 43% |
| same, repeat | 8, re-folded | 42% → **58%** | 90% / 98%; broken twice | 54% |

The bullets rule went from broken 30 and 10 times to once and twice, and the folded tenant now follows the rules better than the unfolded one: they sit word for word at the top of its summary instead of at turn 8 of a long conversation. The footer, its change at call 60, and the date format were again kept on every call. Both mini runs still exit `1` under the strict gate, for those one and two slips; with `--max-success-drop 0` they pass, since the folded success rate is higher. `gpt-4.1-nano` still commits no fold.

### Tool turns

Everything above refuses a conversation whose older turns contain tool calls: agent conversations, usually the longest, never folded. `folding.fold_tool_turns: true` (default `false`) lets them fold, under the same rule as the fact ledger — the summarizer proposes, the gateway verifies, and the gateway writes the summary. With the setting off, the same turns are refused exactly as before; it works under `fold_mode: shadow` as well as `on`.

A **tool exchange** — one assistant message with `tool_calls` and every `Tool` message answering those call ids — is folded or kept whole. If the `keep_last` boundary would land inside one, it moves earlier so the exchange stays in the tail. A folded request is checked for split exchanges (`tool_exchange_split`): a `Tool` result whose call is not in the request, or an assistant `tool_calls` message missing a result, is a bug, so it fails the fold and is logged rather than repaired.

What the summary keeps for each folded exchange is written by the gateway from the messages, never by the summarizer, under `Tool results folded away (call the tool again to see one in full):`

```
- read_file(path="src/folding/plan.rs") -> ok, 21,480 chars
- run_tests(filter="shadow") -> error, 3,112 chars
```

Each line gives the tool name, its arguments as compact readable text cut to a fixed length (a hash is kept for de-duplication), the result's size in characters, and whether it looks like an error — decided deterministically: a JSON result with a top-level `error` key is an error; anything else is not. The complete results are gone; the line says how to get one back. Like instructions and facts this section is never cut, and it counts towards both `summary_chars` (the fold is refused under `within_length_bound` if they pass it, not truncated) and the store's `max_total_chars`. Tool records carry forward across later folds and re-folds unchanged.

The summarizer may also propose `tool_facts`: `{call_id, path, value}` where `path` is a JSON pointer such as `/items/0/status`. The gateway accepts one only if `call_id` names a folded call and, for a JSON result, the value at `path` equals `value` by typed comparison — or, for a text result, `path` is empty and `value` is an exact substring of bounded length. A rejected tool fact fails the summary like a rejected fact update (`tool_fact_evidence`) and gets the corrective retry. Tool arguments and accepted fact values pass through the same key-like detection the gateway applies to secrets: a fact whose value looks like a key is refused, and an argument containing one is redacted in the ledger line.

What is dropped: the full result text, in every case. What is kept: the call, its compact arguments, the result's size and error status, and only the facts the gateway could verify against the result itself.

`tools/fold_tools.py` is the paired-tenant agent benchmark: a scripted investigation — five harness-served calls returning several KB of JSON each, one an error — followed by questions answerable only from inside those results, with the same `tools` still offered. A question counts against folding only where the result it needs was folded away and the unfolded tenant answered correctly; a call the model repeats for something already fetched is counted a refetch; provider cost is priced with prompt caching as above. Run it with `python tools/run_fold_retention_local.py --provider openrouter --scenario tools --profile lifecycle --summarizer openrouter/gpt-4.1-mini`. The result files are in `benchmark_results/`.

OpenRouter, 2026-10-26, `gpt-4.1-mini` answering and summarizing, lifecycle profile, 60 calls each:

| Run | Folds committed | Eligible answers, folded correct | Accuracy, unfolded / folded | Tool calls (refetches), unfolded / folded | Provider cost with caching, unfolded → folded | Saving |
|---|---|---|---|---|---|---|
| 1 | 1 (2 rejected, 17 unprofitable) | 25 / 32 | 82% / 70% | 4 (1) / 5 (1) | $0.0478 → $0.0512 | −7% |
| 2 | 1 (2 rejected, 17 unprofitable) | 24 / 32 | 82% / 70% | 4 (1) / 5 (1) | $0.0476 → $0.0507 | −7% |

What this shows:

- **Values the gateway verified were answered from the summary.** The image tag, the replica count and the pass count, all kept as `tool_facts`, were answered correctly on 50 of 51 eligible calls across both runs after their results were folded away, with no tool call. The one miss (run 2, call 29) was the right number written as a sentence ("39 tests in checkout/totals passed."), which strict typed scoring counts wrong.
- **A value the summarizer did not pick was lost, and the model guessed instead of re-calling.** Both runs kept `/failed/0/got` and `/failed/0/want` but not `/failed/0/line` (1043). "At which line does totals/rounds_partial_refund fail?" was then wrong on 7 of 7 calls in each run. On 12 of those 14 the model answered without calling any tool ("fails at line 12.81", "at line 39"), although the summary says the full result is gone and how to get it back. On the other 2 it called `read_file` on a test file never fetched; it never called `run_tests` again. Both runs exit `1`.
- **The error-message and p99 questions were never eligible.** Their results arrived after the only fold, so they were still verbatim; these runs say nothing about them.
- **Folding did not pay here.** The folded tenant's main calls cost 28–31% less ($0.0345 and $0.0330), but the summarizer cost $0.0167–0.0177, so the total was 7% higher. The cost check refused 17 fold attempts per run as unprofitable and only one fold committed, so neither run reached the fold cap or a re-fold. Carry-forward across later folds was not exercised by a live run, only by unit tests.
- **The scenario is synthetic and small:** five scripted calls, six questions, one model. It shows that verified tool facts survive a fold and that an unchosen one does not; it does not show how often real agents need a value the summarizer left out.

Two earlier runs, with a summarizer prompt that showed each tool result on one line, committed no fold at all (result files from 2026-10-26T07:44 and T08:00): the summarizer quoted whole JSON results as fact-update evidence and ran out of tokens every time, at a 512- and then a 1024-token reply budget. Results are now shown to the summarizer one key per line, and its reply lists `tool_facts` before `updates`; the bench tenants' `summarizer_max_tokens` is 1024. Those two runs cost 10% and 20% more than unfolded, and both exit `2` (no eligible question).

Not measured: `fold_mode: shadow` with tool turns (covered by tests only), a re-fold over tool turns, text (non-JSON) tool results in a live run, a provider other than OpenRouter, and a summarizer other than `gpt-4.1-mini`.

## Receipts

To see all of this end to end, run the receipts demo: `python demo/receipts/run_local.py`,
or `docker compose up` in `demo/receipts` (see its [README](demo/receipts/README.md)).

A receipt is a Wicket chain entry: a signed, hash-linked record that a
decision happened. It is separate from the decision row in Postgres. Postgres
keeps quotas, holds, and decision rows. It does not keep the chain head.

The kernel is the vendored `infinity-kernel` crate
(`vendor/wicket/infinity-kernel`), called in-process. Receipt bytes are the
kernel's. Nothing new is added to them. The verifier is the unchanged
`vendor/wicket/verifier/ickverify.py` from
`warheart1984-ctrl/Wicket` commit `bcb0e23f386af61345adb0b1cc9ce72999c6307f`.

**One chain per tenant, per replica.** Each tenant with receipts on has its
own folder, `<[receipts] directory>/<tenant_id>/`, with its own
`receipts.jsonl`, `anchor.jsonl`, Ed25519 key (`signing.priv`, mode 0600 on
Unix; `signing.pub` next to it) and lock. A tenant's evidence is complete on
its own: it can be handed to that customer, and checked, with nothing of any
other tenant in it, not their entries, key ids or traffic. Every tenant's
chain is verified at startup; one added by a reload is opened and verified on
its first request. A tenant id becomes a folder name only if it is 1 to 64
letters, digits, `.`, `_` or `-`, starting with a letter or digit; otherwise
its requests get `503 receipt_unavailable`. The replica holds `replica.lock`
in the root, so a second process pointed at the same root is refused. Locks
are on their own files, so logs stay readable while the gateway runs, on
Windows too. A shared chain left in the root by an earlier version is no
longer written; the gateway warns at startup until it is archived.

**A tenant downloads its own evidence.** `GET /v1/receipts/log`,
`/v1/receipts/anchor` and `/v1/receipts/key` return the caller's tenant's
files as stored, up to their last complete line, for a credential with the
`receipts:read` scope. The tenant comes from the credential, never from the
request, so no request can name another tenant's files. A tenant without
receipts gets `404 receipts_not_enabled`. The key served here is only as
trustworthy as where it came from: compare it with the copy published at
first sight (below).

**What gets a receipt**, on `POST /v1/chat/stream` and `POST /v1/chat/complete`,
for a tenant with `receipts: true`:

| Gateway event | Kernel verdict |
|---|---|
| Reservation (the request is admitted) | `allow` |
| Governance refusal: `402` budget, `403` not allowed, `429` rate or concurrency | `deny` |
| Approval hold (`202`) | `await_human_approval` |
| The answer ends, one way or another (below) | `outcome` (`completed` or `failed`) |

**How each ending is recorded.** Every one of these is tested end to end:

| What happened | Outcome | Reply digest |
|---|---|---|
| Answer delivered in full | `completed` | of the whole answer |
| Provider refused before answering (`502`) | `failed` | none: nothing came back |
| Provider failed part-way (stream gets an `error` event, then `end`) | `failed` | of exactly what the client received |
| Client hung up mid-stream | `failed` | of what was produced before the hang-up |
| Client hung up while a completion was pending | none: the `allow` stays open | — |
| Gateway killed after the `allow` | none: the `allow` stays open | — |

An `allow` with no outcome means the gateway cannot say how the call ended,
and the log says exactly that: `ickverify.py` reports "N allowed calls have no
outcome". A repeat of a finished completion under the same `Idempotency-Key`
is served from the ledger without calling the model and writes nothing new;
the original `allow` and outcome cover it, and the replay returns that
`allow`'s `x-receipt-id` and `x-receipt-salt` (kept with the stored answer,
sealed at rest like it) so the caller can still check the digests.

A malformed or oversized body (`400`, `413`), a duplicate (`409`), a
mismatched resubmission (`422`) and an unauthenticated request (`401`) are
not decisions and leave no receipt. An approved hold, sent again, is admitted
like any request and gets its own `allow`.

**A `deny` says why.** The gateway decides its own refusals, so it records
them with the kernel's `record_external_denial`: `reason_codes` are
`["EXTERNAL_DENY", <code>]`, where the code is the refusal's `error.code` in
upper case (`BUDGET_EXHAUSTED`, `RATE_LIMITED`, `MODEL_NOT_ALLOWED`, …).
`EXTERNAL_DENY` marks it as decided outside the kernel; the kernel's own codes
never start with it. `allow` and `await_human_approval` still come from the
kernel's `evaluate`.

**Hashes are salted.** The prompt and the reply are never written. Each
admitted request gets 32 random bytes, returned to the caller once in the
`x-receipt-salt` response header (hex), next to `x-receipt-id`, the id of its
`allow`. The gateway does not keep the salt. Someone who can read the log
cannot test a guessed prompt against it; the caller, who has the request,
the reply and the salt, can recompute both digests:

```text
request  = sha256( salt || "llm-gateway/request/v1\n" || request body as sent )
reply    = sha256( salt || "llm-gateway/reply/v1\n"
                   || sha256(content) || sha256(reasoning) || sha256(tools) )
raw reply (passthrough)
         = sha256( salt || "llm-gateway/raw-reply/v1\n" || every byte relayed )
```

`content` is every `token` concatenated and `reasoning` every `reasoning`
concatenated. `tools` is, for each tool index in ascending order, the index
in decimal, then `0x1F id 0x1F name 0x1F arguments 0x1E`, where `name` and
`arguments` are that index's deltas concatenated and `id` is the first
non-empty id sent for it. For `/v1/chat/complete`, the same pieces come from
the JSON answer: `content`, `reasoning` (empty if null), and `tool_calls` with
each call's position in the array as its index. The `allow` carries the
request digest as `call_digest`; the outcome carries both. Each is written
`sha256:<hex>`.

**Folded calls.** For a tenant with receipts and conversation folding, a
folded response also carries `x-gateway-folded` (turns replaced) and
`x-gateway-fold-summary`: the summary the model was sent, as UTF-8 in
base64url without padding. Its `allow` still binds the body the client sent;
its outcome binds what the model was sent:

```text
request (folded) = sha256( salt || "llm-gateway/folded-request/v1\n" || call_digest
                           || "\n" || turns_replaced || "\n" || summary )
```

The forwarded messages follow from the body, the number of turns replaced and
the summary, so the caller can check this without reproducing the gateway's
JSON. A request that runs a fold also carries `x-gateway-fold-receipt-id`
and `x-gateway-fold-covered`, the turns the fold set out to cover.
That fold is a model call of its own, so it gets its own `allow`, written
before the summarizer is called (if it cannot be written, nothing is folded
and the request meets the stopped chain), and its own outcome, under the same
salt:

```text
fold allow   = sha256( salt || "llm-gateway/fold-request/v1\n" || call_digest || "\n"
                       || turns_covered_before || "\n" || turns_covered_after || "\n" || prior summary )
fold outcome = completed, reply = sha256( salt || "llm-gateway/fold-reply/v1\n" || summary )
               or failed with no reply, when the fold was rejected or failed
```

`tools/verify_fold.py` recomputes all of these from the body, the headers, and
the previous response's headers (for the summary being extended). A request
that runs a fold writes four entries instead of two.

A fold runs before the request's own admission, so a request can be refused
(402, 429, a reused `Idempotency-Key`) after its fold was paid for. That
refusal has no `x-receipt-id`, since no `allow` was written for the request,
but it still carries `x-receipt-salt`, `x-gateway-fold-receipt-id`,
`x-gateway-fold-covered`, and `x-gateway-fold-summary` if the fold committed,
so the caller can check the fold it was charged for. `verify_fold.py` then
checks only the fold's entries. Someone reading only the
log cannot tell a fold's `allow` from a request's: the action is inside the
proposal hash. The caller can, from the headers.

**Fail closed when receipts are on.** If the `allow` cannot be written, the
reservation is released, the model is not called, and the caller gets `503`
`receipt_unavailable`. If the closing outcome cannot be written, a normalized
stream ends with an `error` event instead of `end`, and a passthrough stream
ends with an error instead of a clean close: the text already sent cannot be
recalled, and the log shows an `allow` without an outcome. A completion
whose outcome cannot be written is withheld (`503`) and is not kept for an
idempotent replay, so a retry cannot fetch it; the provider's usage is still
billed. A `deny` that cannot be written turns the refusal into a `503`. After any failed write the
chain stops, and every request that needs a receipt gets `503` until the
process restarts and the log verifies. There is no fail-open switch. Tenants
with `receipts: false` (the default) are unchanged: a full decision queue
drops the row, counts it in `gw_decisions_dropped_total`, and still returns
the refusal.

**Writing.** One writer thread owns the log. A request hands it the entry
and waits without holding a runtime thread. The writer takes every entry
waiting, appends them in order, flushes the disk once, and only then tells
each request it was written. On one Windows desktop SSD, in a release build:
about 320 receipts a second one at a time, 1,300 with 16 concurrent requests,
and 3,000 with 128. A streamed request writes two (`allow` and outcome).
`cargo test --release --lib receipt_throughput -- --ignored --nocapture`
measures it on your disk.

**Visibility.** The chain's state is visible in metrics and health. Counters,
all labelled by the configured tenant id:

- `gw_receipts_written_total{tenant}` — entries this process durably wrote
  (entries already in the log at startup are in `gw_receipt_entries`).
- `gw_receipt_write_failures_total{tenant}` — jobs answered `Unavailable`:
  their write failed, or the chain had already stopped.
- `gw_receipt_recoveries_total{tenant}` — torn tails quarantined at startup.

Gauges, also labelled by tenant:

- `gw_receipt_chain_stopped{tenant}` — `0`/`1`, whether the chain refused
  further appends.
- `gw_receipt_entries{tenant}` — entries in the log.
- `gw_receipt_anchor_age_seconds{tenant}` — seconds since the last anchor
  this process wrote; `-1` before the first.

`GET /health/ready` gains a `receipts` object:
`{ "enabled": true, "tenants": 3, "stopped": ["acme"] }` (or just
`{ "enabled": false }` when receipts are off). A stopped chain does not make
the whole gateway unready — other tenants still work — but it is listed, and
the writer logs a warning once when a chain stops.

**Checking and anchoring.** Startup verifies the whole chain and every anchor
once, then keeps the head in memory. A request never reads the log. After
`anchor_every` new entries, after `anchor_interval_secs` with anything new,
and at shutdown, the writer reads back only what it wrote since the last
check, confirms each entry's hash, link and signature and that the disk ends
at the head in memory, then appends a signed anchor for that head. A log that
fails this check stops the chain. The file is never split: the verifier needs
one chain whose first entry has no predecessor, so startup verification grows
with the log (about 0.17 ms per entry measured, about three minutes per
million entries). Archive the directory and start a new one, with a new key
listed in your trusted keys, before that matters.

`[receipts] startup_verify` controls how much startup checks: `full` (the
default) verifies every entry; `since_anchor` only re-verifies entries
written after the newest local anchor, so startup stays bounded for a large
log. Tampering before that anchor is not caught at startup — it is still
caught by `ickverify.py` against the published anchor, and by
`startup_verify = "full"`. Neither weakens protection against someone who
holds the signing key, since they could re-sign either way. `since_anchor`
bounds the hashing and signature checks, not the reading: startup still reads
and parses the whole log, so its memory grows with the log either way.

**After a crash.** A kill can interrupt a write, leaving the log or the anchor
file ending part-way through a line. Startup handles that by one fixed rule,
and refuses to start in every other case:

- Only an unterminated last line is considered. A file ending in a newline is
  never changed; a complete line that does not verify refuses to start.
- That fragment was never acknowledged: the writer answers only after the
  whole batch, newlines included, is on disk, and anchors cover only entries
  read back complete.
- A fragment that parses as a whole JSON record is not removed (it could be a
  committed entry that lost only its newline): startup refuses.
- If an anchor counts more entries than are complete on disk, an anchored
  entry is damaged: startup refuses without changing anything. So does an
  anchor line that cannot be read, since then it is unknown what it covers.
- Nothing is deleted. The fragment is moved to
  `quarantine/<file>.<hash>.fragment`, and `recovery.jsonl` records the file,
  the bytes kept, and the fragment's size, hash and quarantine name, before
  the file is cut; on Unix the folders holding them are synced too, so a power
  cut cannot lose their names. Running it again changes nothing and records
  nothing twice.

```bash
python3 vendor/wicket/verifier/ickverify.py \
  /var/lib/llm-gateway/receipts/acme/receipts.jsonl \
  --anchor /var/lib/llm-gateway/receipts/acme/anchor.jsonl \
  --trusted-keys /var/lib/llm-gateway/receipts/acme/signing.pub \
  --require-signatures
```

**Anchors only help where the replica cannot reach them.** The local
`anchor.jsonl` sits next to the log and the key, so it does not stop someone
who can edit all three. `tools/anchor_publisher.py`, run as a separate
account, pushes each tenant's anchors and public key to a git repository the
gateway operator cannot write, never force-pushes, refuses to publish over a
rewritten anchor or key, and stops if the published history is rewritten;
its `verify` command checks a log against what was published.
[docs/anchoring.md](docs/anchoring.md) sets out the accounts, the credential
and the repository protection that make that boundary real, and what it
still does not cover. Entries newer than the last copied anchor can be
removed without detection; `anchor_interval_secs` bounds how long that window
stays open.

```toml
[receipts]
enabled = true
directory = "/var/lib/llm-gateway/receipts"
anchor_every = 1000          # `rotate_every` is still accepted
anchor_interval_secs = 60
```

```yaml
# tenants.yaml
receipts: true   # fail closed for this tenant
```

A tenant file with `receipts: true` while `[receipts] enabled` is false
refuses to start.

## HOLD: requests that wait for approval

Between GO and NO-GO there is a third decision. A tenant's policy can say
that some requests wait for a person: requests for certain models, or whose
worst-case cost is above a threshold.

```yaml
# tenants.yaml, under a tenant
holds:
  models: ["openrouter/gpt-4.1-mini"]   # registry model ids, globs
  above_nano_usd: 5000000000            # worst case above 5 USD
  expiry_secs: 900                      # how long a hold waits (max 3600)
  approval_valid_secs: 300              # how long an approval stays usable (max 3600)
  max_pending: 20                       # held requests waiting at once
```

1. **Held.** A matching request is not executed and not charged. The caller
   gets `202` with `{"status": "held", "hold": {...}}`, a `Hold-Id` header
   and `Location: /v1/holds/{id}`. The hold records who asked, the model, the
   output cap, the worst-case cost and which rule matched. It never records
   the prompt: the client keeps its request.
2. **Decided.** A key with the `approve:holds` scope approves or denies it
   (`POST /v1/admin/holds/{id}/approve` or `/deny`, optionally with
   `{"note": "..."}`). The scope must be granted by name. `admin` does not
   imply it, a legacy wildcard grant does not, and **no key can decide its
   own request**. `GET /v1/admin/holds?state=pending` is the queue.
3. **Executed once.** The client polls `GET /v1/holds/{id}` and, once it is
   `approved`, sends **the same request** again with `Hold-Id: <id>`.

What the approval covers, and what it does not:

- **Exactly the request approved.** The hold stores the request's keyed
  fingerprint, and the approval is bound to it, to the model, and to the
  approved worst-case cost. Another prompt, a bigger `max_tokens`, the other
  endpoint, or a price rise since approval is `422 hold_mismatch`.
- **Once.** The approval is consumed in the admission transaction, with the
  budget: twenty copies sent at once across two replicas execute one (tested).
  A refused admission (budget, rate, concurrency) does not use it up. If the
  provider refuses before accepting anything, the approval is given back,
  like an idempotency key. Once anything was executed, it stays used.
- **Nothing else is waived.** At execution the key, the tenant, the model
  policy, the budget and the limits are all checked again. An approved
  request from a tenant switched off since is refused.
- **No money is held while waiting.** Charging a pending hold would let a
  flood of requests nobody approved exhaust a tenant's budget. The cost: an
  approved request can still be refused for budget (`402`).
- **Every transition is recorded.** Requested, approved, denied, expired and
  used each go on the decision record (kind `hold`) in the same transaction
  as the transition. If the record cannot be written, the transition does
  not happen: an approval with the ledger down is a `503`, and the hold stays
  pending.
- **Time limits.** A pending hold expires after `expiry_secs`; an approval
  after `approval_valid_secs`. Both read as `expired` the moment they lapse,
  and the sweeper records the expiry.
- **Where holds live.** In the ledger: durable on `sqlite` and `postgres`,
  shared across replicas on `postgres`. Under quota split each replica has
  its own holds, so a tenant's requests, polls, approvals and resubmissions
  must reach one replica; the gateway warns at boot. A hold presented to the
  wrong replica is not found and nothing runs.

Approvers are notified by polling the queue. Signed webhooks are not built.

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
# Largest answer held in memory: a whole non-streaming response, or one
# streamed event or line. Beyond it: 502 upstream_protocol_error.
max_response_bytes = 8388608

[governance]
# On: a tenant may call only the models in its `allowed_models`, and an empty
# list means none. Off: an empty list means any registry model, but a
# non-empty list still limits that tenant, and its folding `summarizer_model`.
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
container work. `prompt_overhead_tokens:` is what a vendor adds around every
prompt (its chat template, a default system prompt), which the gateway's
estimate cannot see: Groq's gpt-oss bills a short prompt at ~79 tokens
against ~13 estimated. It is added to the reservation only, so the budget
check covers it; the bill is still the vendor's reported usage. The live
suite fails when a vendor's overhead outgrows the catalogue's number.

`config/tenants.yaml` holds credentials, scopes, allow/deny lists, limits,
per-model policy, and approval holds. Keys are referenced by env var (`key_env`) or by file
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
| **Request size** | Bodies over `request_body_limit_bytes` get a 413 on the declared `content-length` alone, before any byte is read. Headers use hyper's defaults: more than 100 headers is a 431 (tested); the header block is capped at hyper's ~400 KB read buffer. Upstream answers are bounded too: a non-streaming response, one streamed SSE event or one JSON line over `upstream.max_response_bytes` (8 MiB) fails as `upstream_protocol_error` instead of growing a buffer; an upstream error body is read only as far as it is shown (16 KiB). | Set tighter header limits at the proxy (e.g. nginx `large_client_header_buffers`) if you need them. |
| **Operator endpoints** | `/metrics` names every tenant's spend, so on the public port it requires the `admin` scope. Set `server.ops_port` to move `/metrics` and `/v1/admin/*` to a separate listener (loopback by default); they then return 404 on the public port. The reload and the decision record still require `admin` there, and hold decisions `approve:holds`. | Bind the ops listener to a private interface or network and scrape it from inside. |
| **Log redaction** | Logs carry one structured summary per stream: ids and counts, never credentials or message content. An integration test captures every log line from the whole suite at `llm_gateway=trace`, including refused and malformed requests and bearer auth, and fails if a key or a prompt marker appears. | Keep the gateway's filter at `debug` or above for other crates; hyper/reqwest `trace` output is not covered by that test. |
| **Upstream error bodies** | A vendor's error body can name the operator's provider account, its billing state or a key fragment, so it never reaches a tenant. Clients get the stable `code`, status and `retryable` flag plus a fixed sentence per class, such as "the upstream provider refused the request (HTTP 404)" or "the upstream provider is overloaded; retry shortly (HTTP 503)", in the error body and the SSE `error` event alike. The decision record keeps the same sentence. The vendor's text goes to the logs only (`request rejected before streaming` / `upstream failed mid-stream`), bounded to 1,000 characters with key-shaped tokens (known key prefixes, `Bearer` values, long opaque strings) replaced by `[redacted]`. Tested with a mock upstream whose 404 and in-band errors carry an account id and a key. | `passthrough` framing relays the vendor's bytes verbatim once the stream has started, in-band errors included; use `normalized` for tenants who must not see raw vendor output. |
| **Secret rotation** | `key_file` credentials are re-read on every tenant reload, so replacing the file and reloading rotates a key with no restart; the old key stops working the moment the reload lands (tested). `key_env` values, provider API keys and the JWT secret are read once at boot, and rotating them means a restart. There is no dual-secret window for JWTs. | Rotate tenant keys without a gap by adding the new credential alongside the old, moving clients, then removing the old one. |
| **Metric cardinality** | Every label value comes from configuration — provider, registry model id, tenant id from `tenants.yaml` — never from request input. A series is created only after authentication and routing succeed. Labels with delimiter characters or over 128 bytes become `invalid`. Tested with unknown models, unknown keys and junk metadata. | Cardinality is bounded by the size of the model and tenant files; size scrape limits accordingly. |
| **Request ids** | Every response carries `x-request-id`. Without a client one it is the gateway's own id, the one in the decision log and the receipts. A client-supplied `x-request-id` (≤ 128 visible ASCII chars) is echoed instead and logged as `client_request_id` beside the gateway's `request_id`. It is not an identity and is never trusted for anything else. | Overwrite it at the proxy if clients must not choose it. |

## Testing

```bash
cargo test --locked --all-targets                  # 302 tests
cargo clippy --locked --all-targets -- -D warnings
cargo bench --bench framing                        # add `-- --quick` for a fast pass
cargo bench --bench load                           # gateway overhead and ceilings under concurrent streams
```

`benches/load.rs` streams from a paced fake upstream at 50, 200, 500 and
1,000 concurrent clients spread over 10 tenants, first directly and then
through a gateway on each ledger (memory and SQLite, plus Postgres when
`LLM_GATEWAY_TEST_DATABASE_URL` is set). It reports what the gateway adds to
time-to-first-token, reservations settled per second, refusals by status,
reserved and billed totals, and heap per open stream. After every level it
waits for the ledger to settle and reconciles, per tenant, what the clients
were told against the ledger's reservation rows and its daily totals; any
disagreement fails the run. Upstream, gateway, clients and database share one
machine over loopback, so the results show the gateway's own overhead and
limits on that machine, not production numbers. The module docs list the
`LOAD_*` knobs; each run appends to `target/load-report.jsonl`.

For separated-host measurements and RFC-LLMGW-001 evidence bundles, use the
external runner documented in [docs/cloud-benchmark.md](docs/cloud-benchmark.md).
It runs a one-client smoke test first, then emits sanitized request records,
machine metadata, governance evidence, and `SHA256SUMS` without changing the
local load harness.

CI (`.github/workflows/ci.yml`) runs that gate from a clean checkout on Linux
and Windows: the full suite three times over, the money tests five more times,
clippy with warnings as errors, a build on the declared `rust-version`, the
benchmark in quick mode, and the docker compose demo end to end. A separate
`shared-ledger` job runs the pressure suite three times against a real
Postgres, with the database made mandatory so those tests fail rather than
skip if it is missing.

The Postgres tests skip unless `LLM_GATEWAY_TEST_DATABASE_URL` is set. To run
them locally:

```bash
docker run -d --name llmgw-pg -e POSTGRES_PASSWORD=gatewaytest -e POSTGRES_DB=gateway -p 127.0.0.1:55432:5432 postgres:16
```

```bash
LLM_GATEWAY_TEST_DATABASE_URL="postgres://postgres:gatewaytest@127.0.0.1:55432/gateway?sslmode=disable" cargo test --test pressure
```

The SQLite ledger, lease and quota-split tests need no server and always
run. Tests that count within one rate window wait until at least 15 s of the
current minute remain, so they cannot straddle a boundary.

`tests/live.rs` calls the real Groq, OpenRouter and NVIDIA APIs with the
model the shipped catalogue names for each. It checks shape and money, never
content:
- a streamed answer follows the v1 contract and is billed exactly what its
  reported usage costs at catalogue prices;
- **the reservation made before the call covered the bill**;
- a completion is one well-formed document, billed the same way;
- passthrough relays the vendor's stream and bills the reservation;
- a client hanging up mid-stream frees its slot and is billed for what it
  received;
- a model the vendor does not know is a clean 502 that costs nothing.

A refusal the vendor marks retryable (overloaded, rate limited) is retried
twice after a pause; anything else fails at once.

It spends money, so it is opt-in: nothing runs unless `LLM_GATEWAY_LIVE`
names the providers, and a named provider without its key fails rather than
skips. Each test's tenant has a 5-cent daily budget, so a runaway test is
refused by the gateway itself; a full run costs well under a cent.

```bash
LLM_GATEWAY_LIVE=all cargo test --test live -- --nocapture
```

Each run appends estimated vs billed prompt tokens and reserved vs billed
cost to `target/live-report.jsonl`. The `live` workflow runs it nightly and
on demand from repository secrets (`GROQ_API_KEY`, `OPENROUTER_API_KEY`,
`NVIDIA_API_KEY`), outside the gate: it depends on three vendors being up.
Its checks are themselves tested on every normal run, against a local server
that speaks the vendors' wire format, with no key and no cost.

`tests/crash.rs` starts the real `llm-gateway` binary as a separate
process, kills it outright (no destructors, no graceful shutdown) at each
critical point of a request, restarts it on the same SQLite ledger, and
reads the ledger file directly. It covers a kill mid-upload before
admission, after reserving but before any answer, mid-stream, and a retry
with the same idempotency key across the crash.

`tests/pressure.rs` is the boundary pressure suite: one test per attack on
the claim that an unauthorised or financially inadmissible request never
reaches the provider, checked by counting what the provider actually
received. It covers restarts, replicas and replays on each ledger backend,
a database cut mid-run, and approval holds. Tests named `gap_*` pin what still fails open by
design: restarts and replicas on the in-memory ledger, which is per-process.
The claim → test → result table is
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
    ledger/        spend ledger: memory, sqlite, postgres; answer sealing
  api/             HTTP surface: SSE stream and JSON completion
  observability/   structured logging, Prometheus metrics
  bootstrap.rs     startup wiring
migrations/        ledger schema, one folder per database
examples/
  mock_upstream.rs a stand-in provider for the demo
demo/              docker compose demo: config, secrets, scripted client
```

## License

MIT. See [LICENSE](LICENSE).
