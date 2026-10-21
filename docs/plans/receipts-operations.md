# Plan: receipts in production (operations)

**Branch:** `opencode/receipts-operations`, stacked on `claude/openai-compat`
(PR #46, which sits on #44 → #43 → #42). Rebase onto `master` once those merge.
**Builder:** OpenCode. **Reviewer:** Claude Code, after handoff (section 6).
**Goal:** an operator can run receipts in production and see when something is
wrong: a failed disk write is proven to stop the chain, the chain's state is
visible in metrics and readiness, startup time no longer grows with the log
unless the operator asks for that, OpenAI SDKs can list models, and the anchor
publisher reports its own state.

Read this whole file before writing code. Sections 2 and 3 are the contract.
Section 7 lists the traps; read it before Phase 3.

---

## 1. Why, and what is out of scope

Receipts are written, signed, anchored and published (see "Receipts" in
`README.md` and `docs/anchoring.md`). What an operator still cannot do:

- know that a **real** disk error stops the chain. Today the only test hook
  (`fail_appends_after`) fails a write *without* touching the disk, so the
  stop-on-error path is covered only by reading the code;
- **see** the chain's state: nothing in `/metrics` or `/health/ready` says
  whether a tenant's chain has stopped, how old its last anchor is, or how
  many recoveries happened at startup;
- start quickly with a large log: startup verifies every entry
  (about 0.17 ms each; three minutes per million);
- list models from an OpenAI SDK (`client.models.list()`);
- monitor the anchor publisher without reading its stderr.

**Out of scope (do not touch):**
- The receipt format, the vendored Wicket kernel (`vendor/wicket/`) and
  `ickverify.py`. No new field, no new entry type, no edits under `vendor/`.
- Billing, limits, holds, the SSE contract, the OpenAI chat endpoint's shape.
- Per-tenant layout and `/v1/receipts/*` (done in #43).
- Splitting a log into several files. The verifier needs one chain whose first
  entry has no predecessor; do not change that.
- Holds for OpenAI clients (a product decision, not part of this plan).

---

## 2. Invariants that must still hold

All are tested today; every existing test must still pass unchanged.

1. **Fail closed.** If an `allow` cannot be durably written, the model is not
   called and the caller gets `503 receipt_unavailable`. After any failed
   write the chain stops and every later request that needs a receipt gets
   `503` until restart. There is no fail-open switch, and none may be added.
2. **Nobody hears "written" before the flush.** The writer thread answers a job
   only after `write_all` + `sync_data` of its batch succeeded.
3. **Request threads never block on the disk.** All receipt I/O stays on the
   writer thread (`src/receipts/mod.rs`, `Writer`).
4. **Receipt bytes are the kernel's.** Entries come from `issue_receipt` /
   `issue_outcome` / `record_external_denial`; the unchanged `ickverify.py`
   accepts every log the gateway writes.
5. **Recovery never destroys evidence.** The torn-tail rule in
   `recover_torn_tail` (only an unterminated last line; never a fragment that
   parses as JSON; never past what an anchor covers; bytes quarantined before
   the cut; idempotent) is unchanged.
6. **Metric labels come from configuration only** (tenant ids from
   `tenants.yaml`), never from request input. See "Metric cardinality" in the
   README's security table.

---

## 3. Design

### 3.1 A real disk failure (Phase 1)

Add a test-only way to make the writer's **actual** file write fail, so the
same `Err` path a full disk takes is exercised end to end.

- In `src/receipts/mod.rs`, give `Writer` an injectable failure that makes the
  next `write_all` **or** `sync_data` return an `io::Error`
  (`ErrorKind::Other`, message `"injected disk failure"`), through the real
  `commit()` code path. Not `take_failure()` (that one never touches the disk
  and must stay as it is for the tests that use it).
- Expose it as `ReceiptChain::fail_disk_writes_from_now()` behind
  `#[doc(hidden)]`, like `fail_next_appends`.
- Do not add any production behaviour. With the hook unused, nothing changes.

### 3.2 Visibility: metrics and readiness (Phase 2)

**Metrics** (in `src/observability/metrics.rs`, rendered by `render()`), all
labelled by `tenant` (from configuration):

| Metric | Type | Meaning |
|---|---|---|
| `gw_receipts_written_total{tenant}` | counter | entries durably written |
| `gw_receipt_write_failures_total{tenant}` | counter | jobs answered `Unavailable` or `Io` |
| `gw_receipt_chain_stopped{tenant}` | gauge 0/1 | the chain refused further appends (`is_failed()`) |
| `gw_receipt_entries{tenant}` | gauge | `ReceiptChain::len()` |
| `gw_receipt_anchor_age_seconds{tenant}` | gauge | seconds since the last anchor this process wrote; `-1` before the first |
| `gw_receipt_recoveries_total{tenant}` | counter | torn tails quarantined at startup |

Read the gauges at render time from the `ReceiptBook` (open chains only); do not
add locks on the request path. Counters can live in `Shared` (atomics already
there) and be read the same way. Only tenants whose chain is open appear.

**Readiness** (`/health/ready` in `src/api/system.rs`): add a `receipts`
object to the JSON, both when ready and when not:

```json
"receipts": { "enabled": true, "tenants": 3, "stopped": ["acme"] }
```

A stopped chain does **not** make the whole gateway unready (other tenants
still work), but it must be listed, and a WARN log line must be emitted once
when a chain stops (there is one in `Writer::fail` already: keep it).
`enabled: false` when `[receipts] enabled` is false; then no other fields.

### 3.3 Bounded startup (Phase 3)

New setting `[receipts] startup_verify`, `"full"` (default, today's behaviour)
or `"since_anchor"`.

With `since_anchor`, opening a chain:

1. Recovers a torn tail exactly as today (`recover_torn_tail`, unchanged).
2. Parses every line (cheap) but **verifies hashes, links and signatures only
   from the newest local anchor onward**:
   - the newest anchor line must verify (`TrustedKeys::verify_anchor`, the
     replica's own key) and its `head_receipt_id` must equal the
     `receipt_id` of entry number `count` in the log;
   - every entry after it must have a correct hash
     (`verify_receipt` / `verify_outcome`), link to the entry before it, and
     carry a valid signature by this replica's key;
   - an outcome after the anchor that answers an `allow` **before** it cannot be
     checked against that allow here; accept it (the full check still does).
     An outcome answering an `allow` after the anchor must find it, and must be
     the only outcome for it.
3. With no anchor at all, fall back to `full`.
4. Log at INFO which mode was used and how many entries were checked.

`full` stays the default. The README must say plainly what `since_anchor`
gives up: tampering *before* the newest local anchor is no longer caught at
startup; it is still caught by `ickverify.py` against the published anchor,
and by `startup_verify = "full"`. It does not weaken anything against someone
who holds the signing key (they could re-sign either way).

### 3.4 `/v1/models` for OpenAI SDKs (Phase 4)

The response already has `"object": "list"` and `"data"`. Add three fields to
each model entry, **additively** (keep every existing field):
`"object": "model"`, `"created"` (unix seconds of the registry generation's
load time, or 0 if not known), `"owned_by"` (the entry's `provider`).

### 3.5 Anchor publisher status (Phase 5)

`tools/anchor_publisher.py publish` gains `--status FILE`. After every pass it
writes (atomically: temp file + `os.replace`) a JSON object:

```json
{ "checked_at": 1792500000, "ok": true, "last_push": "<sha or null>",
  "last_push_at": 1792499990, "published": {"acme": 2}, "refused": {},
  "error": null }
```

`ok` is false when a rewrite was detected, a tenant was refused, or git failed;
`error` then holds a one-line reason. Times are unix seconds. Nothing else in
the publisher's behaviour changes.

---

## 4. Phases and acceptance tests

Each phase ends with every existing test passing on Windows and Linux,
`cargo clippy --locked --all-targets -- -D warnings` clean, and its own tests
added. Commit per phase.

### Phase 1: real disk failure
Tests (in `src/receipts/mod.rs` tests and `tests/receipts.rs`):
- [x] With the hook set, the next `allow` returns `Unavailable`, `is_failed()`
      is true, the log on disk has no partial line from that job, and a
      following `allow` also returns `Unavailable` without touching the disk.
- [x] Through HTTP: with the hook set, a stream request gets `503
      receipt_unavailable`, the mock provider is **not** called, and a second
      request also gets `503`.
- [x] Reopening the directory after the failure (new `ReceiptChain::open`)
      verifies and accepts appends again.
- [x] Same for a failure on `sync_data` (not just `write_all`).

### Phase 2: metrics and readiness
- [x] After two allows and one outcome for tenant `acme`,
      `gw_receipts_written_total{tenant="acme"} 3` and
      `gw_receipt_entries{tenant="acme"} 3` appear in `/metrics`.
- [x] After the Phase 1 hook stops `acme`'s chain:
      `gw_receipt_chain_stopped{tenant="acme"} 1`, a nonzero
      `gw_receipt_write_failures_total`, and `/health/ready` lists `acme` under
      `receipts.stopped` while still answering `200` if everything else is
      healthy.
- [x] A torn tail at startup increments `gw_receipt_recoveries_total`.
- [x] Unknown tenant ids from requests never create a series (extend the
      existing cardinality test).

### Phase 3: bounded startup
- [x] `since_anchor` opens a 20,000-entry chain with an anchor at 19,990
      checking about 10 entries (assert the count from a test hook or the INFO
      log), and noticeably faster than `full` (assert it checks fewer entries,
      not wall time).
- [x] With `since_anchor`: a damaged entry **after** the newest anchor refuses
      to open; a damaged **anchor** line refuses to open; an anchor whose head
      does not match the log refuses to open.
- [x] With `since_anchor`: a damaged entry **before** the newest anchor opens
      (documented gap), and `ickverify.py` against the published anchor still
      reports NOT VERIFIED for that log.
- [x] With `full` (default), every existing startup test behaves as today.
- [x] No anchor at all: behaves exactly like `full`.

### Phase 4: models list
- [x] `/v1/models` entries carry `object: "model"`, `created` (number) and
      `owned_by`, and every field that was there before.
- [x] `demo/receipts/openai_sdk_check.py` gains a check that
      `client.models.list()` returns `mock/chat`.

### Phase 5: publisher status
- [x] `tools/tests/test_anchor_publisher.py`: status after a successful push
      (`ok: true`, `last_push` set); after a refused tenant (`ok: false`,
      tenant listed under `refused`); after a detected rewrite (`ok: false`,
      `error` set). The file is valid JSON after every pass.

### Phase 6: docs
- [x] README "Receipts": the metrics, the readiness field, `startup_verify`
      with the trade-off in plain words. `docs/anchoring.md`: `--status`.
- [x] `config/default.toml`: `startup_verify = "full"` with a comment.

---

## 5. Process rules for the builder

- One commit per phase, message saying what and why. Never `--no-verify`,
  never force-push, never edit `vendor/`.
- If a design point here turns out wrong or impossible, implement the closest
  safe thing and write it down under "Deviations" in the PR description. Do not
  silently change an invariant in section 2.
- Run the full suite with `--no-fail-fast`. One known flaky test,
  `a_large_backlog_of_old_reservations_is_pruned_in_batches`, is fixed in
  PR #45; if it fails here and #45 is not merged yet, note it, do not "fix" it
  again.
- Windows builds on this machine need
  `CARGO_PROFILE_DEV_DEBUG=line-tables-only` (debug-symbol size limit; see
  "Traps").

---

## 6. Handoff to Claude Code

Open one draft PR for the branch. In its description, list for each phase: what
was done, the tests that prove it, anything skipped, and every deviation from
this plan. Then hand over. The review will check, in this order:

1. Section 2's invariants, by reading `src/receipts/mod.rs` and
   `src/api/chat.rs` diffs line by line, not by trusting test names.
2. That each acceptance box has a test that would fail if the behaviour broke
   (the reviewer will break the code locally to confirm the important ones).
3. Phase 3's verification logic against section 3.3, step by step, including
   the outcome-before-anchor rule.
4. Metric label sources (cardinality) and that nothing new blocks a request
   thread.
5. CI on both platforms, the receipts demo, and the OpenAI SDK check.

---

## 7. Traps

1. **`verify_log` cannot check a slice.** The kernel's `verify_log` expects the
   first entry to have no predecessor and every outcome to answer an `allow`
   in the same slice. For `since_anchor` you must write the tail check yourself
   (hash, link from the anchored head, signature), the way `Writer::anchor()`
   already checks the region written since its last check. Reuse that logic.
2. **Anchors count entries from 1.** Entry number `count` is
   `entries[count - 1]`. Off-by-one here silently skips or double-checks.
3. **Windows file locking.** Never lock the log file itself (readers would
   fail with OS error 33); the lock is `receipts.lock`. Open the log for
   reading with a separate handle.
4. **Line endings.** The writer writes `\n` only. Do not read with any API that
   translates `\r\n`, and do not write files from tests with platform newlines
   when byte offsets matter.
5. **Disk space.** The test suite plus a debug build needs several GB; a full
   disk shows up as `LNK1318` or "no space on device", not as a test failure.
6. **Metrics on the hot path.** `render()` runs on scrape. Reading chain state
   there is fine; adding work per request or per token is not.
7. **The publisher's state file** (`--state`) is not the status file
   (`--status`). Do not merge them: the state file is what detects rewrites.
