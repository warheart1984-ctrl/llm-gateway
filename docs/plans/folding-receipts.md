# Plan: conversation folding for tenants with receipts

**Branch:** `opencode/folding-receipts`, off `master` (PR 51 merged).
**Builder:** OpenCode. **Reviewer:** Claude Code, after handoff (section 6).
**Status: needs the owner's decisions D1–D4 (section 3) before handoff.**
**Goal:** a tenant with `receipts: true` can turn on folding, and its receipts
still prove what happened. The receipts must show what the client sent, what
the model was actually sent, and what the summarizer read and wrote. A client
can check all of it with its own data. No change to the receipt format,
`vendor/` or `ickverify.py`.

Read this whole file before writing code. Sections 2 and 3 are the contract.

---

## 1. Why, and what is out of scope

Today a tenant with both `receipts: true` and `folding.enabled` is refused at
load (`src/governance/policy.rs`, `validate_basic`). Receipts are the
product's core, so folding currently helps only tenants without them. Two
things block lifting that refusal:

1. **The forwarded request is not the client's request.** The `allow`
   receipt's `call_digest`, and today the outcome's `request_sha256`, bind the
   client's body. A folded call sends the model something else, and nothing
   in the log binds that.
2. **The summarizer call is a second model call carrying tenant content.**
   Under the fail-closed rule (`docs/plans/receipts-operations.md` section 2.1)
   a model call needs a durable `allow` first. The summarizer call has none.

**Out of scope:**
- New entry types or fields, or edits under `vendor/`. Only existing entries
  are used: `allow`, and outcome with `request_sha256` and `response_sha256`.
- Background folding for receipts tenants (D4).
- The retention benchmark (separate plan,
  `docs/plans/folding-retention-benchmark.md`). This plan is about evidence,
  not quality.
- The two known follow-ups from PR 51: shared-API-key session collisions, and
  a fold paid for by a request later refused.

---

## 2. Invariants that must still hold

All of `docs/plans/receipts-operations.md` section 2 and
`docs/plans/conversation-folding.md` section 2. In particular:

1. **Fail closed, for both calls.** No main call without a durable `allow`.
   No summarizer call without its own durable `allow` (D3). After any failed
   write the chain stops and every later request that needs a receipt gets
   `503`.
2. **Unfolded calls are unchanged.** A receipts tenant whose request is not
   folded gets exactly today's entries, digests and headers. Every existing
   receipts test passes unchanged.
3. **Every digest is recomputable by the client** from:
   - its own request body;
   - the salt;
   - the headers it received;
   - the summary text the gateway returned (D1).

   A digest nobody can recompute is not evidence.
4. **Still no content on disk.** The summary goes to the client in a response
   header. It is never written to the log, the ledger, the decision line or
   metrics. The log carries only salted hashes, as today.
5. **One salt per request,** as today, with a separate domain string for each
   digest, so no two digests can be confused.

---

## 3. Design, with the decisions to sign off

Each decision is the recommended choice; the alternative is noted.

### D1. The client gets the summary that was sent

- The response carries `x-gateway-fold-summary`: the exact summary text in the
  forwarded request, base64url without padding. That is the clipped,
  committed text, not the summarizer's raw output.
- It is sent on every folded response, whether this request ran a fold or
  reused one committed earlier.
- For receipts tenants, `summary_chars` is capped at 4,000, refused at load
  above that, so the header stays under about 5.4 KB.

*Alternative:* put the summary in the body. That changes the OpenAI response
shape, which is out of bounds.

### D2. The outcome of a folded call binds what the model was sent

For a folded call, and only then:

```
outcome.request_sha256 = sha256( salt
                               || "llm-gateway/folded-request/v1\n"
                               || call_digest            (the allow's, ASCII "sha256:<hex>")
                               || "\n"
                               || turns_replaced         (decimal ASCII, = x-gateway-folded)
                               || "\n"
                               || summary bytes          (UTF-8, = x-gateway-fold-summary decoded) )
```

The forwarded messages are fully determined by these three things, through
`build_forwarded_messages`:
- the client's body;
- how many turns were replaced;
- the exact summary text.

So this binds the forwarded request without the gateway inventing a canonical
JSON form. `allow.call_digest` stays the client body digest. Unfolded calls
keep `request_sha256 = call_digest` as today.

*Alternative:* hash the forwarded JSON. That's fragile, because the client
would have to reproduce `serde_json` output byte for byte.

### D3. The summarizer call has its own `allow` and outcome

- **Before the summarizer is called,** the chain writes an `allow` with
  `Subject.action = "fold"`, the summarizer's registry id as `model`, and:

  ```
  call_digest = sha256( salt
                      || "llm-gateway/fold-request/v1\n"
                      || main call_digest || "\n"
                      || turns_covered_before (decimal) || "\n"
                      || turns_covered_after  (decimal) || "\n"
                      || prior summary bytes  (empty if none) )
  ```

  It binds this request's body, which turns were folded, and the summary
  being extended. All of these the client knows: the prior summary arrived in
  an earlier response's header.
- **If that `allow` cannot be written, nothing is folded.** The main request
  then follows the normal receipts path: if the chain is stopped, it gets
  `503` like any other.
- **After the summarizer replies,** an outcome answers the fold `allow`:
  - status `completed`, or `failed` for a provider error or rejected checks;
  - `request_sha256` = the fold `call_digest`;
  - `response_sha256 = sha256(salt || "llm-gateway/fold-reply/v1\n" || new summary bytes)`
    when the fold commits, absent otherwise.
- **Header:** `x-gateway-fold-receipt-id` names the fold `allow`. It is only
  present when this request ran a fold.
- **The same per-request salt** as the main receipt, in `x-receipt-salt`.

*Alternative:* skip a separate receipt for the summarizer. That breaks the
fail-closed rule.

### D4. No background folding with receipts

`[folding] background` is a global setting. If it is `true` and any tenant has
both `receipts: true` and folding on, refuse to load, with an error naming the
tenant. A background fold runs after the response, so there is no client to
hand the salt and summary to, and its `allow` would bind nothing the client
can check.

*Alternative:* allow it and accept fold receipts nobody can verify. Not
recommended.

### Consequences the owner should know

- **A folded request with a fold writes four entries instead of two:** fold
  `allow`, fold outcome, main `allow`, main outcome. Receipt throughput per
  request roughly halves for those requests.
- **An auditor with only the log cannot tell a fold `allow` from a request
  `allow`.** The action sits inside `proposal_hash`. The client can, from the
  headers. Making it visible in the log would need a format change.
- **Idempotent replay** must store and return `x-gateway-fold-summary`,
  `x-gateway-folded` and `x-gateway-fold-receipt-id`, as it already stores the
  receipt id and salt.

---

## 4. Phases and acceptance tests

Each phase ends with:
- every existing test passing unchanged on Windows and Linux;
- `cargo clippy --locked --all-targets -- -D warnings` clean.

Commit per phase.

### Phase 1: folded-request digest and headers (D1, D2, D4)
- [ ] Load: folding + receipts is accepted. `summary_chars` above 4,000 with
      receipts is refused. `background = true` with any folding + receipts
      tenant is refused. Each refusal names the tenant.
- [ ] A folded stream and a folded completion:
      - the outcome's `request_sha256` equals D2 recomputed in the test from
        the body, salt and headers;
      - `call_digest` still equals the client body digest;
      - `x-gateway-fold-summary` decodes to the summary the mock upstream
        received.
- [ ] A reused summary (no new fold) gets the same binding and headers.
- [ ] An unfolded request from the same tenant gets today's entries and
      headers exactly. Compare against a request with folding off.
- [ ] An idempotent replay returns the same fold headers.

### Phase 2: the summarizer's own receipt (D3)
- [ ] A fold writes, in order: the fold `allow`, then the summarizer call,
      then the fold outcome. Both digests equal D3 recomputed in the test.
- [ ] With the fold `allow` write made to fail (`fail_disk_writes_from_now`),
      no summarizer call is made, and the request then follows the stopped-chain
      rule (`503 receipt_unavailable`).
- [ ] A rejected fold (pinned value dropped) writes a `failed` fold outcome
      without `response_sha256`, and the request goes out unfolded with its
      own receipts.
- [ ] `ickverify.py` (unchanged) accepts a log with folded requests in it.

### Phase 3: client verifier, docs, demo
- [ ] `tools/verify_fold.py` (standard library only) recomputes D2 and D3
      from a request body, the salt, the headers and the prior summary, and
      checks them against the log entries. Tests for a pass, and for one
      flipped byte in each input.
- [ ] README "Receipts": what a folded call's entries mean, the three new
      headers, the four-entries cost, and what an auditor without the
      headers cannot see.
- [ ] The receipts demo (`demo/receipts/`) folds one long conversation and
      runs `verify_fold.py` on it, so CI checks the whole chain.

---

## 5. Process rules for the builder

- One commit per phase, message saying what and why. Never `--no-verify`,
  never force-push, never edit `vendor/`.
- Do not start until the owner has signed off D1–D4. If a decision changes,
  this file is updated first.
- If a design point turns out impossible, implement the closest safe thing and
  list it under "Deviations". Never weaken section 2.
- **"Passing" means on both platforms:** read `gh pr checks` before handing
  over.
- An acceptance box is ticked only by a test that fails when the behaviour it
  names is removed.

---

## 6. Handoff to Claude Code

Open one draft PR. List per phase what was done, the tests that prove it,
anything skipped, and every deviation. The review will check:
1. Fail-closed on both calls, by reading the receipt and fold paths in
   `src/api/chat.rs` line by line. The reviewer will break the fold `allow`
   write on purpose.
2. Every digest recomputed by hand from one test's inputs, and checked to be
   computable from what the client actually holds.
3. Unfolded receipts tenants: byte-for-byte unchanged entries and headers.
4. No summary text in the log, the ledger, the decision line or metrics.
5. CI on both platforms and the receipts demo.
