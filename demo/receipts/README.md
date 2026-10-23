# Receipts demo

A gateway replica with receipts on, a mock model, a publisher that copies the
replica's anchors somewhere the gateway cannot write, and a client that plays
first the caller and then the auditor. No provider account needed.

```bash
# With Docker
cd demo/receipts && docker compose up --build --abort-on-container-exit --exit-code-from client

# Without Docker (Windows, macOS, Linux; needs cargo and Python 3)
python demo/receipts/run_local.py
```

CI runs the Docker version, so a demo that stops passing fails the build.

## What it shows

Each step prints what it is showing and checks it. The run fails if any check
fails.

1. **A streamed answer gets an `allow` receipt and an outcome.** The response
   carries `x-receipt-id` (the allow) and `x-receipt-salt`.
2. **The caller can prove which request and reply the receipts cover.** From
   the bytes it sent, the stream it received and the salt, it recomputes both
   digests and finds them in the log. The log holds neither the prompt, nor
   the answer, nor a plain hash of the prompt.
3. **A refusal is recorded with its reason.** A tenant with a tiny budget
   runs out; the `402` becomes a `deny` with
   `reason_codes ["EXTERNAL_DENY", "BUDGET_EXHAUSTED"]`.
4. **A held request is recorded.** A model that needs a person's approval
   gets `202`, no model call, and an `await_human_approval` receipt.
5. **A failure part-way through is recorded.** A model that fails after a
   few words gets a `failed` outcome whose reply digest matches exactly the
   partial answer the client received.
6. **Each tenant's evidence is its own, and its anchor leaves the gateway.**
   The two tenants' chains share no entries. The publisher copies each
   tenant's anchor and public key out.
7. **The customer audits its own evidence.** It downloads its log, anchor and
   key from `GET /v1/receipts/*` (the client has no access to the gateway's
   disk), checks the served key against the one published at first sight, and
   the unchanged Wicket verifier says `VERIFIED` against the published anchor.
8. **An edited refusal is caught.** A copy of the log with one `deny` turned
   into `allow` is `NOT VERIFIED`.
9. **A quiet deletion is caught, but only by the published anchor.** A copy
   with the newest three entries removed, checked against equally doctored
   local anchors, still verifies. Checked against the published anchor, it is
   `NOT VERIFIED`: the log is shorter than what was anchored.
10. **A folded conversation is checked by the caller.** A tenant with receipts
    and conversation folding sends 30 turns; the 28 oldest reach the model as
    one summary. The response carries the summary and the fold's receipt id,
    the log holds four entries, and `tools/verify_fold.py` recomputes every
    digest from the body, the salt and the headers. A body changed by one
    character does not verify.

Then the official OpenAI Python SDK, with only its `base_url` changed, calls
`/v1/chat/completions`: a completion, a stream with usage, the receipt id in
the response headers, and refusals raised as the SDK's own exceptions.
(`run_local.py` skips this step if the `openai` package is not installed.)

## What it does not show

That the model's answer was right, or anything that never went through the
gateway. In the demo the publisher is a container with a read-only view of the
gateway's volume; in production it should run as a different account and push
to a place the gateway has no credentials for, such as Wicket's anchor
publisher and a separate git repository.

## Files

| File | Role |
|---|---|
| `config/` | Gateway config: receipts on, anchoring every second, two tenants with receipts on |
| `publisher.py` | Copies each tenant's new anchor lines and public key out; never rewrites what it published |
| `walkthrough.py` | The client and auditor; standard library only |
| `run_local.py` | Builds and runs everything without Docker |
| `compose.yaml` | The same, in containers |
