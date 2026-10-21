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
5. **The anchor leaves the gateway.** The publisher copies the anchor and the
   public key out.
6. **The unchanged Wicket verifier says `VERIFIED`** for the log against the
   published anchor and key.
7. **An edited refusal is caught.** A copy of the log with one `deny` turned
   into `allow` is `NOT VERIFIED`.
8. **A quiet deletion is caught, but only by the published anchor.** A copy
   with the newest three entries removed, checked against equally doctored
   local anchors, still verifies. Checked against the published anchor, it is
   `NOT VERIFIED`: the log is shorter than what was anchored.

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
| `publisher.py` | Copies new anchor lines and the public key out; never rewrites what it published |
| `walkthrough.py` | The client and auditor; standard library only |
| `run_local.py` | Builds and runs everything without Docker |
| `compose.yaml` | The same, in containers |
