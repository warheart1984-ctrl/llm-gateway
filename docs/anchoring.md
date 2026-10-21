# Anchoring receipts outside the gateway

A tenant's receipt chain catches an edited or removed entry by itself. It does
**not** catch someone deleting the newest entries together with the local
anchors that mention them: what is left is still a valid, shorter chain. Only a
copy of the anchor kept where that person cannot reach it catches that. This
document says where that copy has to live for the guarantee to hold, and what
it still does not cover.

## The boundary

The rule is one sentence: **nobody who can rewrite the replica's receipts
directory may also be able to rewrite the published anchors.** In practice,
three principals:

| Principal | Can | Must not be able to |
|---|---|---|
| **Gateway operator** (runs `llm-gateway`, owns the receipts directory, holds each tenant's signing key) | write the receipts directory | push to, or administer, the anchor repository |
| **Publisher account** (runs `tools/anchor_publisher.py`) | read the receipts directory; push new commits to the anchor branch | write the receipts directory; force-push or delete the branch |
| **Anchor repository owner** (the customer, an auditor, or a separate organisation) | administer the repository, set branch protection | — |

If one person holds two of these roles, the guarantee is only as strong as
that person's honesty. Say so to the customer rather than implying otherwise.

## Setting it up

1. **Repository.** Create the anchor repository under an owner the gateway
   operator does not control. Protect the `anchors` branch: no force pushes, no
   deletion, and ideally require linear history. On GitHub that is a branch
   protection rule or ruleset; other hosts have the same settings.
2. **Credential.** Give the publisher a deploy key (or token) that can push to
   that one repository and nothing else. Store it only in the publisher
   account's home. The gateway's account must not be able to read it.
3. **Accounts.** Run the publisher as its own OS user with read-only access to
   the receipts root (`[receipts] directory`) and no write access to it. Its
   state file (`--state`) and working copy (`--work`) live in its own home.
4. **Run it.** One publisher per replica, each with its own `--replica` name.
   Several replicas can share a repository; each writes only its own folder,
   and pushes never overwrite each other.

   ```bash
   python tools/anchor_publisher.py publish \
     --source /var/lib/llm-gateway/receipts \
     --repo git@github.com:example-auditor/llm-gateway-anchors.git \
     --replica eu-1 \
     --state /home/anchors/eu-1/state.json \
     --status /home/anchors/eu-1/status.json \
     --watch 60
   ```

   It publishes each tenant's `anchor.jsonl` (appended, never rewritten) and
   its `signing.pub` (copied once, at first sight) to `eu-1/<tenant>/`.
   `anchor_interval_secs` on the gateway and `--watch` here together bound how
   long a new entry can go unanchored.

   Each pass also writes a JSON status summary to `--status` (atomically, via
   a temp file and `os.replace`):

   ```json
   { "checked_at": 1792500000, "ok": true, "last_push": "<sha or null>",
     "last_push_at": 1792499990, "published": {"acme": 2}, "refused": {},
     "error": null }
   ```

   `ok` is false when a rewrite was detected, a tenant was refused, or git
   failed; `error` then holds a one-line reason. Times are unix seconds. The
   file is valid JSON after every pass, so an operator or monitor can read it
   without parsing stderr.

5. **Verify.** Anyone with read access to the repository checks a tenant's log
   against what was published, with the unchanged Wicket verifier:

   ```bash
   python tools/anchor_publisher.py verify \
     --repo https://github.com/example-auditor/llm-gateway-anchors.git \
     --replica eu-1 --tenant acme --log receipts.jsonl
   ```

   The log itself can come from the tenant's own `GET /v1/receipts/log`.

## What the publisher refuses to do

It is built to never become a way to launder a rewrite:

- **It never force-pushes.** A refused push is retried from the remote's
  state, never over it.
- **It does not publish over a rewritten anchor.** If a tenant's anchor file on
  the replica no longer starts with what was already published, it alerts and
  publishes nothing more for that tenant. Other tenants carry on.
- **It does not replace a key.** A changed `signing.pub` is an alert; the key
  published first is kept.
- **It notices a rewritten repository.** It remembers the last commit it
  pushed. If the branch no longer contains it, because history was replaced or
  the branch deleted, it stops, exits `2`, and publishes nothing until a person
  looks.

Each of these is tested in `tools/tests/test_anchor_publisher.py` against real
git repositories.

## What this does not cover

- **A compromised gateway signing new forgeries.** The key lives with the
  gateway. From the moment someone controls it, they can write new, validly
  signed entries; anchors only protect what was published before.
- **Rewriting the published repository with the owner's rights.** Branch
  protection stops the publisher's credential; it does not stop the
  repository's owner. That is why the owner must be someone other than the
  gateway operator. A second, independent mirror (an auditor's own clone that
  is fetched and compared regularly) narrows this further.
- **The window before publication.** Entries newer than the last published
  anchor can still be removed undetected. Shorten `anchor_interval_secs` and
  `--watch` to shorten the window.
- **Hosted services' own behaviour.** The tests use local git repositories.
  Branch protection itself is the hosting service's guarantee, not this tool's.
