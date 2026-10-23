"""Check a folded call's receipts with what the client holds.

    python tools/verify_fold.py --log receipts.jsonl --body request.json --headers response-headers.json \
        [--prior-headers previous-response-headers.json] [--turns-after N]

For a tenant with receipts and conversation folding (docs/plans/folding-receipts.md),
recomputes from the request body the client sent, the salt and the headers it
received:

* the request's `allow`: `call_digest = sha256(salt || "llm-gateway/request/v1\\n" || body)`;
* the request's outcome: for a folded call
  `sha256(salt || "llm-gateway/folded-request/v1\\n" || call_digest || "\\n" || turns_replaced || "\\n" || summary)`,
  otherwise `call_digest` itself;
* when the request ran a fold (`x-gateway-fold-receipt-id`), the fold's `allow`
  `sha256(salt || "llm-gateway/fold-request/v1\\n" || call_digest || "\\n" || turns_before || "\\n" || turns_after || "\\n" || prior_summary)`,
  and its outcome: `completed` with
  `sha256(salt || "llm-gateway/fold-reply/v1\\n" || summary)`, or `failed` with no reply.

`--headers` and `--prior-headers` are JSON objects of response headers (names in
any case). The prior response supplies the summary being extended and how many
turns it covered; without one, the fold started from nothing. `turns_after` is
`x-gateway-fold-covered`; `--turns-after` is only for responses from gateways
that did not send it.

A request refused after its fold ran (402, 429, a reused Idempotency-Key)
carries no `x-receipt-id`, only the salt and the fold's headers; then only the
fold's receipts are checked.

Exit status: 0 every digest matches, 1 a digest does not match or an entry is
missing, 2 the input could not be read. Standard library only.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
from pathlib import Path

REQUEST_DOMAIN = b"llm-gateway/request/v1\n"
FOLDED_REQUEST_DOMAIN = b"llm-gateway/folded-request/v1\n"
FOLD_REQUEST_DOMAIN = b"llm-gateway/fold-request/v1\n"
FOLD_REPLY_DOMAIN = b"llm-gateway/fold-reply/v1\n"


class Unreadable(Exception):
    pass


def digest(salt: bytes, domain: bytes, *parts: bytes) -> str:
    return "sha256:" + hashlib.sha256(salt + domain + b"".join(parts)).hexdigest()


def request_digest(salt: bytes, body: bytes) -> str:
    return digest(salt, REQUEST_DOMAIN, body)


def folded_request_digest(salt: bytes, call_digest: str, turns_replaced: int, summary: str) -> str:
    return digest(salt, FOLDED_REQUEST_DOMAIN, call_digest.encode(), b"\n", str(turns_replaced).encode(), b"\n",
                  summary.encode("utf-8"))


def fold_request_digest(salt: bytes, call_digest: str, before: int, after: int, prior_summary: str) -> str:
    return digest(salt, FOLD_REQUEST_DOMAIN, call_digest.encode(), b"\n", str(before).encode(), b"\n",
                  str(after).encode(), b"\n", prior_summary.encode("utf-8"))


def fold_reply_digest(salt: bytes, summary: str) -> str:
    return digest(salt, FOLD_REPLY_DOMAIN, summary.encode("utf-8"))


def lower_keys(headers: dict) -> dict[str, str]:
    return {str(name).lower(): str(value) for name, value in headers.items()}


def summary_of(headers: dict[str, str]) -> str | None:
    encoded = headers.get("x-gateway-fold-summary")
    if encoded is None:
        return None
    try:
        return base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise Unreadable(f"x-gateway-fold-summary is not base64url UTF-8: {exc}") from None


def folded_turns(headers: dict[str, str]) -> int | None:
    value = headers.get("x-gateway-folded")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        raise Unreadable("x-gateway-folded is not a number") from None


def covered_turns(headers: dict[str, str]) -> int | None:
    value = headers.get("x-gateway-fold-covered")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        raise Unreadable("x-gateway-fold-covered is not a number") from None


def verify(log: list[dict], body: bytes, headers: dict, prior_headers: dict | None = None,
           turns_after: int | None = None) -> list[str]:
    """Every check that fails, as one line each. Empty when all match."""
    headers = lower_keys(headers)
    prior = lower_keys(prior_headers or {})
    try:
        salt = bytes.fromhex(headers["x-receipt-salt"])
    except (KeyError, ValueError):
        raise Unreadable("the headers need a hex x-receipt-salt") from None
    receipt_id = headers.get("x-receipt-id")
    fold_id = headers.get("x-gateway-fold-receipt-id")
    if receipt_id is None and fold_id is None:
        raise Unreadable("the headers name no receipt: neither x-receipt-id nor x-gateway-fold-receipt-id")
    by_id = {entry.get("receipt_id"): entry for entry in log}
    outcomes = {entry.get("decision_receipt_id"): entry for entry in log if "decision_receipt_id" in entry}
    failures: list[str] = []

    def expect(what: str, actual, wanted) -> None:
        if actual != wanted:
            failures.append(f"{what}: log has {actual!r}, recomputed {wanted!r}")

    call_digest = request_digest(salt, body)
    replaced = folded_turns(headers)
    summary = summary_of(headers)
    if receipt_id is not None:
        allow = by_id.get(receipt_id)
        if allow is None:
            failures.append(f"no entry {receipt_id} in the log")
        else:
            expect("request allow call_digest", allow.get("call_digest"), call_digest)
        if (replaced is None) != (summary is None):
            failures.append("x-gateway-folded and x-gateway-fold-summary must come together")
        outcome = outcomes.get(receipt_id)
        if outcome is None:
            failures.append(f"no outcome for {receipt_id}")
        elif replaced is not None and summary is not None:
            expect("request outcome request_sha256 (folded)", outcome.get("request_sha256"),
                   folded_request_digest(salt, call_digest, replaced, summary))
        else:
            expect("request outcome request_sha256", outcome.get("request_sha256"), call_digest)

    if fold_id is not None:
        # What the prior response's summary covered: the turns it sent folded,
        # or, for a refusal whose fold committed, the turns that fold covered.
        before = folded_turns(prior)
        if before is None and summary_of(prior) is not None:
            before = covered_turns(prior)
        before = before or 0
        prior_summary = summary_of(prior) or ""
        after = covered_turns(headers)
        if after is None:
            after = replaced if replaced is not None else turns_after
        if after is None:
            raise Unreadable("no x-gateway-fold-covered for this fold: pass --turns-after")
        fold_allow = by_id.get(fold_id)
        wanted = fold_request_digest(salt, call_digest, before, after, prior_summary)
        if fold_allow is None:
            failures.append(f"no entry {fold_id} in the log")
        else:
            expect("fold allow call_digest", fold_allow.get("call_digest"), wanted)
        fold_outcome = outcomes.get(fold_id)
        if fold_outcome is None:
            failures.append(f"no outcome for the fold {fold_id}")
        else:
            expect("fold outcome request_sha256", fold_outcome.get("request_sha256"), wanted)
            if summary is not None:
                expect("fold outcome status", fold_outcome.get("status"), "completed")
                expect("fold outcome response_sha256", fold_outcome.get("response_sha256"),
                       fold_reply_digest(salt, summary))
            else:
                expect("fold outcome status", fold_outcome.get("status"), "failed")
                expect("fold outcome response_sha256", fold_outcome.get("response_sha256"), None)
    return failures


def read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise Unreadable(f"{path}: {exc}") from None


def read_log(path: Path) -> list[dict]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        return [json.loads(line) for line in lines if line.strip()]
    except (OSError, ValueError) as exc:
        raise Unreadable(f"{path}: {exc}") from None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--log", type=Path, required=True, help="the tenant's receipts.jsonl")
    parser.add_argument("--body", type=Path, required=True, help="the request body exactly as sent")
    parser.add_argument("--headers", type=Path, required=True, help="the response headers, as a JSON object")
    parser.add_argument("--prior-headers", type=Path, help="the previous response's headers in this conversation")
    parser.add_argument("--turns-after", type=int, help="for a rejected fold: the turns it tried to cover")
    args = parser.parse_args(argv)
    try:
        body = args.body.read_bytes()
        failures = verify(read_log(args.log), body, read_json(args.headers),
                          read_json(args.prior_headers) if args.prior_headers else None, args.turns_after)
    except (OSError, Unreadable) as exc:
        print(f"verify-fold: {exc}", file=sys.stderr)
        return 2
    for failure in failures:
        print(f"MISMATCH {failure}")
    print("VERIFIED" if not failures else "NOT VERIFIED")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
