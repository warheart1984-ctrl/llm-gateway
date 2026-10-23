"""Tests for tools/verify_fold.py: a matching log verifies, and one changed byte in any input does not.

The gateway's own digests are checked against this script in tests/fold_receipts.rs;
here the log is built with the script's functions so each input can be varied.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import verify_fold as vf  # noqa: E402

SALT = bytes(range(32))
BODY = b'{"model":"mock/chat","messages":[]}'
PRIOR = "Mira owns CEDAR-731."
SUMMARY = "Mira owns CEDAR-731; staging is on 8443."


def b64(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def folded_case(summary: str = SUMMARY) -> tuple[list[dict], dict, dict]:
    """A request that ran an incremental fold (covering 4 -> 8 turns) and was sent folded."""
    call = vf.request_digest(SALT, BODY)
    fold_request = vf.fold_request_digest(SALT, call, 4, 8, PRIOR)
    log = [
        {"receipt_id": "fold-allow", "verdict": "allow", "call_digest": fold_request},
        {"receipt_id": "fold-outcome", "decision_receipt_id": "fold-allow", "status": "completed",
         "request_sha256": fold_request, "response_sha256": vf.fold_reply_digest(SALT, summary)},
        {"receipt_id": "main-allow", "verdict": "allow", "call_digest": call},
        {"receipt_id": "main-outcome", "decision_receipt_id": "main-allow", "status": "completed",
         "request_sha256": vf.folded_request_digest(SALT, call, 8, summary)},
    ]
    headers = {"X-Receipt-Id": "main-allow", "X-Receipt-Salt": SALT.hex(), "X-Gateway-Folded": "8",
               "X-Gateway-Fold-Summary": b64(summary), "X-Gateway-Fold-Receipt-Id": "fold-allow"}
    prior = {"x-gateway-folded": "4", "x-gateway-fold-summary": b64(PRIOR)}
    return log, headers, prior


class Verifying(unittest.TestCase):
    def test_a_folded_call_verifies(self) -> None:
        log, headers, prior = folded_case()
        self.assertEqual(vf.verify(log, BODY, headers, prior), [])

    def test_an_unfolded_call_verifies(self) -> None:
        call = vf.request_digest(SALT, BODY)
        log = [{"receipt_id": "a", "call_digest": call},
               {"receipt_id": "o", "decision_receipt_id": "a", "request_sha256": call}]
        self.assertEqual(vf.verify(log, BODY, {"x-receipt-id": "a", "x-receipt-salt": SALT.hex()}), [])

    def test_a_rejected_fold_verifies_with_its_turns_after(self) -> None:
        call = vf.request_digest(SALT, BODY)
        fold_request = vf.fold_request_digest(SALT, call, 0, 28, "")
        log = [
            {"receipt_id": "f", "call_digest": fold_request},
            {"receipt_id": "fo", "decision_receipt_id": "f", "status": "failed", "request_sha256": fold_request},
            {"receipt_id": "a", "call_digest": call},
            {"receipt_id": "o", "decision_receipt_id": "a", "request_sha256": call},
        ]
        headers = {"x-receipt-id": "a", "x-receipt-salt": SALT.hex(), "x-gateway-fold-receipt-id": "f"}
        self.assertEqual(vf.verify(log, BODY, headers, None, turns_after=28), [])
        with self.assertRaises(vf.Unreadable):
            vf.verify(log, BODY, headers)

    def test_a_rejected_fold_verifies_from_its_covered_header(self) -> None:
        call = vf.request_digest(SALT, BODY)
        fold_request = vf.fold_request_digest(SALT, call, 0, 28, "")
        log = [
            {"receipt_id": "f", "call_digest": fold_request},
            {"receipt_id": "fo", "decision_receipt_id": "f", "status": "failed", "request_sha256": fold_request},
            {"receipt_id": "a", "call_digest": call},
            {"receipt_id": "o", "decision_receipt_id": "a", "request_sha256": call},
        ]
        headers = {"x-receipt-id": "a", "x-receipt-salt": SALT.hex(), "x-gateway-fold-receipt-id": "f",
                   "x-gateway-fold-covered": "28"}
        self.assertEqual(vf.verify(log, BODY, headers), [])
        self.assertTrue(vf.verify(log, BODY, dict(headers, **{"x-gateway-fold-covered": "27"})))

    def test_a_request_refused_after_its_fold_verifies_the_fold(self) -> None:
        # No request allow was written; the refusal carries the salt and the
        # fold's headers, including the summary of a fold that committed.
        call = vf.request_digest(SALT, BODY)
        fold_request = vf.fold_request_digest(SALT, call, 4, 8, PRIOR)
        log = [
            {"receipt_id": "f", "call_digest": fold_request},
            {"receipt_id": "fo", "decision_receipt_id": "f", "status": "completed", "request_sha256": fold_request,
             "response_sha256": vf.fold_reply_digest(SALT, SUMMARY)},
        ]
        refusal = {"x-receipt-salt": SALT.hex(), "x-gateway-fold-receipt-id": "f", "x-gateway-fold-covered": "8",
                   "x-gateway-fold-summary": b64(SUMMARY)}
        prior = {"x-gateway-folded": "4", "x-gateway-fold-summary": b64(PRIOR)}
        self.assertEqual(vf.verify(log, BODY, refusal, prior), [])
        self.assertTrue(vf.verify(log, BODY + b" ", refusal, prior), "the body sent")
        with self.assertRaises(vf.Unreadable):
            vf.verify(log, BODY, {"x-receipt-salt": SALT.hex()})

    def test_a_refusal_whose_fold_committed_is_the_next_fold_prior(self) -> None:
        # The prior response was a refusal: no x-gateway-folded, but its fold
        # committed, so its covered count is where the next fold starts.
        call = vf.request_digest(SALT, BODY)
        fold_request = vf.fold_request_digest(SALT, call, 8, 12, SUMMARY)
        log = [
            {"receipt_id": "f2", "call_digest": fold_request},
            {"receipt_id": "fo2", "decision_receipt_id": "f2", "status": "failed", "request_sha256": fold_request},
            {"receipt_id": "a", "call_digest": call},
            {"receipt_id": "o", "decision_receipt_id": "a", "request_sha256": call},
        ]
        prior = {"x-gateway-fold-covered": "8", "x-gateway-fold-summary": b64(SUMMARY)}
        headers = {"x-receipt-id": "a", "x-receipt-salt": SALT.hex(), "x-gateway-fold-receipt-id": "f2",
                   "x-gateway-fold-covered": "12"}
        self.assertEqual(vf.verify(log, BODY, headers, prior), [])

    def test_one_changed_byte_in_any_input_fails(self) -> None:
        log, headers, prior = folded_case()
        changed_body = BODY.replace(b"mock", b"mocK")
        self.assertTrue(vf.verify(log, changed_body, headers, prior), "body")

        salt = dict(headers, **{"X-Receipt-Salt": bytes([1]) .hex() + SALT.hex()[2:]})
        self.assertTrue(vf.verify(log, BODY, salt, prior), "salt")

        summary = dict(headers, **{"X-Gateway-Fold-Summary": b64(SUMMARY.replace("8443", "8444"))})
        self.assertTrue(vf.verify(log, BODY, summary, prior), "summary")

        turns = dict(headers, **{"X-Gateway-Folded": "9"})
        self.assertTrue(vf.verify(log, BODY, turns, prior), "turns replaced")

        prior_summary = dict(prior, **{"x-gateway-fold-summary": b64(PRIOR.replace("Mira", "Noor"))})
        self.assertTrue(vf.verify(log, BODY, headers, prior_summary), "prior summary")

        prior_turns = dict(prior, **{"x-gateway-folded": "5"})
        self.assertTrue(vf.verify(log, BODY, headers, prior_turns), "turns before")

        tampered = json.loads(json.dumps(log))
        tampered[1]["response_sha256"] = vf.fold_reply_digest(SALT, SUMMARY + ".")
        self.assertTrue(vf.verify(tampered, BODY, headers, prior), "fold reply in the log")

    def test_the_command_line_exit_codes(self) -> None:
        log, headers, prior = folded_case()
        with tempfile.TemporaryDirectory() as directory:
            d = Path(directory)
            (d / "log.jsonl").write_text("".join(json.dumps(e) + "\n" for e in log), encoding="utf-8")
            (d / "body.json").write_bytes(BODY)
            (d / "headers.json").write_text(json.dumps(headers), encoding="utf-8")
            (d / "prior.json").write_text(json.dumps(prior), encoding="utf-8")
            args = ["--log", str(d / "log.jsonl"), "--body", str(d / "body.json"),
                    "--headers", str(d / "headers.json"), "--prior-headers", str(d / "prior.json")]
            self.assertEqual(vf.main(args), 0)
            (d / "body.json").write_bytes(BODY + b" ")
            self.assertEqual(vf.main(args), 1)
            self.assertEqual(vf.main(args[:2] + ["--body", str(d / "missing"), "--headers", str(d / "headers.json")]), 2)


if __name__ == "__main__":
    unittest.main()
