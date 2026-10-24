"""The shadow report totals. Each assertion fails if that total is dropped."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import shadow_report  # noqa: E402


def _line(tenant: str, request_id: str, fold: dict, cost: int = 0, message: str = "stream complete") -> str:
    return json.dumps({
        "message": message,
        "tenant": tenant,
        "request_id": request_id,
        "cost_nano_usd": cost,
        "fold": json.dumps(fold),
    })


class ShadowReportTests(unittest.TestCase):
    def test_totals_per_tenant(self) -> None:
        log = Path(tempfile.mkdtemp()) / "gateway.log"
        log.write_text(
            "\n".join([
                _line("acme", "11111111-1111-4111-8111-111111111111", {
                    "shadow": True, "would_fold": True, "outcome": "applied", "reason": "batch_ready",
                    "net_saving_nano_usd": 40, "summarizer_cost_nano_usd": 10,
                }, cost=5),
                _line("acme", "22222222-2222-4222-8222-222222222222", {
                    "shadow": True, "would_fold": False, "outcome": "refused", "reason": "cost_check",
                    "net_saving_nano_usd": -3, "summarizer_cost_nano_usd": 0,
                }),
                _line("acme", "33333333-3333-4333-8333-333333333333", {
                    "shadow": True, "would_fold": False, "outcome": "skipped", "reason": "backoff",
                    "net_saving_nano_usd": 0, "summarizer_cost_nano_usd": 0,
                }, cost=80),
                # Not due for a new fold, but it would have gone out with the summary it has.
                _line("acme", "66666666-6666-4666-8666-666666666666", {
                    "shadow": True, "would_fold": True, "outcome": "not_due", "reason": "not_enough_new_turns",
                    "net_saving_nano_usd": 20, "summarizer_cost_nano_usd": 0,
                }, cost=70),
                # A background fold's summarizer spend, logged after its request.
                _line("acme", "-", {
                    "shadow": True, "background": True, "committed": True,
                    "net_saving_nano_usd": -15, "summarizer_cost_nano_usd": 15,
                }, message="shadow fold finished"),
                # An on-mode fold is not a shadow request.
                _line("acme", "44444444-4444-4444-8444-444444444444", {
                    "outcome": "applied", "reason": "batch_ready",
                }, cost=999),
                _line("other", "55555555-5555-4555-8555-555555555555", {
                    "shadow": True, "would_fold": True, "outcome": "applied", "reason": "batch_ready",
                    "net_saving_nano_usd": 7, "summarizer_cost_nano_usd": 2,
                }),
                "not json",
            ]) + "\n",
            encoding="utf-8",
        )
        totals = shadow_report.load(log)
        # The background line is spend, not a request.
        self.assertEqual(totals["acme"]["requests"], 4)
        self.assertEqual(totals["acme"]["background_folds"], 1)
        self.assertEqual(totals["acme"]["would_fold"], 2)
        self.assertEqual(totals["acme"]["outcomes"][("refused", "cost_check")], 1)
        self.assertEqual(totals["acme"]["outcomes"][("skipped", "backoff")], 1)
        self.assertEqual(totals["acme"]["outcomes"][("not_due", "not_enough_new_turns")], 1)
        self.assertEqual(totals["acme"]["outcomes"][("applied", "batch_ready")], 1)
        self.assertEqual(totals["acme"]["net_saving_nano_usd"], 40 - 3 + 20 - 15)
        self.assertEqual(totals["acme"]["summarizer_cost_nano_usd"], 25)
        self.assertEqual(totals["other"]["requests"], 1)
        self.assertEqual(totals["other"]["net_saving_nano_usd"], 7)
        text = shadow_report.render(totals)
        self.assertIn("tenant acme", text)
        self.assertIn("requests 4", text)
        self.assertIn("would_fold_rate 0.5000", text)
        self.assertIn("outcome skipped backoff 0.2500", text)
        self.assertIn("outcome refused cost_check 0.2500", text)
        self.assertIn("outcome not_due not_enough_new_turns 0.2500", text)
        self.assertIn("outcome applied batch_ready 0.2500", text)
        self.assertIn("background_folds 1", text)
        self.assertIn("net_saving_nano_usd 42", text)
        self.assertIn("summarizer_cost_nano_usd 25", text)
        # Only the fold's own outcome is a refusal, and no request's own cost
        # is passed off as a cost of folding.
        self.assertNotIn("refused not_enough_new_turns", text)
        self.assertNotIn("refused backoff", text)
        self.assertNotIn("backoff_cost", text)
        self.assertIn("tenant other", text)


if __name__ == "__main__":
    unittest.main()
