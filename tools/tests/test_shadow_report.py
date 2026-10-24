"""The shadow report totals. Each assertion fails if that total is dropped."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import shadow_report  # noqa: E402


def _line(tenant: str, request_id: str, fold: dict, cost: int = 0) -> str:
    return json.dumps({
        "message": "stream complete",
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
        self.assertEqual(totals["acme"]["requests"], 3)
        self.assertEqual(totals["acme"]["would_fold"], 1)
        self.assertEqual(totals["acme"]["reasons"]["cost_check"], 1)
        self.assertEqual(totals["acme"]["reasons"]["backoff"], 1)
        self.assertNotIn("batch_ready", totals["acme"]["reasons"])
        self.assertEqual(totals["acme"]["net_saving_nano_usd"], 37)
        self.assertEqual(totals["acme"]["summarizer_cost_nano_usd"], 10)
        self.assertEqual(totals["acme"]["backoff_cost_nano_usd"], 80)
        self.assertEqual(totals["other"]["requests"], 1)
        self.assertEqual(totals["other"]["net_saving_nano_usd"], 7)
        text = shadow_report.render(totals)
        self.assertIn("tenant acme", text)
        self.assertIn("requests 3", text)
        self.assertIn("would_fold_rate 0.3333", text)
        self.assertIn("refused backoff 0.3333", text)
        self.assertIn("refused cost_check 0.3333", text)
        self.assertIn("net_saving_nano_usd 37", text)
        self.assertIn("summarizer_cost_nano_usd 10", text)
        self.assertIn("backoff_cost_nano_usd 80", text)
        self.assertIn("tenant other", text)


if __name__ == "__main__":
    unittest.main()
