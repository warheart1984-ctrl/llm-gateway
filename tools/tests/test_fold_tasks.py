"""Unit and local HTTP tests for tools/fold_tasks.py."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fold_tasks as ft  # noqa: E402

COMPLIANT = "- Owner: Noor, delivery to Bergen\n- Release: 04.03.2027\nRef: CEDAR-731"


class Checkers(unittest.TestCase):
    def test_bullets(self) -> None:
        self.assertTrue(ft.follows_bullets("- one\n- two\nRef: CEDAR-731"))
        self.assertTrue(ft.follows_bullets("- one"))
        self.assertFalse(ft.follows_bullets("- one\n- two\n- three\n- four"))
        self.assertFalse(ft.follows_bullets("The owner is Noor.\n- two"))
        self.assertFalse(ft.follows_bullets("Ref: CEDAR-731"))
        self.assertFalse(ft.follows_bullets("* one"))

    def test_no_birch(self) -> None:
        self.assertTrue(ft.avoids_birch(COMPLIANT))
        self.assertTrue(ft.avoids_birch("A banana budget."))
        self.assertFalse(ft.avoids_birch("Project Birch-204 has 7200 USD."))
        self.assertFalse(ft.avoids_birch("Owned by Ana."))

    def test_footer(self) -> None:
        self.assertTrue(ft.ends_with_footer(COMPLIANT, "Ref: CEDAR-731"))
        self.assertFalse(ft.ends_with_footer(COMPLIANT, "Ref: CEDAR-731/B"))
        self.assertFalse(ft.ends_with_footer("- one\nRef: CEDAR-731.", "Ref: CEDAR-731"))
        self.assertFalse(ft.ends_with_footer("", "Ref: CEDAR-731"))

    def test_dates(self) -> None:
        self.assertTrue(ft.writes_dates_dotted("Release: 04.03.2027"))
        self.assertFalse(ft.writes_dates_dotted("Release: 2027-03-04"))
        self.assertFalse(ft.writes_dates_dotted("Release: 04.03.2027 (2027-03-04)"))
        self.assertFalse(ft.writes_dates_dotted("No date here."))


class Scenario(unittest.TestCase):
    def test_the_seed_rules_sit_where_their_turns_are(self) -> None:
        scenario = ft.TaskScenario()
        for name in ("bullets", "no_birch", "footer"):
            at = scenario.introduced_at[name]
            self.assertGreater(at, 0, name)
            self.assertIn("Standing rule", scenario.messages[at - 1]["content"], name)
        self.assertEqual(scenario.introduced_at["dates"], 0)
        self.assertEqual(
            [scenario.introduced_at[name] - 1 for name in ("bullets", "no_birch", "footer")],
            list(ft.SEED_RULE_POSITIONS),
        )

    def test_the_dates_rule_and_the_footer_change_come_later(self) -> None:
        scenario = ft.TaskScenario()
        first_footer_at = scenario.introduced_at["footer"]
        for call in range(1, ft.FOOTER_CHANGE_CALL + 1):
            scenario.advance(call, 0, 5)
            if call == ft.DATES_RULE_CALL:
                self.assertEqual(scenario.introduced_at["dates"], len(scenario.messages))
                self.assertIn("DD.MM.YYYY", scenario.messages[-1]["content"])
        self.assertEqual(scenario.expected("footer"), ft.CHANGED_FOOTER)
        self.assertGreater(scenario.introduced_at["footer"], first_footer_at)
        self.assertIn(ft.CHANGED_FOOTER, scenario.messages[-1]["content"])

    def test_the_tasks_alternate_and_the_rules_are_never_in_the_question(self) -> None:
        scenario = ft.TaskScenario()
        self.assertEqual([scenario.task(call) for call in (1, 2, 3)], ["status", "budgets", "status"])
        body = scenario.request("test/model", 64, "budgets")
        self.assertEqual(body["messages"][0]["role"], "system")
        self.assertEqual(body["messages"][-1]["content"], ft.TASKS["budgets"])
        self.assertNotIn("Ref:", body["messages"][0]["content"])

    def test_score_checks_only_rules_in_force_for_the_task(self) -> None:
        scenario = ft.TaskScenario()
        self.assertEqual(set(ft.score(COMPLIANT, "stop", scenario, "status")), {"bullets", "no_birch", "footer"})
        scenario.advance(ft.DATES_RULE_CALL, 0, 5)
        self.assertIn("dates", ft.score(COMPLIANT, "stop", scenario, "status"))
        self.assertNotIn("dates", ft.score(COMPLIANT, "stop", scenario, "budgets"))
        self.assertTrue(all(ft.score(COMPLIANT, "stop", scenario, "status").values()))
        # An answer cut short obeys nothing.
        self.assertFalse(any(ft.score(COMPLIANT, "length", scenario, "status").values()))

    def test_a_rule_counts_only_when_folded_away_and_obeyed_unfolded(self) -> None:
        self.assertTrue(ft.rule_eligible(9, 9, True))
        self.assertFalse(ft.rule_eligible(9, 8, True))
        self.assertFalse(ft.rule_eligible(9, 9, False))
        self.assertFalse(ft.rule_eligible(0, 50, True))


class StubState:
    def __init__(self, folded_answer: str, folded_turns: str) -> None:
        self.spend = {"plain-key": 0, "folded-key": 0}
        self.committed = 0
        self.folded_answer = folded_answer
        self.folded_turns = folded_turns


class StubHandler(BaseHTTPRequestHandler):
    state: StubState

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _send(self, status: int, data: bytes, content_type: str, headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("content-type", content_type)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/v1/usage":
            body = json.dumps({"spent_nano_usd": self.state.spend.get(self.headers.get("x-api-key", ""), 0)})
            self._send(200, body.encode(), "application/json")
            return
        metrics = f'gw_folds_total{{tenant="folded",outcome="committed"}} {self.state.committed}\n'
        self._send(200, metrics.encode(), "text/plain")

    def do_POST(self) -> None:  # noqa: N802
        self.rfile.read(int(self.headers.get("content-length", "0")))
        key = self.headers.get("x-api-key", "")
        folded = key == "folded-key"
        self.state.spend[key] = self.state.spend.get(key, 0) + (40 if folded else 100)
        headers = {"x-gateway-folded": self.state.folded_turns, "x-request-id": "req"} if folded else {}
        if folded:
            self.state.committed = 1
        answer = self.state.folded_answer if folded else COMPLIANT
        payload = {"choices": [{"message": {"role": "assistant", "content": answer}, "finish_reason": "stop"}],
                   "usage": {"prompt_tokens": 100, "completion_tokens": 10}}
        self._send(200, json.dumps(payload).encode(), "application/json", headers)


class StubRuns(unittest.TestCase):
    def run_stub(self, folded_answer: str, folded_turns: str = "200", **kwargs) -> tuple[dict, int, str]:
        state = StubState(folded_answer, folded_turns)
        server = ThreadingHTTPServer(("127.0.0.1", 0), type("Handler", (StubHandler,), {"state": state}))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "tasks.json"
                report, code = ft.run_tasks(
                    base_url=f"http://127.0.0.1:{server.server_port}", plain_key="plain-key", folded_key="folded-key",
                    admin_key="admin-secret", model="test/model", profile="lifecycle", output=output, max_calls=3,
                    timeout=2, **kwargs,
                )
                return report, code, output.read_text(encoding="utf-8")
        finally:
            server.shutdown()
            server.server_close()

    def test_a_broken_rule_that_was_folded_away_is_a_regression(self) -> None:
        report, code, text = self.run_stub("- Project BIRCH-204 has 7200 USD.\nRef: CEDAR-731")
        self.assertEqual(code, 1)
        agg = report["aggregates"]
        self.assertEqual(agg["success_rate"], {"plain": 1.0, "folded": 0.0})
        self.assertEqual(agg["success_drop"], 1.0)
        self.assertEqual(agg["compliance"]["no_birch"]["eligible_broken"], 3)
        self.assertEqual(agg["compliance"]["footer"]["eligible_broken"], 0)
        self.assertEqual(agg["eligible_rules_broken"], 3)
        self.assertEqual(report["calls"][0]["folded"]["answer"], "- Project BIRCH-204 has 7200 USD.\nRef: CEDAR-731")
        self.assertNotIn("answer", report["calls"][0]["plain"])
        for secret in ("plain-key", "folded-key", "admin-secret"):
            self.assertNotIn(secret, text)

    def test_a_success_drop_within_the_margin_passes(self) -> None:
        _report, code, _text = self.run_stub("- Project BIRCH-204 has 7200 USD.\nRef: CEDAR-731", max_success_drop=1.0)
        self.assertEqual(code, 0)
        _report, code, _text = self.run_stub("- Project BIRCH-204 has 7200 USD.\nRef: CEDAR-731", max_success_drop=0.5)
        self.assertEqual(code, 1)

    def test_obeyed_rules_pass(self) -> None:
        report, code, _text = self.run_stub(COMPLIANT)
        self.assertEqual(code, 0)
        self.assertEqual(report["aggregates"]["success_rate"], {"plain": 1.0, "folded": 1.0})

    def test_nothing_folded_away_is_incomplete(self) -> None:
        report, code, _text = self.run_stub("- Project BIRCH-204.\nRef: CEDAR-731", folded_turns="0")
        self.assertEqual(code, 2)
        self.assertEqual(report["aggregates"]["eligible_rules"], 0)


if __name__ == "__main__":
    unittest.main()
