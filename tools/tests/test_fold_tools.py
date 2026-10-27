"""Unit tests for tools/fold_tools.py."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fold_retention as fr  # noqa: E402
import fold_tools as fto  # noqa: E402


def arm(content=None, tool_calls=None, finish_reason="stop"):
    message = {}
    if content is not None:
        message["content"] = content
    if tool_calls:
        message["tool_calls"] = tool_calls
    return {"payload": {"choices": [{"message": message, "finish_reason": finish_reason}]}}


class Scoring(unittest.TestCase):
    def test_parse_answer(self) -> None:
        self.assertEqual(fto.parse_answer('{"answer": 1043}', "stop"), (1043, True))
        self.assertEqual(fto.parse_answer('{"answer": "ok"}', "stop"), ("ok", True))
        self.assertEqual(fto.parse_answer('{"answer": "ok", "other": 1}', "stop"), (None, False))
        self.assertEqual(fto.parse_answer('it is 1043', "stop"), (None, False))
        self.assertEqual(fto.parse_answer('{"answer": 1043', "stop"), (None, False))
        self.assertEqual(fto.parse_answer('{"answer": 1043}', "length"), (None, False))

    def test_strict_match_keeps_types_apart(self) -> None:
        self.assertTrue(fto.strict_match(39, 39))
        self.assertTrue(fto.strict_match(1043, 1043))
        self.assertFalse(fto.strict_match("39", 39))
        self.assertFalse(fto.strict_match("1043", 1043))
        self.assertFalse(fto.strict_match(True, 1))
        self.assertFalse(fto.strict_match(1, True))

    def test_lenient_match_accepts_a_numeric_string_and_a_short_sentence(self) -> None:
        self.assertTrue(fto.lenient_match(39, 39))
        self.assertTrue(fto.lenient_match("39", 39))
        self.assertTrue(fto.lenient_match("1043", 1043))
        self.assertTrue(fto.lenient_match("fails at line 1043", 1043))
        self.assertTrue(fto.lenient_match("totals/rounds_partial_refund fails at line 1043", 1043))
        self.assertTrue(fto.lenient_match("39 tests in checkout/totals passed.", 39))

    def test_lenient_match_rejects_a_wrong_or_ambiguous_number(self) -> None:
        self.assertFalse(fto.lenient_match("fails at line 12.81", 1043))
        self.assertFalse(fto.lenient_match("at line 39", 1043))
        self.assertFalse(fto.lenient_match("1043 and 887", 1043))
        self.assertFalse(fto.lenient_match("1043 and 1043", 1043))
        self.assertFalse(fto.lenient_match("10430", 1043))
        self.assertFalse(fto.lenient_match("039", 39))
        self.assertFalse(fto.lenient_match("the line is 01043", 1043))
        long_sentence = " ".join(["note"] * 30) + " 1043"
        self.assertGreater(len(long_sentence.split()), 24)
        self.assertFalse(fto.lenient_match(long_sentence, 1043))
        self.assertFalse(fto.lenient_match("true", True))
        self.assertFalse(fto.lenient_match(1, True))
        self.assertTrue(fto.strict_match("v2.14.7", "v2.14.7"))
        self.assertTrue(fto.lenient_match("v2.14.7", "v2.14.7"))
        self.assertFalse(fto.lenient_match("the tag is v2.14.7", "v2.14.7"))

    def test_emitted_calls(self) -> None:
        calls = [{"id": "x", "type": "function",
                  "function": {"name": "read_file", "arguments": '{"path":"deploy/notes.txt"}'}}]
        got = fto.emitted_calls(arm(tool_calls=calls))
        self.assertEqual(got, [{"name": "read_file",
                                "arguments": json.dumps({"path": "deploy/notes.txt"}, sort_keys=True)}])
        self.assertEqual(fto.emitted_calls(arm(content="{}")), [])

    def test_refetch_detection(self) -> None:
        # A call for a path the scenario already fetched is a refetch.
        key = next(k for k in fto.FETCHED if k[0] == "read_file" and "notes" in k[1])
        self.assertIn(("read_file", key[1]), fto.FETCHED)
        self.assertNotIn(("read_file", json.dumps({"path": "other.txt"}, sort_keys=True)), fto.FETCHED)


class ScenarioShape(unittest.TestCase):
    def test_results_are_large_json_or_errors(self) -> None:
        # The results are real work to keep straight: KB of JSON, one an error.
        self.assertGreater(len(fto.RESULTS["deploy_yaml"]), 2000)
        self.assertGreater(len(fto.RESULTS["metrics_json"]), 2000)
        self.assertIn("error", json.loads(fto.RESULTS["error_log"]))

    def test_questions_answer_from_scripted_results_only(self) -> None:
        for call_index, gold, text in fto.QUESTIONS:
            name, _args, key = fto.CALLS[call_index][1], fto.CALLS[call_index][2], fto.CALLS[call_index][3]
            result = fto.RESULTS[key]
            if key.endswith("_txt"):
                self.assertIn(str(gold), result)
            else:
                # Typed golds appear verbatim in the result JSON.
                self.assertIn(json.dumps(gold).strip('"'), result)
            self.assertNotIn(str(gold), text)  # the question never leaks the answer

    def test_request_offers_tools_and_temperature_zero(self) -> None:
        scenario = fto.AgentScenario()
        scenario.advance(1)
        body = scenario.request("m", 64, "q?")
        self.assertEqual(body["temperature"], 0)
        self.assertEqual([t["function"]["name"] for t in body["tools"]],
                         ["read_file", "query_logs", "run_tests"])
        roles = [m["role"] for m in body["messages"]]
        self.assertEqual(roles, ["system", "user", "assistant", "tool", "user"])

    def test_gold_values_are_not_in_history(self) -> None:
        # A question's answer must come from the result, not the script's prose.
        scenario = fto.AgentScenario()
        for call in range(1, 29):
            if call <= 28:
                scenario.advance(call)
        history = json.dumps(scenario.messages)
        self.assertIn("v2.14.7", history)
        self.assertNotIn("What image tag", history)


class Aggregates(unittest.TestCase):
    def row(self, correct_p, correct_f, eligible, refetch_p=0, refetch_f=0):
        call = {"name": "read_file", "arguments": "{}"}
        usage = {"prompt_tokens": 10, "cached_tokens": 0, "completion_tokens": 1}
        return {
            "result_message_at": 10, "eligible": eligible,
            "plain": {"correct": correct_p, "spend_delta_nano_usd": 100,
                      "usage": dict(usage), "refetches": [call] * refetch_p, "tool_calls": []},
            "folded": {"correct": correct_f, "spend_delta_nano_usd": 50,
                       "usage": dict(usage), "refetches": [call] * refetch_f, "tool_calls": [],
                       "folded_turns": 10, "fold_count": 2,
                       "summarizer_prompt_tokens": 5, "summarizer_completion_tokens": 1},
        }

    def test_counts(self) -> None:
        rows = [self.row(True, True, True), self.row(True, False, True),
                self.row(False, False, False)]
        agg = fto.aggregates(rows, {"max_folds": 4}, {}, {})
        self.assertEqual(agg["eligible_questions"], 2)
        self.assertEqual(agg["eligible_correct"], 1)
        self.assertEqual(agg["eligible_wrong"], 1)
        self.assertEqual(agg["accuracy"]["plain"], 2 / 3)
        self.assertEqual(agg["accuracy_lenient"]["plain"], 2 / 3)
        self.assertEqual(agg["format_only_misses"], {"plain": 0, "folded": 0})
        self.assertEqual(agg["eligible_format_only"], 0)
        self.assertEqual(agg["total_spend_nano_usd"], {"plain": 300, "folded": 150})

    def test_format_only_misses_are_counted_and_strict_still_fails(self) -> None:
        strict_hit = self.row(True, True, True)
        strict_hit["plain"]["lenient_correct"] = True
        strict_hit["folded"]["lenient_correct"] = True
        format_miss = self.row(True, False, True)
        format_miss["plain"]["lenient_correct"] = True
        format_miss["folded"]["lenient_correct"] = True
        real_miss = self.row(True, False, True)
        real_miss["plain"]["lenient_correct"] = True
        real_miss["folded"]["lenient_correct"] = False
        agg = fto.aggregates([strict_hit, format_miss, real_miss], {"max_folds": 4}, {}, {})
        self.assertEqual(agg["accuracy"]["folded"], 1 / 3)
        self.assertEqual(agg["accuracy_lenient"]["folded"], 2 / 3)
        self.assertEqual(agg["format_only_misses"]["folded"], 1)
        self.assertEqual(agg["eligible_wrong"], 2)
        self.assertEqual(agg["eligible_format_only"], 1)

    def test_a_recall_of_the_needed_tool_is_counted_apart_from_a_guess(self) -> None:
        guessed = self.row(True, False, True)
        recalled = self.row(True, False, True)
        recalled["needs_tool"] = "run_tests"
        recalled["folded"]["tool_calls"] = [{"name": "run_tests", "arguments": "{}"}]
        wrong_tool = self.row(True, False, True)
        wrong_tool["needs_tool"] = "run_tests"
        wrong_tool["folded"]["tool_calls"] = [{"name": "read_file", "arguments": "{}"}]
        agg = fto.aggregates([guessed, recalled, wrong_tool], {"max_folds": 4}, {}, {})
        self.assertEqual(agg["eligible_wrong"], 3)
        self.assertEqual(agg["eligible_recalled"], 1)

    def test_refetch_counts(self) -> None:
        rows = [self.row(True, True, True, refetch_p=0, refetch_f=1),
                self.row(True, True, True, refetch_p=0, refetch_f=2)]
        agg = fto.aggregates(rows, {"max_folds": 4}, {}, {})
        self.assertEqual(agg["refetches"], {"plain": 0, "folded": 3})


PRICES = {
    "main_input_per_mtok_usd": 0.40,
    "main_cached_input_per_mtok_usd": 0.10,
    "main_output_per_mtok_usd": 1.60,
    "summarizer_input_per_mtok_usd": 0.10,
    "summarizer_output_per_mtok_usd": 0.40,
}


class CostGuard(unittest.TestCase):
    def test_call_cost_prices_both_arms_and_the_summarizer(self) -> None:
        row = {
            "plain": {"usage": {"prompt_tokens": 1_000_000, "cached_tokens": 250_000, "completion_tokens": 0},
                      "spend_delta_nano_usd": 0},
            "folded": {"usage": {"prompt_tokens": 0, "cached_tokens": 0, "completion_tokens": 1_000_000},
                       "spend_delta_nano_usd": 0,
                       "summarizer_prompt_tokens": 1_000_000, "summarizer_completion_tokens": 0},
        }
        # 750k fresh input at 0.40, 250k cached at 0.10, 1M output at 1.60, 1M summarizer input at 0.10.
        self.assertAlmostEqual(fto.call_cost_usd(row, PRICES), 0.325 + 1.60 + 0.10)

    def test_a_missing_rate_uses_the_gateway_ledger(self) -> None:
        row = {
            "plain": {"usage": {"prompt_tokens": 1, "cached_tokens": 0, "completion_tokens": 0},
                      "spend_delta_nano_usd": 1_500_000_000},
            "folded": {"usage": {"prompt_tokens": 1, "cached_tokens": 0, "completion_tokens": 0},
                       "spend_delta_nano_usd": 500_000_000},
        }
        self.assertAlmostEqual(fto.call_cost_usd(row, {}), 2.0)

    def test_a_run_stops_once_spend_passes_the_cap_and_keeps_the_partial_file(self) -> None:
        calls = {"n": 0}

        def arm(*_args, **_kwargs):
            calls["n"] += 1
            return {
                "finish_reason": "stop",
                "spend_delta_nano_usd": 0,
                "folded_turns": 0,
                "request_id": f"req-{calls['n']}",
                "fold_summary": None,
                "retries": 0,
                "usage": {"prompt_tokens": 1_000_000, "cached_tokens": 0, "completion_tokens": 0},
                "content": '{"answer": 39}',
                "payload": {"choices": [{"message": {"content": '{"answer": 39}'}, "finish_reason": "stop"}]},
            }

        original_arm, original_metrics = fr._arm, fr._metrics
        fr._arm, fr._metrics = arm, lambda *_args, **_kwargs: {}
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "out.json"
                report, code = fto.run_agent(
                    base_url="http://stub", plain_key="p", folded_key="f", admin_key="a",
                    model="m", profile="lifecycle", output=output, max_calls=5,
                    prices=PRICES, max_cost_usd=0.50,
                )
                self.assertTrue(output.exists())
        finally:
            fr._arm, fr._metrics = original_arm, original_metrics
        # Both arms of the first call run (0.40 + 0.40), then the cap stops the rest.
        self.assertEqual(calls["n"], 2)
        self.assertEqual(code, 2)
        self.assertEqual(len(report["calls"]), 1)
        self.assertIn("passed the 0.50 USD limit", report["error"]["message"])
        self.assertEqual(report["settings"]["max_cost_usd"], 0.50)

    def test_without_a_cap_the_run_is_not_stopped_for_cost(self) -> None:
        calls = {"n": 0}

        def arm(*_args, **_kwargs):
            calls["n"] += 1
            return {
                "finish_reason": "stop",
                "spend_delta_nano_usd": 0,
                "folded_turns": 0,
                "request_id": "req",
                "fold_summary": None,
                "retries": 0,
                "usage": {"prompt_tokens": 10, "cached_tokens": 0, "completion_tokens": 1},
                "content": '{"answer": 39}',
                "payload": {"choices": [{"message": {"content": '{"answer": 39}'}, "finish_reason": "stop"}]},
            }

        original_arm, original_metrics = fr._arm, fr._metrics
        fr._arm, fr._metrics = arm, lambda *_args, **_kwargs: {}
        try:
            with tempfile.TemporaryDirectory() as tmp:
                report, _code = fto.run_agent(
                    base_url="http://stub", plain_key="p", folded_key="f", admin_key="a",
                    model="m", profile="lifecycle", output=Path(tmp) / "out.json", max_calls=2,
                    prices=PRICES,
                )
        finally:
            fr._arm, fr._metrics = original_arm, original_metrics
        self.assertEqual(calls["n"], 4)
        self.assertEqual(len(report["calls"]), 2)
        self.assertNotIn("max_cost_usd", report["settings"])
        self.assertNotIn("error", report)


if __name__ == "__main__":
    unittest.main()
