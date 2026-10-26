"""Unit tests for tools/fold_tools.py."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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
        self.assertEqual(agg["total_spend_nano_usd"], {"plain": 300, "folded": 150})

    def test_refetch_counts(self) -> None:
        rows = [self.row(True, True, True, refetch_p=0, refetch_f=1),
                self.row(True, True, True, refetch_p=0, refetch_f=2)]
        agg = fto.aggregates(rows, {"max_folds": 4}, {}, {})
        self.assertEqual(agg["refetches"], {"plain": 0, "folded": 3})


if __name__ == "__main__":
    unittest.main()
