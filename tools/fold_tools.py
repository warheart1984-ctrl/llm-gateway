"""Paired agent benchmark for folding tool turns (`folding.fold_tool_turns`).

`fold_retention.py` asks whether a folded conversation still recalls facts the
user stated. This asks whether a conversation full of tool calls still knows
what its tools said: the scenario is a scripted agent investigation — user
turns, assistant `tool_calls`, and `Tool` results served by the harness — so
both tenants receive byte-identical request bodies while the results are real
work for the model to keep straight: several KB of JSON, and some errors.

Later calls ask for values that live only inside earlier tool results. The
request also advertises the same `tools`, so a model that no longer trusts its
memory can call one again: every such call is counted, and a call for a path
or filter already fetched is counted a refetch. A folded summary that keeps
the verified tool ledger should answer from it instead of fetching again.

Answers are scored two ways against the scripted gold values. Strict equality
requires the same type and value. Lenient equality also accepts an integer
given as its canonical decimal string, or embedded as the only number in a
short sentence. Both accuracies are reported, with a count of answers that
miss only on format. The exit status still uses the strict score. A question
counts against folding only where the result it depends on was folded away
and the unfolded tenant answered correctly, strictly, on the same call.

Exit status: 0 no regression, 1 regression, 2 incomplete.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

import fold_retention as fr

SYSTEM = (
    "You are an operations agent investigating a checkout deploy. The conversation "
    "shows tool calls you made and their results. Answer the user's question from "
    "what the conversation already contains; call a tool again only if the answer "
    "is not there. Reply as one JSON object with exactly one key, \"answer\", whose "
    "value is a string, number, boolean or null — no other text."
)

#: The tools the script "called" and the request keeps offering. Arguments are
#: canonical JSON strings the harness compares a refetch against.
TOOLS = [
    {"type": "function", "function": {
        "name": "read_file",
        "description": "Read a file from the deploy checkout.",
        "parameters": {"type": "object", "properties": {"path": {"type": "string"}},
                       "required": ["path"]},
    }},
    {"type": "function", "function": {
        "name": "query_logs",
        "description": "Query structured service logs.",
        "parameters": {"type": "object", "properties": {"service": {"type": "string"},
                       "filter": {"type": "string"}}, "required": ["service"]},
    }},
    {"type": "function", "function": {
        "name": "run_tests",
        "description": "Run a filtered test selection.",
        "parameters": {"type": "object", "properties": {"filter": {"type": "string"}},
                       "required": ["filter"]},
    }},
]

#: Deterministic tool results: several KB of JSON each, some of them errors.
RESULTS = {
    "deploy_yaml": json.dumps({
        "service": "checkout", "image_tag": "v2.14.7", "replicas": 3,
        "rollout": {"strategy": "canary", "steps": [{"weight": 10}, {"weight": 50}, {"weight": 100}]},
        "env": {f"CHECKOUT_FEATURE_FLAG_{i:03d}": f"rollout-setting-{i}-enabled" for i in range(90)},
        "resources": {"cpu": "750m", "memory": "1Gi"},
        "annotations": {f"team.checkout/deploy-annotation-{i:02d}": f"audit note {i} recorded by the release pipeline" for i in range(40)},
    }, separators=(",", ":")),
    "totals_test": json.dumps({
        "suite": "checkout/totals", "ran": 41, "passed": 39, "failed": [
            {"name": "totals/rounds_partial_refund", "line": 1043,
             "want": "12.80", "got": "12.81"},
            {"name": "totals/crosses_currency", "line": 887,
             "want": "EUR 9.99", "got": "USD 9.99"},
        ],
        "duration_ms": 3112,
        "cases": [{"name": f"totals/case_{i:02d}", "ok": True} for i in range(39)],
    }, separators=(",", ":")),
    "error_log": json.dumps({
        "error": "log stream unavailable",
        "code": "LOG_BACKEND_DOWN",
        "detail": {"service": "checkout", "window": "15m",
                   "attempts": [{"at": "14:02", "status": 502}, {"at": "14:03", "status": 502}]},
    }, separators=(",", ":")),
    "metrics_json": json.dumps({
        "service": "checkout", "window": "24h",
        "p50_ms": 118, "p95_ms": 412, "p99_ms": 1307,
        "error_rate": 0.0042,
        "buckets": [{"minute": f"{m:02d}", "requests": 1800 + m * 7,
                     "errors": m % 3, "region": "eu-north-1"} for m in range(120)],
    }, separators=(",", ":")),
    "notes_txt": (
        "deploy notes: image v2.14.7 rolled out clean at 14:05, "
        "canary held at 50% for twenty minutes while the metrics endpoint was polled."
    ),
}

#: Scripted calls: (id, name, arguments-JSON, result key).
CALLS = [
    ("call_01", "read_file", {"path": "deploy/checkout.yaml"}, "deploy_yaml"),
    ("call_02", "run_tests", {"filter": "checkout/totals"}, "totals_test"),
    ("call_03", "query_logs", {"service": "checkout", "filter": "error"}, "error_log"),
    ("call_04", "query_logs", {"service": "checkout", "filter": "requests"}, "metrics_json"),
    ("call_05", "read_file", {"path": "deploy/notes.txt"}, "notes_txt"),
]

#: Later questions, each answerable only from an earlier result. (call index
#: of the result it needs, gold value).
QUESTIONS = [
    (0, "v2.14.7", "What image tag is the checkout deploy running?"),
    (1, 1043, "At which line does totals/rounds_partial_refund fail?"),
    (2, "log stream unavailable", "Why could the error log not be read?"),
    (3, 1307, "What was the p99 latency for checkout over 24h?"),
    (1, 39, "How many cases in checkout/totals passed?"),
    (0, 3, "How many replicas does the deploy declare?"),
]

#: Calls at which the exchanges are appended; questions start afterwards.
FILLER = (
    "Workshop note {index:02d}: the team reviewed the packaging checklist, discussed "
    "the order of verification steps, and filed the meeting notes. This discussion "
    "introduced no changes to the project requirements."
)


class AgentScenario:
    """A scripted agent investigation. The exchanges and their results are
    written by the harness, so both arms get identical bodies; the model only
    answers the questions. Gold values never enter questions or history."""

    name = "tools"

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = [
            {"role": "user", "content": "Investigate the checkout deploy; tools have been run as asked."}
        ]
        #: Message index of each tool result, for eligibility.
        self.result_at: list[int] = []
        self.made = 0

    def _exchange(self, call: tuple[str, str, dict[str, Any], str]) -> None:
        call_id, name, arguments, result_key = call
        self.messages.append({
            "role": "assistant", "content": f"Calling {name}.",
            "tool_calls": [{"id": call_id, "type": "function",
                            "function": {"name": name, "arguments": json.dumps(arguments)}}],
        })
        self.messages.append({
            "role": "tool", "tool_call_id": call_id, "name": name,
            "content": RESULTS[result_key],
        })
        self.result_at.append(len(self.messages))
        self.made += 1

    def advance(self, call: int) -> None:
        # Two exchanges early, then filler, then the remaining three spread
        # out so early results sit deep inside the folded prefix.
        schedule = {1: 0, 2: 1, 9: 2, 17: 3, 25: 4}
        if call in schedule:
            self._exchange(CALLS[schedule[call]])
        else:
            self.messages.append({"role": "user", "content": FILLER.format(index=call)})

    def question(self, call: int) -> tuple[int, Any, str]:
        """(result index, gold value, question) for this call."""
        result_index, gold, text = QUESTIONS[(call - 1) % len(QUESTIONS)]
        return result_index, gold, text

    def request(self, model: str, max_tokens: int, question: str) -> dict[str, Any]:
        return {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM},
                *deepcopy(self.messages),
                {"role": "user", "content": question},
            ],
            "temperature": 0,
            "max_tokens": max_tokens,
            "tools": TOOLS,
            "stream": False,
        }


def parse_answer(content: Any, finish_reason: str | None) -> tuple[Any, bool]:
    """The {"answer": value} object, or failure."""
    try:
        if finish_reason != "stop" or not isinstance(content, str):
            return None, False
        parsed = json.loads(content)
        if not isinstance(parsed, dict) or set(parsed) != {"answer"}:
            return None, False
        return parsed["answer"], True
    except (ValueError, TypeError):
        return None, False


# A number token, including a decimal. Used to reject a sentence that names
# more than one number, or a different one.
_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")
# "fails at line 1043" and "39 tests in checkout/totals passed." are short.
# A long reply that happens to contain the gold number is not an answer.
_LENIENT_MAX_CHARS = 160
_LENIENT_MAX_WORDS = 24


def strict_match(value: Any, gold: Any) -> bool:
    """Same Python type and value. `True` is not `1`."""
    return type(value) is type(gold) and value == gold


def lenient_match(value: Any, gold: Any) -> bool:
    """Strict match, or an integer written as its canonical decimal string, or
    as the only number in a short sentence.

    "39" and "fails at line 1043" match. "12.81", "at line 39" for 1043,
    "1043 and 887", and "10430" do not. Booleans and non-integer golds stay
    strict: `bool` is a subclass of `int` and must not take this path.
    """
    if strict_match(value, gold):
        return True
    if isinstance(gold, bool) or not isinstance(gold, int) or isinstance(value, bool):
        return False
    if not isinstance(value, str):
        return False
    text = value.strip()
    if text == str(gold):
        return True
    if len(text) > _LENIENT_MAX_CHARS or len(text.split()) > _LENIENT_MAX_WORDS:
        return False
    numbers = _NUMBER.findall(text)
    if len(numbers) != 1 or "." in numbers[0]:
        return False
    token = numbers[0]
    if token != str(int(token)):
        return False
    return int(token) == gold


def emitted_calls(arm: dict[str, Any]) -> list[dict[str, str]]:
    """Tool calls the model emitted instead of answering, as (name, canonical args)."""
    message = (arm["payload"].get("choices") or [{}])[0].get("message") or {}
    calls = message.get("tool_calls") or []
    out = []
    for call in calls:
        function = call.get("function") or {}
        try:
            args = json.dumps(json.loads(function.get("arguments") or "{}"), sort_keys=True)
        except ValueError:
            args = function.get("arguments") or ""
        out.append({"name": function.get("name") or "", "arguments": args})
    return out


#: The calls the scenario already fetched, as (name, canonical args) sets.
FETCHED = {(name, json.dumps(arguments, sort_keys=True)) for _id, name, arguments, _key in CALLS}


def run_agent(
    *,
    base_url: str,
    plain_key: str,
    folded_key: str,
    admin_key: str,
    model: str,
    profile: str,
    output: Path,
    max_calls: int = 60,
    max_tokens: int = 512,
    timeout: float = 240.0,
    prices: dict[str, float | None] | None = None,
) -> tuple[dict[str, Any], int]:
    if profile not in fr.PROFILES:
        raise fr.BenchmarkError(f"unknown profile {profile!r}")
    scenario = AgentScenario()
    profile_data = fr.PROFILES[profile]
    started = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    last_summary: str | None = None
    error: dict[str, str] | None = None
    try:
        for call in range(1, max_calls + 1):
            metrics_before = fr._metrics(base_url, admin_key, timeout)
            if call <= 28 or call % 4 == 0:
                scenario.advance(call)
            result_index, gold, text = scenario.question(call)
            body = scenario.request(model, max_tokens, text)
            plain_first = call % 2 == 1
            if plain_first:
                plain = fr._arm(base_url, plain_key, deepcopy(body), timeout)
                folded = fr._arm(base_url, folded_key, deepcopy(body), timeout)
            else:
                folded = fr._arm(base_url, folded_key, deepcopy(body), timeout)
                plain = fr._arm(base_url, plain_key, deepcopy(body), timeout)

            def score(arm: dict[str, Any]) -> dict[str, Any]:
                value, ok = parse_answer(arm["content"], arm["finish_reason"])
                calls = emitted_calls(arm)
                return {
                    "answer": value,
                    "correct": bool(ok and strict_match(value, gold)),
                    "lenient_correct": bool(ok and lenient_match(value, gold)),
                    "tool_calls": calls,
                    "refetches": [c for c in calls if (c["name"], c["arguments"]) in FETCHED],
                }

            plain_score, folded_score = score(plain), score(folded)
            result_message_at = scenario.result_at[result_index] if result_index < len(scenario.result_at) else 0
            eligible = bool(
                plain_score["correct"] and 0 < result_message_at <= folded["folded_turns"]
            )
            if folded["fold_summary"] is not None and folded["fold_summary"] != last_summary:
                last_summary = folded["fold_summary"]
                summaries.append({"call": call, "folded_turns": folded["folded_turns"],
                                  "chars": len(last_summary), "text": last_summary})
            metrics_after = fr._metrics(base_url, admin_key, timeout)
            prompt_tokens = lambda m: fr._metric(m, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "prompt"})
            completion_tokens = lambda m: fr._metric(m, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "completion"})
            rows.append({
                "call": call,
                "order": "plain-first" if plain_first else "folded-first",
                "question": text,
                "gold": gold,
                "result_message_at": result_message_at,
                "needs_tool": CALLS[result_index][1],
                "eligible": eligible,
                "plain": {"correct": plain_score["correct"], "lenient_correct": plain_score["lenient_correct"],
                          "answer": plain_score["answer"],
                          "finish_reason": plain["finish_reason"],
                          "spend_delta_nano_usd": plain["spend_delta_nano_usd"],
                          "usage": plain["usage"], "retries": plain["retries"],
                          "tool_calls": plain_score["tool_calls"],
                          "refetches": plain_score["refetches"]},
                "folded": {"correct": folded_score["correct"], "lenient_correct": folded_score["lenient_correct"],
                           "answer": folded_score["answer"],
                           "finish_reason": folded["finish_reason"],
                           "spend_delta_nano_usd": folded["spend_delta_nano_usd"],
                           "usage": folded["usage"], "retries": folded["retries"],
                           "tool_calls": folded_score["tool_calls"],
                           "refetches": folded_score["refetches"],
                           "folded_turns": folded["folded_turns"],
                           "request_id": folded["request_id"],
                           "fold_outcomes": fr.fold_outcome_deltas(metrics_before, metrics_after),
                           "failed_checks": fr.metric_deltas(metrics_before, metrics_after, "gw_fold_checks_failed_total", "check"),
                           "fold_count": fr._metric(metrics_after, "gw_folds_total", {"tenant": "folded", "outcome": "committed"}),
                           "summarizer_prompt_tokens": prompt_tokens(metrics_after) - prompt_tokens(metrics_before),
                           "summarizer_completion_tokens": completion_tokens(metrics_after) - completion_tokens(metrics_before)},
            })
    except fr.BenchmarkError as exc:
        error = {"type": type(exc).__name__, "message": str(exc)}

    report: dict[str, Any] = {
        "schema": "folding.tools.v1",
        "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "settings": {
            "model": model, "scenario": AgentScenario.name, "profile": profile,
            "max_calls": max_calls, "max_tokens": max_tokens, "temperature": 0,
            "fold_cap": profile_data["max_folds"], "secrets_written": False,
            "git_commit": fr._git_commit(), "prices": prices or {},
        },
        "calls": rows,
        "fold_summaries": summaries,
    }
    if error is not None:
        report["error"] = error
        _write(output, report)
        return report, 2

    report["aggregates"] = aggregates(rows, profile_data, prices or {}, fr._metrics(base_url, admin_key, timeout))
    _write(output, report)
    agg = report["aggregates"]
    if not rows or agg["eligible_questions"] == 0:
        return report, 2
    regressed = agg["eligible_wrong"] > 0
    return report, 1 if regressed else 0


def aggregates(rows: list[dict[str, Any]], profile_data: dict[str, Any],
               prices: dict[str, float | None], final_metrics: dict) -> dict[str, Any]:
    asked = [row for row in rows if row["result_message_at"] > 0]
    eligible = [row for row in asked if row["eligible"]]
    def matched(row: dict[str, Any], arm: str, field: str) -> bool:
        arm_row = row[arm]
        if field == "lenient_correct" and field not in arm_row:
            return bool(arm_row["correct"])
        return bool(arm_row[field])
    def rate(arm: str, field: str = "correct") -> float | None:
        return sum(matched(row, arm, field) for row in asked) / len(asked) if asked else None
    def format_only(arm: str) -> int:
        return sum(not matched(row, arm, "correct") and matched(row, arm, "lenient_correct") for row in asked)
    def refetches(arm: str) -> int:
        return sum(len(row[arm]["refetches"]) for row in rows)
    def calls(arm: str) -> int:
        return sum(len(row[arm]["tool_calls"]) for row in rows)
    max_fold_count = max((row["folded"]["fold_count"] for row in rows), default=0)
    plain_spend = sum(row["plain"]["spend_delta_nano_usd"] for row in rows)
    folded_spend = sum(row["folded"]["spend_delta_nano_usd"] for row in rows)
    return {
        "questions_asked": len(asked),
        "accuracy": {"plain": rate("plain"), "folded": rate("folded")},
        "accuracy_lenient": {"plain": rate("plain", "lenient_correct"), "folded": rate("folded", "lenient_correct")},
        "format_only_misses": {"plain": format_only("plain"), "folded": format_only("folded")},
        "eligible_questions": len(eligible),
        "eligible_correct": sum(row["folded"]["correct"] for row in eligible),
        "eligible_wrong": sum(not row["folded"]["correct"] for row in eligible),
        "eligible_format_only": sum(
            not matched(row, "folded", "correct") and matched(row, "folded", "lenient_correct") for row in eligible
        ),
        # Of those, the answers that called again the tool whose result the
        # question needs, rather than guessing: what the summary asks for when
        # a value is gone. Still counted wrong above; the harness serves no
        # second result.
        "eligible_recalled": sum(
            not row["folded"]["correct"]
            and any(call["name"] == row.get("needs_tool") for call in row["folded"]["tool_calls"])
            for row in eligible
        ),
        "tool_calls_made": {"plain": calls("plain"), "folded": calls("folded")},
        "refetches": {"plain": refetches("plain"), "folded": refetches("folded")},
        "total_spend_nano_usd": {"plain": plain_spend, "folded": folded_spend},
        "folds": {
            "committed": max_fold_count,
            "rejected": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "rejected"}),
            "unprofitable": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "unprofitable"}),
            "failed_checks": fr.metric_deltas({}, final_metrics, "gw_fold_checks_failed_total", "check"),
        },
        "reached": "re-folded" if max_fold_count > profile_data["max_folds"] else (
            "frozen" if max_fold_count >= profile_data["max_folds"] else "warm"),
        "caching": fr._caching_aggregates(rows, prices),
    }


def _write(output: Path, report: dict[str, Any]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base-url", default=os.environ.get("LLM_GATEWAY_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--plain-key", default=os.environ.get("LLM_GATEWAY_PLAIN_KEY"))
    parser.add_argument("--folded-key", default=os.environ.get("LLM_GATEWAY_FOLDED_KEY"))
    parser.add_argument("--admin-key", default=os.environ.get("LLM_GATEWAY_ADMIN_KEY"))
    parser.add_argument("--model", required=True)
    parser.add_argument("--scenario", choices=[AgentScenario.name], default=AgentScenario.name)
    parser.add_argument("--profile", choices=sorted(fr.PROFILES), default="lifecycle")
    parser.add_argument("--max-calls", type=int, default=60)
    parser.add_argument("--max-tokens", type=int, default=512)
    parser.add_argument("--timeout", type=float, default=240.0)
    parser.add_argument("--main-input-per-mtok-usd", type=float)
    parser.add_argument("--main-output-per-mtok-usd", type=float)
    parser.add_argument("--main-cached-input-per-mtok-usd", type=float)
    parser.add_argument("--summarizer-input-per-mtok-usd", type=float)
    parser.add_argument("--summarizer-output-per-mtok-usd", type=float)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    missing = [name for name, value in (("--plain-key", args.plain_key), ("--folded-key", args.folded_key),
                                        ("--admin-key", args.admin_key)) if not value]
    if missing:
        parser.error(f"missing secret references: {', '.join(missing)}")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output or Path("benchmark_results") / f"fold_tools_{args.profile}_{stamp}.json").resolve()
    try:
        return run_agent(
            base_url=args.base_url, plain_key=args.plain_key, folded_key=args.folded_key,
            admin_key=args.admin_key, model=args.model, profile=args.profile, output=output,
            max_calls=args.max_calls, max_tokens=args.max_tokens, timeout=args.timeout,
            prices={
                "main_input_per_mtok_usd": args.main_input_per_mtok_usd,
                "main_output_per_mtok_usd": args.main_output_per_mtok_usd,
                "main_cached_input_per_mtok_usd": args.main_cached_input_per_mtok_usd,
                "summarizer_input_per_mtok_usd": args.summarizer_input_per_mtok_usd,
                "summarizer_output_per_mtok_usd": args.summarizer_output_per_mtok_usd,
            },
        )[1]
    except fr.BenchmarkError as exc:
        print(f"fold-tools: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
