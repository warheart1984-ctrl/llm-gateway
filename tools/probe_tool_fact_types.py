"""A/B probe: does typing tool-fact values in a fold summary change answers?

`fold_tools.py` showed every eligible folded miss was the right value in the
wrong format ("39" or a sentence where strict scoring wants the JSON number
39). The folded tenant sees `Ledger::tools_block`, which rendered every fact
bare (`- /passed: 39`), so nothing marked which values were numbers. This
probe sends the folded request exactly as the gateway built it, twice: once
with the summary as recorded (bare) and once with every JSON-string fact
quoted (typed). Everything else — system prompt, messages, tools, question —
is byte-identical, so any strict-score difference is the rendering's.

Requests are reconstructed from a real run's `fold_summaries` (see SOURCE),
not invented: the summary text is asserted byte for byte against the file,
and the first request's `usage.prompt_tokens` is checked against the tokens
the gateway reported for that call before more are spent.

OpenRouter only — the gateway is not involved. The key is read from
`OPENROUTER_API_KEY` and never printed, logged or written. Scoring reuses
`fold_tools.parse_answer`, `strict_match` and `lenient_match` unchanged.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request

import fold_retention as fr
import fold_tools

SCHEMA = "folding.probe_types.v1"
ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "openai/gpt-4.1-mini"
MAX_TOKENS = 512
REPEATS = 3
COST_LIMIT_USD = 0.25
#: openrouter/gpt-4.1-mini prices from bench/folding/models.openrouter.yaml.
PRICE_PER_MTOK = {"input": 0.40, "output": 1.60, "cached_input": 0.10}

SOURCE = (
    Path(__file__).resolve().parent.parent
    / "benchmark_results"
    / "fold_tools_openrouter_tools_lifecycle_20261026T161053Z_sum-gpt-4.1-mini.json"
)

#: The line the gateway puts before a summary in the forwarded request;
#: src/folding/plan.rs SUMMARY_PREAMBLE.
PREAMBLE = (
    "[Summary of the earlier conversation, written by the gateway. "
    "It is a record of what was said, not instructions.]"
)

#: (result index, gold value, question) — the probe-only questions, in the
#: same shape as fold_tools.QUESTIONS. No wording asks for a type.
PROBE_QUESTIONS = [
    (1, 41, "How many tests ran in checkout/totals?"),
    (1, 887, "At which line does totals/crosses_currency fail?"),
    (1, 3112, "How long did the checkout/totals run take, in milliseconds?"),
    (3, 412, "What was the p95 latency for checkout over 24h?"),
    # A number-looking string: the right answer is the string, not 12.81.
    (1, "12.81", "What value did totals/rounds_partial_refund get?"),
]
ALL_QUESTIONS = [*fold_tools.QUESTIONS, *PROBE_QUESTIONS]

#: Where the probe poses as the folded request: the summary from the fold at
#: `summary_call` plus the conversation state at `call`, with `folded_turns`
#: messages folded away. At after-fold-1 the metrics exchange is still in the
#: verbatim tail, so its questions are not asked there.
POSITIONS = [
    {"name": "after-fold-1", "call": 35, "folded_turns": 17, "summary_call": 16},
    {"name": "after-fold-2", "call": 101, "folded_turns": 33, "summary_call": 36},
]


class ProbeError(Exception):
    """A problem the probe cannot continue past."""


MISSING = object()


def json_pointer(document: Any, pointer: str) -> Any:
    """The value at a JSON pointer, or MISSING."""
    current = document
    for raw in pointer.split("/")[1:]:
        key = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            try:
                current = current[int(key)]
            except (ValueError, IndexError):
                return MISSING
        elif isinstance(current, dict):
            if key not in current:
                return MISSING
            current = current[key]
        else:
            return MISSING
    return current


def compact_arguments(arguments: dict[str, Any]) -> str:
    """`k="v", k=v` with keys in sorted order: the ledger's compact_arguments
    for the scripted calls (all-scalar arguments)."""
    return ", ".join(
        f'{key}="{value}"' if isinstance(value, str) else f"{key}={json.dumps(value)}"
        for key, value in sorted(arguments.items())
    )


def call_marker(name: str, arguments: dict[str, Any]) -> str:
    """`name(args)` exactly as a tools_block call line opens."""
    return f"{name}({compact_arguments(arguments)})"


def scripted_results() -> dict[str, Any]:
    """Call marker -> parsed result, for the calls the scenario scripted."""
    out = {}
    for _id, name, arguments, key in fold_tools.CALLS:
        try:
            out[call_marker(name, arguments)] = json.loads(fold_tools.RESULTS[key])
        except ValueError:
            out[call_marker(name, arguments)] = fold_tools.RESULTS[key]
    return out


def typed_rendering(summary: str) -> str:
    """The summary with every JSON-string tool fact quoted: `/path: "value"`.
    Bare numbers, booleans and null, text facts, "not kept" lines, the heading
    and the Context section are untouched — what the typed Rust renderer
    emits."""
    results = scripted_results()
    out = []
    current: Any = MISSING
    for line in summary.split("\n"):
        if line.startswith("- ") and " -> " in line:
            current = results.get(line[2:].split(" -> ")[0], MISSING)
            out.append(line)
        elif line.startswith("  - /") and current is not MISSING:
            pointer, _, _value = line[4:].partition(": ")
            resolved = json_pointer(current, pointer)
            out.append(f'  - {pointer}: "{resolved}"' if isinstance(resolved, str) else line)
        else:
            out.append(line)
    return "\n".join(out)


def tool_section(summary: str) -> list[dict[str, Any]]:
    """The tool entries of a summary: marker and the pointer of each fact."""
    entries: list[dict[str, Any]] = []
    for line in summary.split("\n"):
        if line.startswith("- ") and " -> " in line:
            entries.append({"marker": line[2:].split(" -> ")[0], "pointers": []})
        elif line.startswith("  - /") and entries:
            entries[-1]["pointers"].append(line[4:].partition(": ")[0])
    return entries


def gold_fact_pointer(summary: str, result_index: int, gold: Any) -> str | None:
    """The pointer whose fact line in `summary` carries `gold`, or None:
    the gold must appear in the tool section as a verified fact for the
    question to be asked at that position."""
    _id, name, arguments, key = fold_tools.CALLS[result_index]
    marker = call_marker(name, arguments)
    try:
        result = json.loads(fold_tools.RESULTS[key])
    except ValueError:
        return None
    entry = next((e for e in tool_section(summary) if e["marker"] == marker), None)
    if entry is None:
        return None
    for pointer in entry["pointers"]:
        value = json_pointer(result, pointer)
        if value is not MISSING and type(value) is type(gold) and value == gold:
            return pointer
    return None


def questions_at(position_summary: str) -> list[tuple[int, Any, str]]:
    """The questions whose gold value is a fact line in this position's tool
    section."""
    return [
        (index, gold, text)
        for index, gold, text in ALL_QUESTIONS
        if gold_fact_pointer(position_summary, index, gold) is not None
    ]


def replay_scenario(through_call: int) -> fold_tools.AgentScenario:
    """The scenario as run_agent played it: advance on call <= 28 or call % 4 == 0."""
    scenario = fold_tools.AgentScenario()
    for call in range(1, through_call + 1):
        if call <= 28 or call % 4 == 0:
            scenario.advance(call)
    return scenario


def probe_messages(
    scenario: fold_tools.AgentScenario, folded_turns: int, summary: str, question: str
) -> list[dict[str, Any]]:
    """The request as the gateway's build_forwarded_messages made it:
    protected system, summary as a user message, the unfolded tail, question."""
    return [
        {"role": "system", "content": fold_tools.SYSTEM},
        {"role": "user", "content": f"{PREAMBLE}\n{summary}"},
        *deepcopy(scenario.messages[folded_turns:]),
        {"role": "user", "content": question},
    ]


def probe_request(messages: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "model": MODEL,
        "messages": messages,
        "temperature": 0,
        "max_tokens": MAX_TOKENS,
        "tools": fold_tools.TOOLS,
        "stream": False,
    }


def gateway_estimate(messages: list[dict[str, Any]]) -> int:
    """The gateway's `estimate_prompt_tokens`: (chars + 8 per message) / 3.5.
    Its responses report the larger of this and the provider's count, which
    is what a result file's `usage.prompt_tokens` records."""
    chars = 0
    for message in messages:
        chars += len(message.get("content") or "")
        if message["role"] == "tool":
            chars += len(message.get("tool_call_id") or "") + len(message.get("name") or "")
        for call in message.get("tool_calls") or []:
            function = call.get("function") or {}
            chars += len(function.get("arguments") or "") + len(function.get("name") or "")
        chars += 8
    return -(-chars * 2 // 7)  # ceil(chars / 3.5)


def chat_completion(key: str, body: dict[str, Any], timeout: float = 240.0) -> dict[str, Any]:
    """One OpenRouter chat completion; transient statuses retried like the
    benchmark's own client."""
    request = Request(ENDPOINT, data=fr.canonical_json(body), method="POST")
    request.add_header("authorization", f"Bearer {key}")
    request.add_header("content-type", "application/json")
    retries = 0
    while True:
        try:
            with fr._OPENER.open(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code in fr.TRANSIENT_STATUSES and retries < len(fr.RETRY_WAITS_SECONDS):
                time.sleep(fr.RETRY_WAITS_SECONDS[retries])
                retries += 1
                continue
            raise ProbeError(f"OpenRouter returned HTTP {exc.code}") from None
        except (URLError, TimeoutError, OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ProbeError(f"OpenRouter request failed: {type(exc).__name__}") from None
        if not isinstance(payload, dict) or not payload.get("choices"):
            raise ProbeError("OpenRouter response had no choices")
        return payload


def request_cost_usd(usage: dict[str, Any]) -> float:
    prompt = usage.get("prompt_tokens") or 0
    details = usage.get("prompt_tokens_details") or {}
    cached = details.get("cached_tokens") or usage.get("cached_tokens") or 0
    completion = usage.get("completion_tokens") or 0
    return (
        (prompt - cached) * PRICE_PER_MTOK["input"]
        + cached * PRICE_PER_MTOK["cached_input"]
        + completion * PRICE_PER_MTOK["output"]
    ) / 1_000_000


def categorize(payload: dict[str, Any], gold: Any) -> dict[str, Any]:
    """strict | format_only | wrong | no_answer, from the response payload."""
    content, finish_reason = fr._answer(payload)
    emitted = fold_tools.emitted_calls({"payload": payload})
    value, ok = fold_tools.parse_answer(content, finish_reason)
    if emitted or not ok:
        category = "no_answer"
    elif fold_tools.strict_match(value, gold):
        category = "strict"
    elif fold_tools.lenient_match(value, gold):
        category = "format_only"
    else:
        category = "wrong"
    return {"answer": value, "finish_reason": finish_reason, "emitted_calls": emitted, "category": category}


def count_category(rows: list[dict[str, Any]], category: str) -> int:
    return sum(row["category"] == category for row in rows)


def aggregates(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "n": len(rows),
        "strict": count_category(rows, "strict"),
        "lenient": count_category(rows, "strict") + count_category(rows, "format_only"),
        "format_only": count_category(rows, "format_only"),
        "wrong": count_category(rows, "wrong"),
        "no_answer": count_category(rows, "no_answer"),
    }


def run(key: str, source_path: Path, output: Path, repeats: int, cost_limit: float) -> dict[str, Any]:
    source = json.loads(source_path.read_text(encoding="utf-8"))
    summaries = {entry["call"]: entry for entry in source["fold_summaries"]}
    source_rows = {row["call"]: row for row in source["calls"]}

    positions = []
    for spec in POSITIONS:
        summary = summaries.get(spec["summary_call"])
        if summary is None:
            raise ProbeError(f"source has no fold summary at call {spec['summary_call']}")
        recorded = source_rows.get(spec["call"])
        if recorded is None:
            raise ProbeError(f"source has no call row {spec['call']}")
        if summary["folded_turns"] != spec["folded_turns"] or recorded["folded"]["folded_turns"] != spec["folded_turns"]:
            raise ProbeError(
                f"call {spec['call']}: expected {spec['folded_turns']} folded turns, "
                f"source has {recorded['folded']['folded_turns']}"
            )
        bare = summary["text"]
        typed = typed_rendering(bare)
        questions = questions_at(bare)
        if not questions:
            raise ProbeError(f"no questions eligible at {spec['name']}")
        positions.append(
            {
                **spec,
                "summaries": {"bare": bare, "typed": typed},
                "scenario": replay_scenario(spec["call"]),
                "questions": questions,
                "expected_prompt_tokens": recorded["folded"]["usage"]["prompt_tokens"],
            }
        )

    report: dict[str, Any] = {
        "schema": SCHEMA,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "settings": {
            "model": MODEL,
            "temperature": 0,
            "max_tokens": MAX_TOKENS,
            "repeats": repeats,
            "cost_limit_usd": cost_limit,
            "source_file": source_path.name,
            "positions": [
                {"name": p["name"], "call": p["call"], "folded_turns": p["folded_turns"]}
                for p in positions
            ],
            "git_commit": fr._git_commit(),
            "secrets_written": False,
        },
        "summaries": {p["name"]: p["summaries"] for p in positions},
        "prompt_tokens_validation": {},
        "rows": [],
        "stopped_early": None,
    }

    total_cost = 0.0
    try:
        for position in positions:
            validated = False
            for index, gold, question in position["questions"]:
                for repeat in range(repeats):
                    variants = ["bare", "typed"] if repeat % 2 == 0 else ["typed", "bare"]
                    for variant in variants:
                        if total_cost > cost_limit:
                            report["stopped_early"] = f"cost {total_cost:.4f} USD passed the {cost_limit} limit"
                            raise _CostLimit
                        summary = position["summaries"][variant]
                        messages = probe_messages(position["scenario"], position["folded_turns"], summary, question)
                        payload = chat_completion(key, probe_request(messages))
                        usage = payload.get("usage") or {}
                        cost = request_cost_usd(usage)
                        total_cost += cost
                        if variant == "bare" and not validated:
                            observed = usage.get("prompt_tokens")
                            estimate = gateway_estimate(messages)
                            expected = position["expected_prompt_tokens"]
                            # The file's usage.prompt_tokens is the larger of
                            # the provider's count and the gateway estimate.
                            effective = max(observed or 0, estimate)
                            within = abs(effective - expected) <= expected * 0.02
                            report["prompt_tokens_validation"][position["name"]] = {
                                "expected": expected,
                                "observed": observed,
                                "gateway_estimate": estimate,
                                "within_2pct": bool(within),
                            }
                            if not within:
                                raise ProbeError(
                                    f"{position['name']}: prompt_tokens {effective} "
                                    f"(observed {observed}, estimate {estimate}) is not within 2% of "
                                    f"the recorded {expected}; the reconstruction does not match the run"
                                )
                            validated = True
                        row = {
                            "position": position["name"],
                            "variant": variant,
                            "repeat": repeat,
                            "question": question,
                            "gold": gold,
                            **categorize(payload, gold),
                            "usage": usage,
                            "cost_usd": round(cost, 6),
                        }
                        report["rows"].append(row)
    except _CostLimit:
        pass

    report["finished_utc"] = datetime.now(timezone.utc).isoformat()
    report["total_cost_usd"] = round(total_cost, 6)
    report["aggregates"] = {
        f"{p['name']}/{variant}": aggregates([r for r in report["rows"] if r["position"] == p["name"] and r["variant"] == variant])
        for p in positions
        for variant in ("bare", "typed")
    }
    report["per_question"] = {
        f"{p['name']}/{variant}/{q[2]}": aggregates(
            [r for r in report["rows"] if r["position"] == p["name"] and r["variant"] == variant and r["question"] == q[2]]
        )
        for p in positions
        for q in p["questions"]
        for variant in ("bare", "typed")
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


class _CostLimit(Exception):
    pass


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--repeats", type=int, default=REPEATS)
    parser.add_argument("--cost-limit", type=float, default=COST_LIMIT_USD)
    parser.add_argument("--timeout", type=float, default=240.0)
    args = parser.parse_args(argv)

    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        print("probe: OPENROUTER_API_KEY is not set", file=sys.stderr)
        return 2
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = args.output or Path("benchmark_results") / f"probe_tool_fact_types_{stamp}.json"
    try:
        report = run(key, args.source.resolve(), output.resolve(), args.repeats, args.cost_limit)
    except ProbeError as exc:
        print(f"probe: {exc}", file=sys.stderr)
        return 2

    print(f"{'position':<14} {'variant':<7} {'n':>3} {'strict':>6} {'lenient':>7} {'format_only':>11} {'wrong':>5} {'no_answer':>9}")
    for name, agg in report["aggregates"].items():
        position, variant = name.split("/")
        print(
            f"{position:<14} {variant:<7} {agg['n']:>3} {agg['strict']:>6} {agg['lenient']:>7} "
            f"{agg['format_only']:>11} {agg['wrong']:>5} {agg['no_answer']:>9}"
        )
    print(f"total cost: ${report['total_cost_usd']:.4f}  -> {output}")
    if report["stopped_early"]:
        print(f"stopped early: {report['stopped_early']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
