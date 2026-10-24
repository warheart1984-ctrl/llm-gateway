"""Paired task-success benchmark for the conversation-folding gateway.

`fold_retention.py` asks whether a folded conversation still recalls its
facts. This asks whether it still does the task right: the conversation gives
standing rules in its own user turns (an answer format, a forbidden topic, a
required closing line, a date format, and later a change to one of them), and
every answer is checked against every rule in force, by a deterministic
checker, for both tenants. Recall is not enough: a summary can keep every fact
and drop "never mention BIRCH-204".

A rule counts against folding only where it was folded away and the unfolded
tenant obeyed it on the same call, as a fact does in `fold_retention.py`. The
report gives both tenants' task success rate (calls where every rule in force
was obeyed) next to the saving, so the trade-off is visible.

Exit status: 0 no regression, 1 regression, 2 incomplete. Without
`--max-success-drop`, any folded-away rule the folded tenant broke while the
unfolded one obeyed it is a regression; with it, the folded tenant's success
rate may fall that far below the unfolded tenant's before it is.
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
from typing import Any, Callable

import fold_retention as fr

SYSTEM = (
    "You are the assistant for a delivery project. Follow every standing rule the user "
    "has given in this conversation for as long as it stands; a later rule change replaces "
    "the earlier rule. Use the latest corrected facts."
)
TASKS = {
    "status": "Give a short status update for the delivery project: who owns it, where it is "
              "delivered, its budget, and its release date.",
    "budgets": "Which projects and budgets have come up in this conversation so far?",
}
FIRST_FOOTER = "Ref: CEDAR-731"
CHANGED_FOOTER = "Ref: CEDAR-731/B"
#: Calls after which a rule is given or changed.
DATES_RULE_CALL = 15
FOOTER_CHANGE_CALL = 60
#: Where the first three rules sit among the seed turns (message indexes).
SEED_RULE_POSITIONS = (8, 20, 32)
ANSWER_CHARS_KEPT = 1500


def _lines(text: str) -> list[str]:
    return [line.strip() for line in text.strip().splitlines() if line.strip()]


def follows_bullets(text: str, _expected: str | None = None) -> bool:
    """At most three lines, each a "- " bullet; a closing "Ref:" line is not one."""
    body = _lines(text)
    if body and body[-1].startswith("Ref:"):
        body = body[:-1]
    return 1 <= len(body) <= 3 and all(line.startswith("- ") for line in body)


def avoids_birch(text: str, _expected: str | None = None) -> bool:
    """No mention of BIRCH-204 or of Ana, its owner."""
    return re.search(r"birch|\bana\b", text, re.IGNORECASE) is None


def ends_with_footer(text: str, expected: str | None = None) -> bool:
    """The last line is exactly the closing line in force."""
    lines = _lines(text)
    return bool(lines) and lines[-1] == expected


def writes_dates_dotted(text: str, _expected: str | None = None) -> bool:
    """A DD.MM.YYYY date, and no YYYY-MM-DD one."""
    return (re.search(r"\b\d{4}-\d{2}-\d{2}\b", text) is None
            and re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", text) is not None)


@dataclass(frozen=True)
class Rule:
    name: str
    check: Callable[[str, str | None], bool]
    #: The tasks it is checked on.
    tasks: tuple[str, ...]


RULES = (
    Rule("bullets", follows_bullets, ("status", "budgets")),
    Rule("no_birch", avoids_birch, ("status", "budgets")),
    Rule("footer", ends_with_footer, ("status", "budgets")),
    Rule("dates", writes_dates_dotted, ("status",)),
)


class TaskScenario:
    """The mutating scenario's conversation, with standing rules given in user
    turns. Gold answers never enter the conversation."""

    name = "constraints"

    def __init__(self) -> None:
        self.facts = fr.MutatingScenario()
        self.messages = self.facts.messages
        self.footer = FIRST_FOOTER
        #: Message count when each rule took its current form; 0 until given.
        self.introduced_at = {rule.name: 0 for rule in RULES}
        seed = {
            SEED_RULE_POSITIONS[0]: ("bullets", "Standing rule for every answer from now on: at most three "
                                                "bullet points, each line starting with '- '."),
            SEED_RULE_POSITIONS[1]: ("no_birch", "Standing rule: never mention project BIRCH-204 or Ana in any "
                                                 "answer; that project is confidential."),
            SEED_RULE_POSITIONS[2]: ("footer", f"Standing rule: end every answer with this exact line: {FIRST_FOOTER}"),
        }
        # In order, so each rule lands at its own position.
        for position in sorted(seed):
            self.messages.insert(position, {"role": "user", "content": seed[position][1]})
        for index, message in enumerate(self.messages):
            for name, text in seed.values():
                if message["content"] == text:
                    self.introduced_at[name] = index + 1

    def advance(self, call: int, folds: int, fold_cap: int) -> None:
        self.facts.advance(call, folds, fold_cap)
        if call == DATES_RULE_CALL:
            self._give("dates", "Standing rule from now on: write every date as DD.MM.YYYY, for example 18.02.2027.")
        if call == FOOTER_CHANGE_CALL:
            self.footer = CHANGED_FOOTER
            self._give("footer", f"Rule change: from now on end every answer with this exact line instead: {CHANGED_FOOTER}")

    def _give(self, name: str, text: str) -> None:
        self.messages.append({"role": "user", "content": text})
        self.introduced_at[name] = len(self.messages)

    def task(self, call: int) -> str:
        return "status" if call % 2 == 1 else "budgets"

    def request(self, model: str, max_tokens: int, task: str) -> dict[str, Any]:
        return {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM},
                *deepcopy(self.messages),
                {"role": "user", "content": TASKS[task]},
            ],
            "temperature": 0,
            "max_tokens": max_tokens,
            "stream": False,
        }

    def expected(self, rule: str) -> str | None:
        return self.footer if rule == "footer" else None


def score(text: Any, finish_reason: str | None, scenario: TaskScenario, task: str) -> dict[str, bool]:
    """Each rule in force that applies to `task`: obeyed or not. An answer that
    did not finish, or is not text, obeys none."""
    usable = isinstance(text, str) and finish_reason == "stop"
    return {
        rule.name: bool(usable and rule.check(text, scenario.expected(rule.name)))
        for rule in RULES
        if scenario.introduced_at[rule.name] > 0 and task in rule.tasks
    }


def rule_eligible(introduced_at: int, folded_turns: int, plain_pass: bool) -> bool:
    """A rule counts against folding only where it was folded away and the
    unfolded tenant obeyed it."""
    return bool(plain_pass and 0 < introduced_at <= folded_turns)


def run_tasks(
    *,
    base_url: str,
    plain_key: str,
    folded_key: str,
    admin_key: str,
    model: str,
    profile: str,
    output: Path,
    max_calls: int = 120,
    max_tokens: int = 512,
    timeout: float = 240.0,
    prices: dict[str, float | None] | None = None,
    max_success_drop: float | None = None,
) -> tuple[dict[str, Any], int]:
    if profile not in fr.PROFILES:
        raise fr.BenchmarkError(f"unknown profile {profile!r}")
    if max_calls <= 0 or max_tokens <= 0 or timeout <= 0:
        raise fr.BenchmarkError("max_calls, max_tokens and timeout must be positive")
    scenario = TaskScenario()
    profile_data = fr.PROFILES[profile]
    started = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    last_summary: str | None = None
    error: dict[str, str] | None = None
    try:
        for call in range(1, max_calls + 1):
            metrics_before = fr._metrics(base_url, admin_key, timeout)
            committed = fr._metric(metrics_before, "gw_folds_total", {"tenant": "folded", "outcome": "committed"})
            scenario.advance(call, committed, profile_data["max_folds"])
            task = scenario.task(call)
            body = scenario.request(model, max_tokens, task)
            plain_first = call % 2 == 1
            if plain_first:
                plain = fr._arm(base_url, plain_key, deepcopy(body), timeout)
                folded = fr._arm(base_url, folded_key, deepcopy(body), timeout)
            else:
                folded = fr._arm(base_url, folded_key, deepcopy(body), timeout)
                plain = fr._arm(base_url, plain_key, deepcopy(body), timeout)
            plain_rules = score(plain["content"], plain["finish_reason"], scenario, task)
            folded_rules = score(folded["content"], folded["finish_reason"], scenario, task)
            rules = {
                name: {
                    "introduced_at": scenario.introduced_at[name],
                    "plain_pass": plain_rules[name],
                    "folded_pass": folded_rules[name],
                    "eligible": rule_eligible(scenario.introduced_at[name], folded["folded_turns"], plain_rules[name]),
                }
                for name in plain_rules
            }
            if folded["fold_summary"] is not None and folded["fold_summary"] != last_summary:
                last_summary = folded["fold_summary"]
                summaries.append({"call": call, "folded_turns": folded["folded_turns"], "chars": len(last_summary),
                                  "text": last_summary})
            metrics_after = fr._metrics(base_url, admin_key, timeout)

            def arm_row(arm: dict[str, Any], passed: dict[str, bool]) -> dict[str, Any]:
                row = {
                    "finish_reason": arm["finish_reason"],
                    "spend_delta_nano_usd": arm["spend_delta_nano_usd"],
                    "usage": arm["usage"],
                    "retries": arm["retries"],
                    "success": bool(passed) and all(passed.values()),
                }
                if passed and not row["success"]:
                    text = arm["content"] if isinstance(arm["content"], str) else ""
                    row["answer"] = text[:ANSWER_CHARS_KEPT]
                return row

            folded_row = arm_row(folded, folded_rules)
            prompt_tokens = lambda m: fr._metric(m, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "prompt"})
            completion_tokens = lambda m: fr._metric(m, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "completion"})
            folded_row.update({
                "folded_turns": folded["folded_turns"],
                "request_id": folded["request_id"],
                "fold_count": fr._metric(metrics_after, "gw_folds_total", {"tenant": "folded", "outcome": "committed"}),
                "fold_outcomes": fr.fold_outcome_deltas(metrics_before, metrics_after),
                "failed_checks": fr.metric_deltas(metrics_before, metrics_after, "gw_fold_checks_failed_total", "check"),
                "corrective_retries": fr.metric_deltas(metrics_before, metrics_after,
                                                       "gw_fold_corrective_retries_total", "result"),
                "summarizer_prompt_tokens": prompt_tokens(metrics_after) - prompt_tokens(metrics_before),
                "summarizer_completion_tokens": completion_tokens(metrics_after) - completion_tokens(metrics_before),
            })
            rows.append({
                "call": call,
                "order": "plain-first" if plain_first else "folded-first",
                "task": task,
                "plain": arm_row(plain, plain_rules),
                "folded": folded_row,
                "rules": rules,
            })
    except fr.BenchmarkError as exc:
        error = {"type": type(exc).__name__, "message": str(exc)}

    report: dict[str, Any] = {
        "schema": "folding.tasks.v1",
        "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "settings": {
            "model": model,
            "scenario": TaskScenario.name,
            "profile": profile,
            "max_calls": max_calls,
            "max_tokens": max_tokens,
            "temperature": 0,
            "fold_cap": profile_data["max_folds"],
            "max_success_drop": max_success_drop,
            "secrets_written": False,
            "git_commit": fr._git_commit(),
            "prices": prices or {},
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
    if not rows or agg["eligible_rules"] == 0:
        return report, 2
    if max_success_drop is None:
        regressed = agg["eligible_rules_broken"] > 0
    else:
        regressed = agg["success_drop"] is not None and agg["success_drop"] > max_success_drop
    return report, 1 if regressed else 0


def aggregates(rows: list[dict[str, Any]], profile_data: dict[str, Any], prices: dict[str, float | None],
               final_metrics: dict) -> dict[str, Any]:
    scored = [row for row in rows if row["rules"]]

    def rate(arm: str) -> float | None:
        return sum(row[arm]["success"] for row in scored) / len(scored) if scored else None

    compliance: dict[str, dict[str, Any]] = {}
    for rule in RULES:
        applied = [row["rules"][rule.name] for row in rows if rule.name in row["rules"]]
        compliance[rule.name] = {
            "calls": len(applied),
            "plain": sum(item["plain_pass"] for item in applied) / len(applied) if applied else None,
            "folded": sum(item["folded_pass"] for item in applied) / len(applied) if applied else None,
            "eligible": sum(item["eligible"] for item in applied),
            "eligible_broken": sum(item["eligible"] and not item["folded_pass"] for item in applied),
        }
    eligible = [item for row in rows for item in row["rules"].values() if item["eligible"]]
    plain_rate, folded_rate = rate("plain"), rate("folded")
    max_fold_count = max((row["folded"]["fold_count"] for row in rows), default=0)
    plain_spend = sum(row["plain"]["spend_delta_nano_usd"] for row in rows)
    folded_spend = sum(row["folded"]["spend_delta_nano_usd"] for row in rows)
    return {
        "calls_scored": len(scored),
        "success_rate": {"plain": plain_rate, "folded": folded_rate},
        "success_drop": (plain_rate - folded_rate) if scored else None,
        "compliance": compliance,
        "eligible_rules": len(eligible),
        "eligible_rules_broken": sum(not item["folded_pass"] for item in eligible),
        "total_spend_nano_usd": {"plain": plain_spend, "folded": folded_spend},
        "folds": {
            "committed": max_fold_count,
            "rejected": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "rejected"}),
            "unprofitable": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "unprofitable"}),
            "backoff": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "backoff"}),
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
    parser.add_argument("--scenario", choices=[TaskScenario.name], default=TaskScenario.name)
    parser.add_argument("--profile", choices=sorted(fr.PROFILES), default="lifecycle")
    parser.add_argument("--max-calls", type=int, default=120)
    parser.add_argument("--max-tokens", type=int, default=512)
    parser.add_argument("--timeout", type=float, default=240.0)
    parser.add_argument("--max-success-drop", type=float,
                        help="how far (0-1) the folded tenant's success rate may fall below the unfolded one's")
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
    output = (args.output or Path("benchmark_results") / f"fold_tasks_{args.profile}_{stamp}.json").resolve()
    try:
        return run_tasks(
            base_url=args.base_url, plain_key=args.plain_key, folded_key=args.folded_key, admin_key=args.admin_key,
            model=args.model, profile=args.profile, output=output, max_calls=args.max_calls,
            max_tokens=args.max_tokens, timeout=args.timeout, max_success_drop=args.max_success_drop,
            prices={
                "main_input_per_mtok_usd": args.main_input_per_mtok_usd,
                "main_output_per_mtok_usd": args.main_output_per_mtok_usd,
                "main_cached_input_per_mtok_usd": args.main_cached_input_per_mtok_usd,
                "summarizer_input_per_mtok_usd": args.summarizer_input_per_mtok_usd,
                "summarizer_output_per_mtok_usd": args.summarizer_output_per_mtok_usd,
            },
        )[1]
    except fr.BenchmarkError as exc:
        print(f"fold-tasks: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
