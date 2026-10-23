"""Paired factual-retention benchmark for the conversation-folding gateway.

The harness is intentionally standard-library-only. It sends the same request
history to a plain tenant and a folding tenant, reads spend from each tenant's
gateway ledger, and writes an artifact containing only synthetic facts and
aggregate measurements. API keys are accepted from arguments or environment
variables but are never printed or serialized.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener


class _NoRedirects(HTTPRedirectHandler):
    """Refuse every redirect. urllib's own handler re-sends a request's
    headers, `x-api-key` included, to wherever a 30x points, so a misconfigured
    or hostile `--base-url` could collect the key. A redirect becomes an
    `HTTPError` instead, reported like any other refusal."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401, ARG002
        return None


#: Every request goes through this opener, never `urlopen`.
_OPENER = build_opener(_NoRedirects)


SYSTEM = (
    "Answer factual questions about the conversation. Use the latest explicit "
    "correction for current facts. Keep facts about different environments separate. "
    "Return only the requested JSON object, with no explanation or Markdown. "
    "Use JSON null for information that has never been supplied. Do not guess."
)
QUESTION = (
    "Report the current facts as one JSON object with exactly these keys: "
    "project_id (string), owner (string), city (string), budget_usd (integer), "
    "external_upload_allowed (boolean), staging_port (integer), production_port "
    "(integer), release_date (YYYY-MM-DD string), recovery_phrase (string or null), "
    "approval_ticket (string or null)."
)
INITIAL = {
    "project_id": "CEDAR-731",
    "owner": "Mira",
    "city": "Oslo",
    "budget_usd": 2400,
    "external_upload_allowed": False,
    "staging_port": 8443,
    "production_port": 9443,
    "release_date": "2027-02-18",
    "recovery_phrase": None,
    "approval_ticket": None,
}

DEFAULT_FOLD_CAP = 5
DEFAULT_MAX_CALLS = 50
PROFILES = {
    "lifecycle": {
        "label": "lifecycle, not economics",
        "min_new_turns": 1,
        "min_new_tokens": 0,
        "max_folds": 5,
        "refold_threshold": 8,
    },
    "economics": {
        "label": "economics",
        "min_new_turns": 4,
        "min_new_tokens": 400,
        "max_folds": 8,
        "refold_threshold": 8,
    },
}


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def message_digest(messages: list[dict[str, str]]) -> str:
    return hashlib.sha256(canonical_json(messages)).hexdigest()


class Scenario:
    """Gold answers never enter questions or generated assistant history."""

    name = "fixed"

    def __init__(self) -> None:
        facts = [
            "The project identifier is CEDAR-731.",
            "Mira owns this project. The delivery city is Oslo.",
            "The spending ceiling is 2400 USD.",
            "External uploads are forbidden, including diagnostic logs.",
            "Staging listens on port 8443. Production listens on port 9443.",
            "The release date is 2027-02-18.",
        ]
        self.messages = [{"role": "user", "content": value} for value in facts]
        self.messages.extend(
            {
                "role": "user",
                "content": (
                    f"Workshop note {index:02d}: the team reviewed the packaging checklist, "
                    "discussed the order of verification steps, and filed the meeting notes. "
                    "This discussion introduced no changes to the project requirements."
                ),
            }
            for index in range(44)
        )
        self.expected = dict(INITIAL)
        self.introduced_at = dict(zip(INITIAL, (1, 2, 2, 3, 4, 5, 5, 6, 0, 0)))
        self.frozen_update_added = False

    def advance(self, call: int, folds: int, fold_cap: int) -> None:
        changes: dict[str, Any] = {}
        if folds >= fold_cap and not self.frozen_update_added:
            content = "New recovery phrase: opal-harbor. Record it for the release."
            changes = {"recovery_phrase": "opal-harbor"}
            self.frozen_update_added = True
        elif call == 3:
            content = "Budget correction: the ceiling is now 1800 USD; 2400 USD is obsolete."
            changes = {"budget_usd": 1800}
        elif call == 5:
            content = "Ownership and destination correction: Noor replaces Mira as owner. Deliver to Bergen instead of Oslo."
            changes = {"owner": "Noor", "city": "Bergen"}
        else:
            content = f"Progress note {call:02d}: packaging review continues; no requirements changed."
        self.messages.append({"role": "user", "content": content})
        self.expected.update(changes)
        self.introduced_at.update({key: len(self.messages) for key in changes})

    def request(self, model: str, max_tokens: int) -> dict[str, Any]:
        return {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM},
                *deepcopy(self.messages),
                {"role": "user", "content": QUESTION},
            ],
            "temperature": 0,
            "max_tokens": max_tokens,
            "stream": False,
        }


MUTATIONS: list[tuple[int, dict[str, Any], str]] = [
    (3, {"budget_usd": 1800}, "Budget correction: the ceiling is now 1800 USD; 2400 USD is void."),
    (6, {"owner": "Noor", "city": "Bergen"}, "Ownership and destination correction: Noor replaces Mira, and delivery moves to Bergen."),
    (11, {"staging_port": 8444}, "Correction: staging now listens on 8444, not 8443."),
    (17, {"external_upload_allowed": True}, "Correction: external uploads are now permitted, including diagnostic logs."),
    (23, {"release_date": "2027-03-04"}, "Correction: release slipped to 2027-03-04."),
    (31, {"budget_usd": 1250}, "Budget correction again: the ceiling is now 1250 USD; 1800 USD is void."),
    (39, {"city": "Tromso"}, "Correction: delivery moves to Tromso, superseding Bergen."),
    (45, {"production_port": 9445}, "Correction: production now listens on 9445."),
]
SEED_MUTATIONS: list[tuple[int, dict[str, Any], str]] = [
    (10, {"budget_usd": 1800}, "Budget correction: the ceiling is now 1800 USD; 2400 USD is void."),
    (20, {"owner": "Noor", "city": "Bergen"}, "Ownership and destination correction: Noor replaces Mira, and delivery moves to Bergen."),
    (30, {"staging_port": 8444}, "Correction: staging now listens on 8444, not 8443."),
]
LATE_MUTATIONS: list[tuple[int, dict[str, Any], str]] = [
    (8, {"external_upload_allowed": True}, "Correction: external uploads are now permitted, including diagnostic logs."),
    (16, {"release_date": "2027-03-04"}, "Correction: release slipped to 2027-03-04."),
    (24, {"budget_usd": 1250}, "Budget correction again: the ceiling is now 1250 USD; 1800 USD is void."),
    (32, {"city": "Tromso"}, "Correction: delivery moves to Tromso, superseding Bergen."),
    (40, {"production_port": 9445}, "Correction: production now listens on 9445."),
]
DISTRACTOR: dict[int, str] = {
    12: "Unrelated: project BIRCH-204 is budgeted at 7200 USD and owned by Ana.",
    20: "Reminder, BIRCH-204 stays at 7200 USD with Ana as owner; that is a different project.",
    33: "BIRCH-204 note: do not merge its 7200 USD budget with the delivery project figures.",
    44: "Last BIRCH-204 mention: 7200 USD, owner Ana, unrelated to this delivery.",
}
CAP_BOUNDARY_MUTATION = {"owner": "Ines"}


class MutatingScenario(Scenario):
    name = "mutating"

    def __init__(self) -> None:
        blocks = [
            "The project identifier is CEDAR-731.",
            "Mira owns this project. The delivery city is Oslo.",
            "The spending ceiling is 2400 USD.",
            "External uploads are forbidden, including diagnostic logs.",
            "Staging listens on port 8443. Production listens on port 9443.",
            "The release date is 2027-02-18.",
        ]
        self.expected = dict(INITIAL)
        self.introduced_at = dict(zip(INITIAL, (1, 2, 2, 3, 4, 5, 5, 6, 0, 0)))
        for turn in range(44):
            blocks.append(
                f"Workshop note {turn:02d}: the team reviewed the packaging checklist, "
                "discussed the order of verification steps, and filed the meeting notes. "
                "This discussion introduced no changes to the project requirements."
            )
            for at, change, text in SEED_MUTATIONS:
                if at == turn:
                    blocks.append(text)
                    self.expected.update(change)
                    self.introduced_at.update({key: len(blocks) - 1 for key in change})
        self.messages = [{"role": "user", "content": value} for value in blocks]
        self._frozen_update_added = False

    def advance(self, call: int, folds: int, fold_cap: int) -> None:
        changes: dict[str, Any] = {}
        if folds >= fold_cap and not self._frozen_update_added:
            key, value = next(iter(CAP_BOUNDARY_MUTATION.items()))
            content = f"Correction: {key} is now {value}, superseding the earlier value."
            changes = {key: value}
            self._frozen_update_added = True
        elif call in DISTRACTOR:
            content = DISTRACTOR[call]
        else:
            content = f"Progress note {call:02d}: packaging review continues; no requirements changed."
            for at, change, text in LATE_MUTATIONS:
                if call == at:
                    content, changes = text, dict(change)
                    break
        self.messages.append({"role": "user", "content": content})
        self.expected.update(changes)
        self.introduced_at.update({key: len(self.messages) for key in changes})


SCENARIOS = {"fixed": Scenario, "mutating": MutatingScenario}
if sorted(set(DISTRACTOR) & {call for call, _change, _text in LATE_MUTATIONS}):
    raise RuntimeError("mutating scenario has a distractor/correction overlap")


def score_answer(content: Any, expected: dict[str, Any], finish_reason: str | None) -> dict[str, Any]:
    """Score a JSON answer with strict types and explicit containment results."""
    dash_variants = str.maketrans({
        "\u2010": "-", "\u2011": "-", "\u2012": "-", "\u2013": "-",
        "\u2014": "-", "\u2212": "-", "\ufe58": "-", "\ufe63": "-", "\uff0d": "-",
    })

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ValueError(f"invalid JSON constant: {value}")

    parsed: Any = None
    error: str | None = None
    try:
        if not isinstance(content, str):
            raise ValueError("missing text content")
        parsed = json.loads(content, object_pairs_hook=unique_object, parse_constant=reject_constant)
        if not isinstance(parsed, dict):
            raise ValueError("answer must be a JSON object")
        if finish_reason != "stop":
            raise ValueError(f"answer did not finish normally: {finish_reason}")
    except (ValueError, TypeError) as exc:
        error = str(exc)

    normalized = {
        key: value.translate(dash_variants) if isinstance(value, str) else value
        for key, value in parsed.items()
    } if isinstance(parsed, dict) else parsed
    fields: dict[str, bool] = {}
    containment: dict[str, bool] = {}
    for key, expected_value in expected.items():
        actual = normalized.get(key) if isinstance(normalized, dict) else None
        exact = error is None and key in normalized and type(actual) is type(expected_value) and actual == expected_value
        contains = exact or (
            error is None and isinstance(expected_value, str) and isinstance(actual, str)
            and expected_value in actual
        )
        fields[key] = exact
        containment[key] = contains
    extra_keys = sorted(set(parsed) - set(expected)) if isinstance(parsed, dict) else []
    return {
        "correct": sum(fields.values()),
        "total": len(expected),
        "fields": fields,
        "correct_containment": sum(containment.values()),
        "fields_containment": containment,
        "all_correct": bool(error is None and all(fields.values()) and not extra_keys),
        "all_correct_containment": bool(error is None and all(containment.values()) and not extra_keys),
        "error": error,
        "extra_keys": extra_keys,
        "parsed": parsed,
    }


def retention_eligible(introduced_at: int, folded_turns: int, plain_correct: bool) -> bool:
    return bool(plain_correct and introduced_at > 0 and folded_turns >= introduced_at)


def fixture_digest(scenario: Scenario, calls: int = DEFAULT_MAX_CALLS, fold_cap: int = DEFAULT_FOLD_CAP) -> list[dict[str, Any]]:
    rows = []
    for call in range(1, calls + 1):
        folds = min((call - 1) // 8, fold_cap)
        scenario.advance(call, folds, fold_cap)
        rows.append({
            "call": call,
            "folds_before_call": folds,
            "messages_sha256": message_digest(scenario.request("fixture/model", 1024)["messages"]),
            "expected": deepcopy(scenario.expected),
            "introduced_at": deepcopy(scenario.introduced_at),
        })
    return rows


class BenchmarkError(RuntimeError):
    pass


class TransientError(BenchmarkError):
    """A refusal worth retrying: the vendor's rate limit (sent on as 429 or
    502) or a 503. The gateway refunds a request refused before streaming, so
    a retried call is billed once."""


#: Statuses retried, and the waits between attempts. Groq's on-demand tier
#: allows 8,000 tokens a minute on gpt-oss-120b, about one call pair, so the
#: waits add up to a little over a minute's window several times over.
TRANSIENT_STATUSES = {429, 502, 503}
RETRY_WAITS_SECONDS = (10, 20, 30, 45, 60, 60, 60, 60)


def _url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + path


def _request(base_url: str, path: str, key: str, body: dict[str, Any] | None, timeout: float) -> tuple[int, dict[str, str], Any]:
    data = None if body is None else canonical_json(body)
    request = Request(_url(base_url, path), data=data, method="GET" if body is None else "POST")
    request.add_header("x-api-key", key)
    if body is not None:
        request.add_header("content-type", "application/json")
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            text = response.read().decode("utf-8")
            try:
                payload: Any = json.loads(text)
            except json.JSONDecodeError:
                payload = text
            return response.status, {key.lower(): value for key, value in response.headers.items()}, payload
    except HTTPError as exc:
        if exc.code in TRANSIENT_STATUSES:
            raise TransientError(f"HTTP {exc.code} from {path}") from None
        raise BenchmarkError(f"HTTP {exc.code} from {path}") from None
    except (URLError, TimeoutError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BenchmarkError(f"request to {path} failed: {type(exc).__name__}") from None


def _usage(base_url: str, key: str, timeout: float) -> int:
    status, _headers, payload = _request(base_url, "/v1/usage", key, None, timeout)
    if status != 200 or not isinstance(payload, dict) or not isinstance(payload.get("spent_nano_usd"), int):
        raise BenchmarkError("usage response did not contain spent_nano_usd")
    return payload["spent_nano_usd"]


_METRIC_RE = re.compile(r"^(?P<name>[A-Za-z_:][A-Za-z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>[-+0-9.eE]+)$")


def _metrics(base_url: str, key: str, timeout: float) -> dict[tuple[str, tuple[tuple[str, str], ...]], int]:
    status, _headers, payload = _request(base_url, "/metrics", key, None, timeout)
    if status != 200 or not isinstance(payload, str):
        raise BenchmarkError("metrics response was not text")
    result: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}
    for line in payload.splitlines():
        match = _METRIC_RE.match(line)
        if not match:
            continue
        labels: list[tuple[str, str]] = []
        raw_labels = match.group("labels") or ""
        for part in raw_labels.split(",") if raw_labels else []:
            name, separator, value = part.partition("=")
            if separator:
                labels.append((name, value.strip('"')))
        try:
            value = int(float(match.group("value")))
        except ValueError:
            continue
        result[(match.group("name"), tuple(sorted(labels)))] = value
    return result


def _metric(metrics: dict, name: str, labels: dict[str, str]) -> int:
    return metrics.get((name, tuple(sorted(labels.items()))), 0)


def _answer(payload: Any) -> tuple[Any, str | None]:
    if not isinstance(payload, dict):
        return None, None
    choices = payload.get("choices")
    first = choices[0] if isinstance(choices, list) and choices else {}
    if not isinstance(first, dict):
        return None, None
    message = first.get("message")
    if not isinstance(message, dict):
        return None, first.get("finish_reason")
    return message.get("content"), first.get("finish_reason")


def _git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    value = result.stdout.strip()
    return value or None


def _arm(base_url: str, tenant_key: str, body: dict[str, Any], timeout: float, sleep=time.sleep) -> dict[str, Any]:
    before = _usage(base_url, tenant_key, timeout)
    retries = 0
    while True:
        try:
            status, headers, payload = _request(base_url, "/v1/chat/completions", tenant_key, deepcopy(body), timeout)
            break
        except TransientError:
            if retries == len(RETRY_WAITS_SECONDS):
                raise
            sleep(RETRY_WAITS_SECONDS[retries])
            retries += 1
    after = _usage(base_url, tenant_key, timeout)
    if status != 200:
        raise BenchmarkError(f"chat request returned HTTP {status}")
    content, finish_reason = _answer(payload)
    folded_header = headers.get("x-gateway-folded")
    try:
        folded_turns = int(folded_header) if folded_header is not None else 0
    except ValueError:
        raise BenchmarkError("x-gateway-folded was not an integer") from None
    return {
        "finish_reason": finish_reason,
        "spend_delta_nano_usd": after - before,
        "folded_turns": folded_turns,
        "retries": retries,
        "content": content,
        "payload": payload,
    }


def run_benchmark(
    *,
    base_url: str,
    plain_key: str,
    folded_key: str,
    admin_key: str,
    model: str,
    scenario_name: str,
    profile: str,
    output: Path,
    max_calls: int = DEFAULT_MAX_CALLS,
    max_tokens: int = 1024,
    timeout: float = 240.0,
    prices: dict[str, float | None] | None = None,
) -> tuple[dict[str, Any], int]:
    if scenario_name not in SCENARIOS:
        raise BenchmarkError(f"unknown scenario {scenario_name!r}")
    if profile not in PROFILES:
        raise BenchmarkError(f"unknown profile {profile!r}")
    if max_calls <= 0 or max_tokens <= 0 or timeout <= 0:
        raise BenchmarkError("max_calls, max_tokens and timeout must be positive")
    scenario = SCENARIOS[scenario_name]()
    profile_data = PROFILES[profile]
    started = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    try:
        for call in range(1, max_calls + 1):
            metrics_before = _metrics(base_url, admin_key, timeout)
            committed_before = _metric(metrics_before, "gw_folds_total", {"tenant": "folded", "outcome": "committed"})
            scenario.advance(call, committed_before, profile_data["max_folds"])
            body = scenario.request(model, max_tokens)
            plain = _arm(base_url, plain_key, deepcopy(body), timeout)
            folded = _arm(base_url, folded_key, deepcopy(body), timeout)
            plain_score = score_answer(plain["content"], scenario.expected, plain["finish_reason"])
            folded_score = score_answer(folded["content"], scenario.expected, folded["finish_reason"])
            fields = {
                key: {
                    "expected": value,
                    "introduced_at": scenario.introduced_at.get(key, 0),
                    "eligible": retention_eligible(
                        scenario.introduced_at.get(key, 0), folded["folded_turns"], plain_score["fields"].get(key, False)
                    ),
                    "plain_correct": plain_score["fields"].get(key, False),
                    "folded_correct": folded_score["fields"].get(key, False),
                    "plain_containment_correct": plain_score["fields_containment"].get(key, False),
                    "folded_containment_correct": folded_score["fields_containment"].get(key, False),
                }
                for key, value in scenario.expected.items()
            }
            metrics_after = _metrics(base_url, admin_key, timeout)
            row = {
                "call": call,
                "plain": {
                    "correct": plain_score["correct"],
                    "total": plain_score["total"],
                    "containment_correct": plain_score["correct_containment"],
                    "finish_reason": plain["finish_reason"],
                    "spend_delta_nano_usd": plain["spend_delta_nano_usd"],
                    "retries": plain["retries"],
                    "error": plain_score["error"],
                    "extra_keys": plain_score["extra_keys"],
                },
                "folded": {
                    "correct": folded_score["correct"],
                    "total": folded_score["total"],
                    "containment_correct": folded_score["correct_containment"],
                    "finish_reason": folded["finish_reason"],
                    "spend_delta_nano_usd": folded["spend_delta_nano_usd"],
                    "folded_turns": folded["folded_turns"],
                    "retries": folded["retries"],
                    "fold_count": _metric(metrics_after, "gw_folds_total", {"tenant": "folded", "outcome": "committed"}),
                    "summarizer_prompt_tokens": _metric(metrics_after, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "prompt"})
                    - _metric(metrics_before, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "prompt"}),
                    "summarizer_completion_tokens": _metric(metrics_after, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "completion"})
                    - _metric(metrics_before, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "completion"}),
                    "error": folded_score["error"],
                    "extra_keys": folded_score["extra_keys"],
                },
                "fields": fields,
            }
            rows.append(row)
    except BenchmarkError as exc:
        report = {
            "schema": "folding.retention.v1",
            "started_utc": started,
            "finished_utc": datetime.now(timezone.utc).isoformat(),
            "settings": {
                "model": model,
                "scenario": scenario_name,
                "profile": profile,
                "label": profile_data["label"],
                "git_commit": _git_commit(),
                "prices": prices or {},
            },
            "rows": rows,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report, 2

    eligible = [field for row in rows for field in row["fields"].values() if field["eligible"]]
    plain_fields = [field for row in rows for field in row["fields"].values()]
    folded_correct = sum(field["folded_correct"] for field in eligible)
    plain_correct = sum(field["plain_correct"] for field in plain_fields)
    total = len(plain_fields)
    plain_spend = sum(row["plain"]["spend_delta_nano_usd"] for row in rows)
    folded_spend = sum(row["folded"]["spend_delta_nano_usd"] for row in rows)
    max_fold_count = max((row["folded"]["fold_count"] for row in rows), default=0)
    reached = "re-folded" if max_fold_count > profile_data["max_folds"] else (
        "frozen" if max_fold_count >= profile_data["max_folds"] else "warm"
    )
    final_metrics = _metrics(base_url, admin_key, timeout)
    report = {
        "schema": "folding.retention.v1",
        "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "settings": {
            "model": model,
            "scenario": scenario_name,
            "profile": profile,
            "label": profile_data["label"],
            "max_calls": max_calls,
            "max_tokens": max_tokens,
            "temperature": 0,
            "fold_cap": profile_data["max_folds"],
            "refold_threshold": profile_data["refold_threshold"],
            "secrets_written": False,
            "git_commit": _git_commit(),
            "prices": prices or {},
        },
        "calls": rows,
        "aggregates": {
            "retention_rate": folded_correct / len(eligible) if eligible else None,
            "eligible_fields": len(eligible),
            "folded_correct_eligible_fields": folded_correct,
            "accuracy": {
                "plain": plain_correct / total if total else None,
                "folded": sum(field["folded_correct"] for field in plain_fields) / total if total else None,
            },
            "total_spend_nano_usd": {"plain": plain_spend, "folded": folded_spend},
            "net_saving_nano_usd": plain_spend - folded_spend,
            "folds": {
                "committed": max((row["folded"]["fold_count"] for row in rows), default=0),
                "rejected": _metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "rejected"}),
                "failed": _metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "failed"}),
                "unprofitable": _metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "unprofitable"}),
                "refused_admission": _metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "refused_admission"}),
            },
            "reached": reached,
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    regressed = any(field["eligible"] and field["plain_correct"] and not field["folded_correct"] for row in rows for field in row["fields"].values())
    incomplete = not rows or not eligible
    return report, 2 if incomplete else (1 if regressed else 0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base-url", default=os.environ.get("LLM_GATEWAY_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--plain-key", default=os.environ.get("LLM_GATEWAY_PLAIN_KEY"))
    parser.add_argument("--folded-key", default=os.environ.get("LLM_GATEWAY_FOLDED_KEY"))
    parser.add_argument("--admin-key", default=os.environ.get("LLM_GATEWAY_ADMIN_KEY"))
    parser.add_argument("--model", required=True)
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="fixed")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="economics")
    parser.add_argument("--max-calls", type=int, default=DEFAULT_MAX_CALLS)
    parser.add_argument("--max-tokens", type=int, default=1024)
    parser.add_argument("--timeout", type=float, default=240.0)
    parser.add_argument("--main-input-per-mtok-usd", type=float)
    parser.add_argument("--main-output-per-mtok-usd", type=float)
    parser.add_argument("--summarizer-input-per-mtok-usd", type=float)
    parser.add_argument("--summarizer-output-per-mtok-usd", type=float)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    missing = [name for name, value in (("--plain-key", args.plain_key), ("--folded-key", args.folded_key), ("--admin-key", args.admin_key)) if not value]
    if missing:
        parser.error(f"missing secret references: {', '.join(missing)}")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output or Path("benchmark_results") / f"fold_retention_{args.scenario}_{args.profile}_{stamp}.json").resolve()
    try:
        return run_benchmark(
            base_url=args.base_url,
            plain_key=args.plain_key,
            folded_key=args.folded_key,
            admin_key=args.admin_key,
            model=args.model,
            scenario_name=args.scenario,
            profile=args.profile,
            output=output,
            max_calls=args.max_calls,
            max_tokens=args.max_tokens,
            timeout=args.timeout,
            prices={
                "main_input_per_mtok_usd": args.main_input_per_mtok_usd,
                "main_output_per_mtok_usd": args.main_output_per_mtok_usd,
                "summarizer_input_per_mtok_usd": args.summarizer_input_per_mtok_usd,
                "summarizer_output_per_mtok_usd": args.summarizer_output_per_mtok_usd,
            },
        )[1]
    except BenchmarkError as exc:
        print(f"fold-retention: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
