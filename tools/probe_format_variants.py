"""A/B probe: does the summary's format change answer format, not its facts?

Folded misses are the right value in the wrong format ("39" or a sentence
where strict scoring wants the JSON number 39). This probe replays captured
folded requests — the exact bodies the gateway forwarded to OpenRouter — and
changes ONLY the summary text inside message [1]: the preamble and every
other byte stay identical.

Variants: A control (unchanged), B (drop Context sentences that restate a
numeric tool fact as a standalone token), C (each call's JSON facts rendered
as one compact JSON object), D (B and C together). C's known limit: the
ledger stores no type, so a number-looking string ("12.81") emits as a
number.

Pre-registered decision bar (written before the live run): over the 5 miss
bodies x repeats tries per variant, V wins iff
  1. strict(V) >= strict(A) + 6 on misses;
  2. lenient(V) >= lenient(A) on misses;
  3. on controls, strict(V) >= strict(A) - 2 and lenient(V) >= lenient(A).
Among winners the highest miss-strict count wins; ties go to the simpler
variant in the order B < C < D. No winner means "no code change".

The key is read from OPENROUTER_API_KEY and never printed, logged or
written. Scoring reuses fold_tools.parse_answer, strict_match and
lenient_match unchanged. Source: PR 72 captures (branch
cursor/fold-forward-capture-3c87 @ 00e36e0), not committed to the repo.
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
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request

import fold_retention as fr
import fold_tools

SCHEMA = "folding.probe_format.v1"
ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"
REPEATS = 5
COST_LIMIT_USD = 0.30
SOURCE_BRANCH = "cursor/fold-forward-capture-3c87"
SOURCE_COMMIT = "00e36e0"

DEFAULT_CAPTURE_DIR = Path("/tmp/cap")
FILE_144858Z = "fold_tools_openrouter_tools_lifecycle_20261027T144858Z_sum-gpt-4.1-mini"
FILE_145527Z = "fold_tools_openrouter_tools_lifecycle_20261027T145527Z_sum-gpt-4.1-mini"

#: (result file stem, call) — the captured format-only misses.
MISSES = [
    (FILE_144858Z, 26),
    (FILE_145527Z, 26),
    (FILE_145527Z, 101),
    (FILE_145527Z, 107),
    (FILE_145527Z, 149),
]

VARIANTS = ("A", "B", "C", "D")
NUMERIC = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?")
FACT_LINE = re.compile(r"^  - (/[^:]*): (.*)$")
TEXT_FACT_LINE = re.compile(r'^  - "(.*)"$')


class ProbeError(Exception):
    pass


def is_numeric(value: str) -> bool:
    return NUMERIC.fullmatch(value) is not None


def split_summary(summary: str) -> tuple[str, str | None]:
    """(summary before the Context section, context text or None)."""
    index = summary.find("\n\nContext:\n")
    if index < 0:
        return summary, None
    return summary[:index], summary[index + len("\n\nContext:\n") :]


def tool_block_lines(summary: str) -> list[str]:
    """The lines of the tool block: from 'Tool results folded away' to the
    next blank line."""
    lines = summary.split("\n")
    for i, line in enumerate(lines):
        if line.startswith("Tool results folded away"):
            block = [line]
            for rest in lines[i + 1 :]:
                if rest == "":
                    break
                block.append(rest)
            return block
    return []


def json_facts(block: list[str]) -> list[tuple[str, str]]:
    return [(m.group(1), m.group(2)) for line in block if (m := FACT_LINE.match(line))]


def _token_match(text: str, value: str) -> bool:
    """value occurs as a standalone token: no alphanumeric or '.' before, no
    alphanumeric or '.<digit>' after."""
    start = 0
    while True:
        i = text.find(value, start)
        if i < 0:
            return False
        before = i == 0 or (not text[i - 1].isalnum() and text[i - 1] != ".")
        j = i + len(value)
        after = j == len(text) or (
            not text[j].isalnum()
            and not (text[j] == "." and j + 1 < len(text) and text[j + 1].isdigit())
        )
        if before and after:
            return True
        start = i + 1


def _sentences(context: str) -> list[str]:
    """Maximal segments ending at ., ! or ? followed by whitespace, or at
    end of text (the mark stays on the sentence)."""
    parts = re.split(r"(?<=[.!?])\s+", context.strip())
    return [part for part in parts if part]


def variant_b(summary: str) -> str:
    """Drop Context sentences that contain a numeric tool-fact value as a
    standalone token; drop the Context section if nothing remains."""
    head, context = split_summary(summary)
    if context is None:
        return summary
    values = {value for _pointer, value in json_facts(tool_block_lines(summary)) if is_numeric(value)}
    kept = [s for s in _sentences(context) if not any(_token_match(s, v) for v in values)]
    kept = [s.strip() for s in kept if s.strip()]
    return head if not kept else f"{head}\n\nContext:\n{' '.join(kept)}"


def _build_tree(facts: list[tuple[str, str]]) -> dict | None:
    """Pointer paths -> nested dict, first-seen order; None on conflict."""
    root: dict = {}
    for pointer, value in facts:
        keys = [k.replace("~1", "/").replace("~0", "~") for k in pointer.split("/")[1:]]
        if not keys:
            return None
        node = root
        for key in keys[:-1]:
            child = node.setdefault(key, {})
            if not isinstance(child, dict):
                return None
            node = child
        if keys[-1] in node:
            return None
        node[keys[-1]] = value
    return root


def _to_json_tree(node: Any) -> Any:
    """A dict whose keys are exactly '0'..'n-1' becomes a list in index order."""
    if not isinstance(node, dict):
        return node
    keys = list(node.keys())
    if sorted(keys) == [str(i) for i in range(len(keys))]:
        return [_to_json_tree(node[str(i)]) for i in range(len(keys))]
    return {key: _to_json_tree(value) for key, value in node.items()}


def _emit(node: Any) -> str:
    if isinstance(node, dict):
        return "{" + ",".join(json.dumps(key, ensure_ascii=False) + ":" + _emit(value) for key, value in node.items()) + "}"
    if isinstance(node, list):
        return "[" + ",".join(_emit(item) for item in node) + "]"
    value = node
    if is_numeric(value) or value in ("true", "false", "null"):
        return value
    return json.dumps(value, ensure_ascii=False)


def facts_object(facts: list[tuple[str, str]]) -> str | None:
    """The compact JSON object for one call's facts, or None on conflict."""
    tree = _build_tree(facts)
    if tree is None:
        return None
    return _emit(_to_json_tree(tree))


def variant_c(summary: str) -> str:
    """Each call's consecutive JSON-fact lines collapse into one compact
    JSON object line. Text facts and 'not kept' lines stay in place."""
    out = []
    run: list[tuple[str, str]] = []

    def flush() -> None:
        nonlocal run
        if not run:
            return
        rendered = facts_object(run)
        if rendered is not None:
            out.append(f"  {rendered}")
        else:
            for pointer, value in run:
                out.append(f"  - {pointer}: {value}")
        run = []

    for line in summary.split("\n"):
        if m := FACT_LINE.match(line):
            run.append((m.group(1), m.group(2)))
        else:
            flush()
            out.append(line)
    flush()
    return "\n".join(out)


def variant(summary: str, name: str) -> str:
    if name == "A":
        return summary
    if name == "B":
        return variant_b(summary)
    if name == "C":
        return variant_c(summary)
    if name == "D":
        return variant_c(variant_b(summary))
    raise ValueError(name)


def summary_of(body: dict[str, Any]) -> tuple[str, str]:
    """(preamble line incl. newline, summary text) from message [1]."""
    content = body["messages"][1]["content"]
    index = content.index("\n")
    return content[: index + 1], content[index + 1 :]


def with_summary(body: dict[str, Any], text: str) -> dict[str, Any]:
    new = deepcopy(body)
    prefix, _old = summary_of(new)
    new["messages"][1]["content"] = prefix + text
    return new


def load_bodies(capture_dir: Path) -> list[dict[str, Any]]:
    bodies = []
    skipped = []
    for stem, call in MISSES:
        body = _body_at(capture_dir, stem, call)
        bodies.append({"role": "miss", "file": stem, "call": call, **body})
    # Controls: the last eligible-and-correct call for each question text.
    data = json.loads((capture_dir / f"{FILE_145527Z}.json").read_text(encoding="utf-8"))
    last: dict[str, Any] = {}
    for row in data["calls"]:
        if row["eligible"] and row["folded"]["correct"]:
            last[row["question"]] = row
    for text in {row["question"] for row in data["calls"]}:
        row = last.get(text)
        if row is None:
            skipped.append(text)
            continue
        body = _row_body(capture_dir, FILE_145527Z, row)
        bodies.append({"role": "control", "file": FILE_145527Z, "call": row["call"], **body})
    return bodies, skipped


def _row_body(capture_dir: Path, stem: str, row: dict) -> dict[str, Any]:
    data = json.loads((capture_dir / f"{stem}.json").read_text(encoding="utf-8"))
    side = {}
    for line in (capture_dir / f"{stem}.forwarded.jsonl").read_text(encoding="utf-8").splitlines():
        entry = json.loads(line)
        side[entry["request_id"]] = entry
    request_id = row["folded"]["request_id"]
    if request_id not in side:
        raise ProbeError(f"{stem} call {row['call']}: request {request_id} not in the jsonl")
    return {"request_id": request_id, "gold": row["gold"], "question": row["question"], "body": side[request_id]["body"]}


def _body_at(capture_dir: Path, stem: str, call: int) -> dict[str, Any]:
    data = json.loads((capture_dir / f"{stem}.json").read_text(encoding="utf-8"))
    row = next((r for r in data["calls"] if r["call"] == call), None)
    if row is None:
        raise ProbeError(f"{stem} has no call {call}")
    return _row_body(capture_dir, stem, row)


def chat_completion(key: str, body: dict[str, Any], timeout: float = 240.0) -> dict[str, Any]:
    request = Request(ENDPOINT, data=json.dumps(body).encode(), method="POST")
    request.add_header("authorization", f"Bearer {key}")
    request.add_header("content-type", "application/json")
    retries = 0
    while True:
        try:
            with fr._OPENER.open(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code in fr.TRANSIENT_STATUSES and retries < len(fr.RETRY_WAITS_SECONDS):
                import time

                time.sleep(fr.RETRY_WAITS_SECONDS[retries])
                retries += 1
                continue
            raise ProbeError(f"OpenRouter returned HTTP {exc.code}") from None
        except (URLError, TimeoutError, OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ProbeError(f"OpenRouter request failed: {type(exc).__name__}") from None
        if not isinstance(payload, dict) or not payload.get("choices"):
            raise ProbeError("OpenRouter response had no choices")
        return payload


def categorize(payload: dict[str, Any], gold: Any) -> dict[str, Any]:
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


def evaluate_bar(miss: dict[str, dict[str, int]], control: dict[str, dict[str, int]]) -> dict[str, Any]:
    """miss/control: variant -> {strict, lenient, format_only}. Pre-registered
    bar: win needs +6 miss-strict, miss-lenient not worse, controls within -2
    strict and not worse lenient; ties go to the simpler variant B<C<D."""
    a = miss["A"]
    result = {"criteria": {}, "winners": [], "winner": None}
    for name in "BCD":
        v, c = miss[name], control[name]
        checks = {
            "miss_strict_plus6": v["strict"] >= a["strict"] + 6,
            "miss_lenient_not_worse": v["lenient"] >= a["lenient"],
            "control_strict_within_2": c["strict"] >= control["A"]["strict"] - 2,
            "control_lenient_not_worse": c["lenient"] >= control["A"]["lenient"],
        }
        result["criteria"][name] = checks
        if all(checks.values()):
            result["winners"].append(name)
    if result["winners"]:
        best = max(result["winners"], key=lambda name: miss[name]["strict"])
        tied = [name for name in result["winners"] if miss[name]["strict"] == miss[best]["strict"]]
        result["winner"] = min(tied, key="BCD".index)
    return result


def count(rows: list[dict[str, Any]], category: str) -> int:
    return sum(row["category"] == category for row in rows)


def variant_counts(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    return {
        name: {
            "strict": count(sub, "strict"),
            "lenient": count(sub, "strict") + count(sub, "format_only"),
            "format_only": count(sub, "format_only"),
        }
        for name in VARIANTS
        for sub in [[r for r in rows if r["variant"] == name]]
    }


def run(key: str, capture_dir: Path, output: Path, repeats: int, cost_limit: float) -> dict[str, Any]:
    bodies, skipped = load_bodies(capture_dir)
    misses = [b for b in bodies if b["role"] == "miss"]
    controls = [b for b in bodies if b["role"] == "control"]
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "settings": {
            "model": "openai/gpt-4.1-mini",
            "repeats": repeats,
            "cost_limit_usd": cost_limit,
            "source_branch": SOURCE_BRANCH,
            "source_commit": SOURCE_COMMIT,
            "source_files": sorted({b["file"] for b in bodies}),
            "controls_skipped_no_eligible_correct": skipped,
            "secrets_written": False,
            "git_commit": fr._git_commit(),
        },
        "bodies": [],
        "rows": [],
        "stopped_early": None,
    }
    for entry in bodies:
        _prefix, text = summary_of(entry["body"])
        texts = {name: variant(text, name) for name in VARIANTS}
        assert texts["A"] == text
        entry["summaries"] = texts
        report["bodies"].append(
            {
                "file": entry["file"],
                "call": entry["call"],
                "request_id": entry["request_id"],
                "body_sha256": hashlib.sha256(fr.canonical_json(entry["body"])).hexdigest(),
                "gold": entry["gold"],
                "question": entry["question"],
                "role": entry["role"],
                "summaries": texts,
            }
        )

    spent = 0.0
    try:
        for entry in bodies:
            for repeat in range(repeats):
                order = VARIANTS[repeat % 4 :] + VARIANTS[: repeat % 4]
                for name in order:
                    if spent > cost_limit:
                        report["stopped_early"] = f"cost {spent:.4f} USD passed the {cost_limit} limit"
                        raise _CostLimit
                    body = with_summary(entry["body"], entry["summaries"][name])
                    body["usage"] = {"include": True}
                    payload = chat_completion(key, body)
                    cost = (payload.get("usage") or {}).get("cost") or 0
                    spent += cost
                    report["rows"].append(
                        {
                            "file": entry["file"],
                            "call": entry["call"],
                            "role": entry["role"],
                            "variant": name,
                            "repeat": repeat,
                            "question": entry["question"],
                            "gold": entry["gold"],
                            **categorize(payload, entry["gold"]),
                            "usage": payload.get("usage"),
                            "cost_usd": cost,
                        }
                    )
    except _CostLimit:
        pass

    rows = report["rows"]
    miss_counts = variant_counts([r for r in rows if r["role"] == "miss"])
    ctrl_counts = variant_counts([r for r in rows if r["role"] == "control"])
    per_body = {
        f"{entry['file'][-20:]}:{entry['call']}:{name}": {
            "strict": count([r for r in rows if r["call"] == entry["call"] and r["file"] == entry["file"] and r["variant"] == name], "strict"),
            "lenient": count([r for r in rows if r["call"] == entry["call"] and r["file"] == entry["file"] and r["variant"] == name], "strict")
            + count([r for r in rows if r["call"] == entry["call"] and r["file"] == entry["file"] and r["variant"] == name], "format_only"),
        }
        for entry in bodies
        for name in VARIANTS
    }
    report["finished_utc"] = datetime.now(timezone.utc).isoformat()
    report["total_cost_usd"] = round(spent, 6)
    report["aggregates"] = {"misses": miss_counts, "controls": ctrl_counts, "per_body": per_body}
    report["bar"] = evaluate_bar(miss_counts, ctrl_counts)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


class _CostLimit(Exception):
    pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--capture-dir", type=Path, default=DEFAULT_CAPTURE_DIR)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--repeats", type=int, default=REPEATS)
    parser.add_argument("--cost-limit", type=float, default=COST_LIMIT_USD)
    args = parser.parse_args(argv)

    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        print("probe: OPENROUTER_API_KEY is not set", file=sys.stderr)
        return 2
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = args.output or Path("benchmark_results") / f"probe_format_variants_{stamp}.json"
    try:
        report = run(key, args.capture_dir.resolve(), output.resolve(), args.repeats, args.cost_limit)
    except ProbeError as exc:
        print(f"probe: {exc}", file=sys.stderr)
        return 2

    print(f"{'role':<8} {'variant':<7} {'strict':>6} {'lenient':>7} {'format_only':>11}")
    for role in ("misses", "controls"):
        for name, counts in report["aggregates"][role].items():
            print(f"{role:<8} {name:<7} {counts['strict']:>6} {counts['lenient']:>7} {counts['format_only']:>11}")
    bar = report["bar"]
    print(f"bar winners: {bar['winners']} -> {bar['winner'] or 'no code change'}")
    print(f"total cost: ${report['total_cost_usd']:.4f}  -> {output}")
    if report["stopped_early"]:
        print(f"stopped early: {report['stopped_early']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
