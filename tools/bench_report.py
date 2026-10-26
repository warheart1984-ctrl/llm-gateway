#!/usr/bin/env python3
"""Print the tool-folding benchmark results as one table, from the result files.

Every number comes from a `fold_tools_*.json` file written by
`tools/fold_tools.py`: the build is the commit the run recorded, the costs are
`aggregates.caching.provider_cost_usd` (provider prices, prompt caching
included, summarizer calls counted), and the answers are both tenants' scores
over every question asked. A run that stopped early (an `error`, or fewer calls
than it set out to make) is listed as incomplete, never with numbers that read
like a finished run's.

Usage: python tools/bench_report.py [result files or folders ...]
       (default: benchmark_results/)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

PATTERN = "fold_tools_*.json"
HEADER = ("calls", "build", "unfolded", "folded", "saved", "folds", "strict u/f", "lenient f")


def result_files(paths: list[str]) -> list[Path]:
    files: list[Path] = []
    for name in paths or ["benchmark_results"]:
        path = Path(name)
        files.extend(sorted(path.glob(PATTERN)) if path.is_dir() else [path])
    return files


def _percent(value: float | None) -> str:
    return "-" if value is None else f"{value * 100:.0f}%"


def row(report: dict[str, Any]) -> dict[str, Any]:
    """One run as table cells, or as incomplete with the reason."""
    settings = report.get("settings") or {}
    planned = settings.get("max_calls") or 0
    made = len(report.get("calls") or [])
    build = str(settings.get("git_commit") or "-")[:7]
    base = {"calls": planned or made, "build": build, "started": report.get("started_utc") or ""}
    error = report.get("error")
    aggregates = report.get("aggregates")
    if error or not aggregates or (planned and made < planned):
        reason = (error or {}).get("message") if isinstance(error, dict) else error
        return {**base, "incomplete": f"incomplete: stopped after {made} of {planned} calls" + (f" ({reason})" if reason else "")}
    cost = (aggregates.get("caching") or {}).get("provider_cost_usd") or {}
    strict = aggregates.get("accuracy") or {}
    lenient = aggregates.get("accuracy_lenient") or {}
    return {
        **base,
        "unfolded": cost.get("plain"),
        "folded": cost.get("folded"),
        "saved": cost.get("saving_share"),
        "folds": (aggregates.get("folds") or {}).get("committed"),
        "strict_plain": strict.get("plain"),
        "strict_folded": strict.get("folded"),
        # Older result files predate lenient scoring.
        "lenient_folded": lenient.get("folded"),
    }


def render(rows: list[dict[str, Any]]) -> str:
    lines = ["  ".join(f"{h:>10}" for h in HEADER)]
    for r in sorted(rows, key=lambda r: (r["calls"], r["started"])):
        head = f"{r['calls']:>10}  {r['build']:>10}"
        if "incomplete" in r:
            lines.append(f"{head}  {r['incomplete']}")
            continue
        money = lambda v: "-" if v is None else f"${v:.4f}"
        saved = "-" if r["saved"] is None else f"{r['saved'] * 100:+.1f}%"
        cells = [money(r["unfolded"]), money(r["folded"]), saved, str(r["folds"] if r["folds"] is not None else "-"),
                 f"{_percent(r['strict_plain'])}/{_percent(r['strict_folded'])}", _percent(r["lenient_folded"])]
        lines.append(head + "  " + "  ".join(f"{c:>10}" for c in cells))
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    files = result_files(argv[1:])
    if not files:
        print("bench_report: no fold_tools result files found", file=sys.stderr)
        return 2
    rows = [row(json.loads(path.read_text(encoding="utf-8"))) for path in files]
    sys.stdout.write(render(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
