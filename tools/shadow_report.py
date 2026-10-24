#!/usr/bin/env python3
"""Summarize shadow-mode fold decisions from a gateway JSON log.

Each finished request logs its fold decision on the ``stream complete`` line,
keyed by ``request_id`` (the caller's ``x-request-id`` when one was sent).
A shadow decision has ``shadow: true`` inside that object. With
``[folding] background = true`` the summarizer runs after that line is
written, so a background shadow fold logs its own ``shadow fold finished``
line (``background: true``) with what the summarizer cost; it counts towards
the cost and the net saving, not towards the requests. This report never
reads summary text: the log does not carry any.

Usage: python tools/shadow_report.py <gateway.log>
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path


def _fold(entry: dict) -> dict | None:
    raw = entry.get("fold")
    if raw is None and isinstance(entry.get("fields"), dict):
        raw = entry["fields"].get("fold")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if isinstance(raw, dict) and raw.get("shadow") is True:
        return raw
    return None


def _tenant(entry: dict) -> str:
    tenant = entry.get("tenant")
    if tenant is None and isinstance(entry.get("fields"), dict):
        tenant = entry["fields"].get("tenant")
    return str(tenant or "unknown")


def load(path: Path) -> dict[str, dict]:
    """Per-tenant totals. A line that is not a shadow decision is ignored."""
    totals: dict[str, dict] = defaultdict(
        lambda: {
            "requests": 0,
            "would_fold": 0,
            # (outcome, reason) of every request's decision, applied included.
            "outcomes": defaultdict(int),
            "background_folds": 0,
            "net_saving_nano_usd": 0,
            "summarizer_cost_nano_usd": 0,
        }
    )
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(entry, dict):
            continue
        fold = _fold(entry)
        if fold is None:
            continue
        row = totals[_tenant(entry)]
        row["net_saving_nano_usd"] += int(fold.get("net_saving_nano_usd") or 0)
        row["summarizer_cost_nano_usd"] += int(fold.get("summarizer_cost_nano_usd") or 0)
        if fold.get("background") is True:
            row["background_folds"] += 1
            continue
        row["requests"] += 1
        if fold.get("would_fold") is True:
            row["would_fold"] += 1
        outcome = str(fold.get("outcome") or "unknown")
        reason = str(fold.get("reason") or "unknown")
        row["outcomes"][(outcome, reason)] += 1
    return totals


def render(totals: dict[str, dict]) -> str:
    lines: list[str] = []
    for tenant in sorted(totals):
        row = totals[tenant]
        requests = row["requests"]
        would = (row["would_fold"] / requests) if requests else 0.0
        lines.append(f"tenant {tenant}")
        lines.append(f"  requests {requests}")
        lines.append(f"  would_fold_rate {would:.4f}")
        # The fold's own outcome. `not_due` can still mean the request would
        # have gone out with an earlier summary: see would_fold_rate.
        for outcome, reason in sorted(row["outcomes"]):
            rate = row["outcomes"][(outcome, reason)] / requests if requests else 0.0
            lines.append(f"  outcome {outcome} {reason} {rate:.4f}")
        lines.append(f"  background_folds {row['background_folds']}")
        lines.append(f"  net_saving_nano_usd {row['net_saving_nano_usd']}")
        lines.append(f"  summarizer_cost_nano_usd {row['summarizer_cost_nano_usd']}")
    return "\n".join(lines) + ("\n" if lines else "")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: python tools/shadow_report.py <gateway.log>", file=sys.stderr)
        return 2
    path = Path(argv[1])
    if not path.is_file():
        print(f"shadow_report: {path} is not a file", file=sys.stderr)
        return 2
    sys.stdout.write(render(load(path)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
