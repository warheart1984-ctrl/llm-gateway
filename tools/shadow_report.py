#!/usr/bin/env python3
"""Summarize shadow-mode fold decisions from a gateway JSON log.

Each finished request logs its fold decision on the ``stream complete`` line,
keyed by ``request_id`` (the caller's ``x-request-id`` when one was sent).
A shadow decision has ``shadow: true`` inside that object. This report never
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


def _cost(entry: dict) -> int:
    raw = entry.get("cost_nano_usd")
    if raw is None and isinstance(entry.get("fields"), dict):
        raw = entry["fields"].get("cost_nano_usd")
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return 0


def load(path: Path) -> dict[str, dict]:
    """Per-tenant totals. A line that is not a shadow decision is ignored."""
    totals: dict[str, dict] = defaultdict(
        lambda: {
            "requests": 0,
            "would_fold": 0,
            "reasons": defaultdict(int),
            "net_saving_nano_usd": 0,
            "summarizer_cost_nano_usd": 0,
            "backoff_cost_nano_usd": 0,
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
        row["requests"] += 1
        if fold.get("would_fold") is True:
            row["would_fold"] += 1
        reason = str(fold.get("reason") or "unknown")
        outcome = str(fold.get("outcome") or "")
        if outcome != "applied":
            row["reasons"][reason] += 1
        row["net_saving_nano_usd"] += int(fold.get("net_saving_nano_usd") or 0)
        row["summarizer_cost_nano_usd"] += int(fold.get("summarizer_cost_nano_usd") or 0)
        if reason == "backoff":
            row["backoff_cost_nano_usd"] += _cost(entry)
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
        for reason in sorted(row["reasons"]):
            rate = row["reasons"][reason] / requests if requests else 0.0
            lines.append(f"  refused {reason} {rate:.4f}")
        lines.append(f"  net_saving_nano_usd {row['net_saving_nano_usd']}")
        lines.append(f"  summarizer_cost_nano_usd {row['summarizer_cost_nano_usd']}")
        lines.append(f"  backoff_cost_nano_usd {row['backoff_cost_nano_usd']}")
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
