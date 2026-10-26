"""The benchmark table. Each assertion fails if that column or rule is dropped."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import bench_report  # noqa: E402


def _report(calls: int, made: int, *, error: str | None = None, lenient: bool = True, started: str = "2026-10-26T10:00:00") -> dict:
    report = {
        "started_utc": started,
        "settings": {"max_calls": calls, "git_commit": "f9b6a634d9ea796a594eabae053d841391ceab1d"},
        "calls": [{}] * made,
        "aggregates": {
            "caching": {"provider_cost_usd": {"plain": 0.7906, "folded": 0.4696, "saving_share": 0.40602}},
            "folds": {"committed": 2},
            "accuracy": {"plain": 0.83, "folded": 0.74},
        },
    }
    if lenient:
        report["aggregates"]["accuracy_lenient"] = {"plain": 0.99, "folded": 0.98}
    if error:
        report["error"] = {"type": "TransientError", "message": error}
        del report["aggregates"]
    return report


class BenchReportTests(unittest.TestCase):
    def write(self, folder: Path, name: str, report: dict) -> None:
        (folder / name).write_text(json.dumps(report), encoding="utf-8")

    def test_one_row_per_run_from_the_files(self) -> None:
        folder = Path(tempfile.mkdtemp())
        self.write(folder, "fold_tools_b.json", _report(500, 500))
        self.write(folder, "fold_tools_a.json", _report(90, 90, lenient=False, started="2026-10-26T09:00:00"))
        self.write(folder, "fold_tools_c.json", _report(150, 82, error="upstream 502"))
        self.write(folder, "other.json", _report(10, 10))
        rows = [bench_report.row(json.loads(p.read_text(encoding="utf-8"))) for p in bench_report.result_files([str(folder)])]
        self.assertEqual(len(rows), 3, "only fold_tools_* files")
        text = bench_report.render(rows)
        lines = text.splitlines()
        self.assertIn("calls", lines[0])
        # Sorted by calls; every cell from the file.
        self.assertTrue(lines[1].split()[0] == "90" and lines[2].split()[0] == "150" and lines[3].split()[0] == "500", text)
        self.assertIn("f9b6a63", lines[3])
        self.assertIn("$0.7906", lines[3])
        self.assertIn("$0.4696", lines[3])
        self.assertIn("+40.6%", lines[3])
        self.assertIn("83%/74%", lines[3])
        self.assertTrue(lines[3].rstrip().endswith("98%"), text)
        # A file from before lenient scoring shows no lenient score.
        self.assertTrue(lines[1].rstrip().endswith("-"), text)
        # A run that stopped is not shown with a finished run's numbers.
        self.assertIn("incomplete: stopped after 82 of 150 calls (upstream 502)", lines[2])
        self.assertNotIn("$", lines[2])

    def test_a_short_run_without_an_error_is_still_incomplete(self) -> None:
        row = bench_report.row(_report(300, 120))
        self.assertIn("incomplete: stopped after 120 of 300 calls", row["incomplete"])

    def test_no_files_is_an_error(self) -> None:
        self.assertEqual(bench_report.main(["bench_report.py", tempfile.mkdtemp()]), 2)


if __name__ == "__main__":
    unittest.main()
