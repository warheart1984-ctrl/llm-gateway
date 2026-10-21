"""Tests for tools/anchor_publisher.py, against real git repositories on disk.

    python -m unittest discover -s tools/tests -v
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import anchor_publisher as ap  # noqa: E402

ID = ["-c", "user.name=someone else", "-c", "user.email=else@invalid"]


def run(*args: str, cwd: Path | None = None) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def anchor(count: int) -> str:
    return json.dumps({"version": "infinity.anchor.v1", "count": count,
                       "head_receipt_id": f"receipt:sha3-256:{count:064x}"})


class Case(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="anchor-publisher-"))
        self.remote = self.tmp / "remote.git"
        run("init", "--quiet", "--bare", str(self.remote))
        self.root = self.tmp / "replica"

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def tenant(self, name: str, counts: list[int], key: str = "ed25519-public:aa") -> Path:
        folder = self.root / name
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "anchor.jsonl").write_text("".join(anchor(c) + "\n" for c in counts), encoding="utf-8")
        (folder / "signing.pub").write_text(key + "\n", encoding="utf-8")
        return folder

    def publisher(self, replica: str = "r1") -> ap.Publisher:
        return ap.Publisher(self.root, str(self.remote), replica, self.tmp / f"state-{replica}" / "state.json",
                            self.tmp / f"work-{replica}")

    def published(self, *path: str) -> str:
        return run("--git-dir", str(self.remote), "show", f"{ap.DEFAULT_BRANCH}:{'/'.join(path)}")

    def commits(self) -> list[str]:
        return run("--git-dir", str(self.remote), "rev-list", ap.DEFAULT_BRANCH).splitlines()


class Publishing(Case):
    def test_each_tenant_goes_to_its_own_folder_and_the_push_is_remembered(self) -> None:
        self.tenant("acme", [1, 2])
        self.tenant("globex", [1], key="ed25519-public:bb")
        publisher = self.publisher()
        report = publisher.publish_once()
        self.assertEqual(report.published, {"acme": 2, "globex": 1})
        self.assertEqual(self.published("r1", "acme", "anchor.jsonl").splitlines(), [anchor(1), anchor(2)])
        self.assertEqual(self.published("r1", "globex", "signing.pub"), "ed25519-public:bb")
        self.assertEqual(publisher.last_pushed(), self.commits()[0])

    def test_nothing_new_makes_no_commit_and_new_lines_are_appended(self) -> None:
        folder = self.tenant("acme", [1])
        publisher = self.publisher()
        publisher.publish_once()
        self.assertEqual(publisher.publish_once().commit, None)
        self.assertEqual(len(self.commits()), 1)
        with (folder / "anchor.jsonl").open("a", encoding="utf-8") as out:
            out.write(anchor(5) + "\n")
        self.assertEqual(publisher.publish_once().published, {"acme": 1})
        self.assertEqual(self.published("r1", "acme", "anchor.jsonl").splitlines(), [anchor(1), anchor(5)])
        self.assertEqual(len(self.commits()), 2)

    def test_a_half_written_anchor_line_waits(self) -> None:
        folder = self.tenant("acme", [1])
        with (folder / "anchor.jsonl").open("a", encoding="utf-8") as out:
            out.write(anchor(2)[:20])
        self.publisher().publish_once()
        self.assertEqual(self.published("r1", "acme", "anchor.jsonl").splitlines(), [anchor(1)])

    def test_two_replicas_share_a_repository_without_losing_each_others_commits(self) -> None:
        self.tenant("acme", [1])
        first, second = self.publisher("r1"), self.publisher("r2")
        first.publish_once()
        second.publish_once()
        with (self.root / "acme" / "anchor.jsonl").open("a", encoding="utf-8") as out:
            out.write(anchor(2) + "\n")
        first.publish_once()
        self.assertEqual(self.published("r2", "acme", "anchor.jsonl").splitlines(), [anchor(1)])
        self.assertEqual(self.published("r1", "acme", "anchor.jsonl").splitlines(), [anchor(1), anchor(2)])
        self.assertEqual(len(self.commits()), 3, "a linear history, nothing pushed over")

    def test_unsafe_names_never_become_paths(self) -> None:
        with self.assertRaises(ValueError):
            ap.Publisher(self.root, str(self.remote), "../escape", self.tmp / "s.json", self.tmp / "w")
        self.tenant("acme", [1])
        (self.root / ".hidden").mkdir(parents=True)
        (self.root / ".hidden" / "anchor.jsonl").write_text(anchor(1) + "\n", encoding="utf-8")
        report = self.publisher().publish_once()
        self.assertEqual(set(report.published), {"acme"})


class Refusing(Case):
    def test_a_rewritten_replica_anchor_is_not_published_over(self) -> None:
        acme = self.tenant("acme", [1, 2])
        self.tenant("globex", [1])
        publisher = self.publisher()
        publisher.publish_once()
        # Someone with the gateway's disk rewrites acme's anchors.
        (acme / "anchor.jsonl").write_text(anchor(1) + "\n", encoding="utf-8")
        with (self.root / "globex" / "anchor.jsonl").open("a", encoding="utf-8") as out:
            out.write(anchor(3) + "\n")
        report = publisher.publish_once()
        self.assertIn("acme", report.refused)
        self.assertEqual(report.published, {"globex": 1}, "other tenants are still published")
        self.assertEqual(self.published("r1", "acme", "anchor.jsonl").splitlines(), [anchor(1), anchor(2)],
                         "the published copy is the evidence and is kept")
        self.assertEqual(ap.main(["publish", "--source", str(self.root), "--repo", str(self.remote),
                                  "--replica", "r1", "--state", str(self.tmp / "state-r1" / "state.json"),
                                  "--work", str(self.tmp / "work-r1")]), 2)

    def test_a_changed_key_is_not_published_over(self) -> None:
        self.tenant("acme", [1])
        publisher = self.publisher()
        publisher.publish_once()
        self.tenant("acme", [1, 2], key="ed25519-public:ff")
        report = publisher.publish_once()
        self.assertIn("acme", report.refused)
        self.assertEqual(self.published("r1", "acme", "signing.pub"), "ed25519-public:aa")
        self.assertEqual(self.published("r1", "acme", "anchor.jsonl").splitlines(), [anchor(1)])

    def test_force_pushed_history_is_detected_and_nothing_is_published(self) -> None:
        self.tenant("acme", [1, 2])
        publisher = self.publisher()
        publisher.publish_once()
        # Someone with push rights replaces the published history.
        other = self.tmp / "other"
        run("clone", "--quiet", str(self.remote), str(other))
        run("checkout", "--quiet", "--orphan", "fake", cwd=other)
        run("rm", "-r", "--quiet", "--cached", "--ignore-unmatch", ".", cwd=other)
        (other / "r1").mkdir(exist_ok=True)
        (other / "r1" / "note").write_text("nothing to see\n", encoding="utf-8")
        run("add", "r1", cwd=other)
        run(*ID, "commit", "--quiet", "-m", "rewritten", cwd=other)
        run("push", "--quiet", "--force", "origin", f"HEAD:{ap.DEFAULT_BRANCH}", cwd=other)
        rewritten = self.commits()

        with (self.root / "acme" / "anchor.jsonl").open("a", encoding="utf-8") as out:
            out.write(anchor(3) + "\n")
        with self.assertRaises(ap.RewriteDetected):
            publisher.publish_once()
        self.assertEqual(self.commits(), rewritten, "the publisher did not push into the rewritten history")

    def test_a_deleted_branch_is_detected(self) -> None:
        self.tenant("acme", [1])
        publisher = self.publisher()
        publisher.publish_once()
        run("--git-dir", str(self.remote), "update-ref", "-d", f"refs/heads/{ap.DEFAULT_BRANCH}")
        with self.assertRaises(ap.RewriteDetected):
            publisher.publish_once()


class Verifying(Case):
    def test_verify_hands_the_published_anchor_and_key_to_the_verifier(self) -> None:
        self.tenant("acme", [1, 2])
        self.publisher().publish_once()
        log = self.tmp / "receipts.jsonl"
        log.write_text("{}\n", encoding="utf-8")
        seen = self.tmp / "seen.json"
        stub = self.tmp / "stub_verifier.py"
        stub.write_text(
            "import json, sys\n"
            "args = sys.argv[1:]\n"
            "anchor = open(args[args.index('--anchor') + 1]).read()\n"
            "key = open(args[args.index('--trusted-keys') + 1]).read()\n"
            f"json.dump({{'log': args[0], 'anchor': anchor, 'key': key, 'strict': '--require-signatures' in args}}, open(r'{seen}', 'w'))\n",
            encoding="utf-8",
        )
        code = ap.main(["verify", "--repo", str(self.remote), "--replica", "r1", "--tenant", "acme",
                        "--log", str(log), "--verifier", str(stub)])
        self.assertEqual(code, 0)
        got = json.loads(seen.read_text(encoding="utf-8"))
        self.assertEqual(got["anchor"].splitlines(), [anchor(1), anchor(2)])
        self.assertEqual(got["key"].strip(), "ed25519-public:aa")
        self.assertTrue(got["strict"])
        self.assertEqual(Path(got["log"]), log)

    def test_verify_with_nothing_published_says_so(self) -> None:
        self.tenant("acme", [1])
        self.publisher().publish_once()
        code = ap.main(["verify", "--repo", str(self.remote), "--replica", "r1", "--tenant", "nobody",
                        "--log", str(self.tmp / "x.jsonl")])
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
