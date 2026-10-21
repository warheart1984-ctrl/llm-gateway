"""Publish a gateway replica's receipt anchors to a git repository it cannot write.

A tenant's receipt log can only prove nothing was deleted from its end if a
copy of its newest anchor lives somewhere the gateway operator cannot change.
This publisher is that copy's writer. It runs as its own account, reads the
replica's receipts root (each tenant's chain is a folder in it), and pushes
each tenant's `anchor.jsonl` and `signing.pub` to a git repository under
`<replica>/<tenant>/`.

It only ever appends:

* It never force-pushes. A push the remote refuses is retried from the
  remote's state, never over it.
* It refuses to publish more for a tenant whose anchor file on the replica
  no longer starts with what was already published, and for a key that has
  changed. The published copy is the evidence, so it is never "corrected".
* It remembers the last commit it pushed. If the remote's history no longer
  contains it, someone rewrote the published history: it stops and says so.

Subcommands:

    python tools/anchor_publisher.py publish --source ROOT --repo URL --replica NAME --state FILE [--watch SECONDS]
    python tools/anchor_publisher.py verify  --repo URL --replica NAME --tenant ID --log receipts.jsonl [--verifier ickverify.py]

Exit status: 0 published (or nothing new), 1 failed (git, I/O), 2 refused
(a rewrite was detected; nothing was published for the affected tenant).
`verify` exits with ickverify.py's status: 0 verified, 1 not, 2 unreadable.

Standard library and the git command line only. See docs/anchoring.md for
the account and repository setup that makes the boundary real.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_BRANCH = "anchors"
SAFE = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")


class GitError(Exception):
    pass


def safe_name(name: str) -> bool:
    return 0 < len(name) <= 64 and name[0].isalnum() and set(name) <= SAFE


def git(*args: str, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess:
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0", LC_ALL="C")
    run = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, env=env)
    if check and run.returncode != 0:
        raise GitError(f"git {' '.join(args)}: {run.stderr.strip() or run.stdout.strip()}")
    return run


def complete_lines(path: Path) -> list[str]:
    """Finished lines only: a last line without a newline is still being written."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    return [line for line in text.split("\n")[:-1] if line.strip()]


@dataclass
class Report:
    published: dict[str, int] = field(default_factory=dict)
    refused: dict[str, str] = field(default_factory=dict)
    commit: str | None = None

    def alert(self, tenant: str, why: str) -> None:
        self.refused[tenant] = why
        print(f"anchor-publisher: ALERT {tenant}: {why}", file=sys.stderr, flush=True)


class RewriteDetected(Exception):
    pass


class Publisher:
    def __init__(self, source: Path, repo: str, replica: str, state: Path, work: Path, branch: str = DEFAULT_BRANCH,
                 author: str = "llm-gateway anchor publisher <anchors@invalid>",
                 status: Path | None = None) -> None:
        if not safe_name(replica):
            raise ValueError(f"replica name {replica!r} must be 1-64 of letters, digits, '.', '_', '-'")
        self.source, self.repo, self.replica = source, repo, replica
        self.state, self.work, self.branch = state, work, branch
        self.status_path = status
        name, _, email = author.partition(" <")
        self.identity = ["-c", f"user.name={name}", "-c", f"user.email={email.rstrip('>')}"]

    # ---- the publisher's own memory of what it pushed ---------------------------------------

    def last_pushed(self) -> str | None:
        try:
            return json.loads(self.state.read_text(encoding="utf-8")).get("last_pushed")
        except (FileNotFoundError, ValueError):
            return None

    def remember(self, commit: str) -> None:
        self.state.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state.with_suffix(".tmp")
        tmp.write_text(json.dumps({"last_pushed": commit, "repo": self.repo, "branch": self.branch}), encoding="utf-8")
        os.replace(tmp, self.state)

    # ---- the working copy, always exactly the remote ----------------------------------------

    def sync(self) -> bool:
        """Make the working copy equal the remote branch. False if the branch does not exist yet."""
        if not (self.work / ".git").exists():
            self.work.parent.mkdir(parents=True, exist_ok=True)
            git("clone", "--quiet", self.repo, str(self.work))
        # --prune: a branch deleted on the remote must look deleted here too,
        # not linger as a stale copy that hides the deletion.
        git("fetch", "--quiet", "--prune", "origin", cwd=self.work)
        exists = git("rev-parse", "--verify", "--quiet", f"refs/remotes/origin/{self.branch}", cwd=self.work, check=False)
        last = self.last_pushed()
        if exists.returncode != 0:
            if last:
                raise RewriteDetected(f"the branch {self.branch!r} this publisher pushed to ({last[:12]}) is gone")
            # Nothing published yet: an empty branch, with nothing left over
            # from an attempt whose push was refused.
            git("symbolic-ref", "HEAD", f"refs/heads/{self.branch}", cwd=self.work)
            git("update-ref", "-d", f"refs/heads/{self.branch}", cwd=self.work, check=False)
            git("rm", "-r", "--quiet", "--cached", "--ignore-unmatch", ".", cwd=self.work)
            git("clean", "-fdxq", cwd=self.work)
            return False
        remote = exists.stdout.strip()
        if last:
            known = git("cat-file", "-e", f"{last}^{{commit}}", cwd=self.work, check=False).returncode == 0
            ancestor = known and git("merge-base", "--is-ancestor", last, remote, cwd=self.work, check=False).returncode == 0
            if not ancestor:
                raise RewriteDetected(
                    f"the remote branch no longer contains {last[:12]}, the last commit this publisher pushed: "
                    "published history was rewritten"
                )
        git("checkout", "--quiet", "--force", "-B", self.branch, remote, cwd=self.work)
        git("reset", "--quiet", "--hard", remote, cwd=self.work)
        git("clean", "-fdxq", cwd=self.work)
        return True

    # ---- one pass ---------------------------------------------------------------------------

    def tenants(self) -> list[Path]:
        if not self.source.is_dir():
            return []
        return [d for d in sorted(self.source.iterdir())
                if d.is_dir() and safe_name(d.name) and ((d / "anchor.jsonl").exists() or (d / "signing.pub").exists())]

    def stage(self, report: Report) -> list[str]:
        """Append what is new for every tenant to the working copy. Returns commit-message lines."""
        lines = []
        for folder in self.tenants():
            tenant = folder.name
            dest = self.work / self.replica / tenant
            key_src, key_dst = folder / "signing.pub", dest / "signing.pub"
            if key_src.exists() and key_dst.exists() and key_src.read_text(encoding="utf-8").strip() != key_dst.read_text(encoding="utf-8").strip():
                report.alert(tenant, "the replica's public key changed; the published key is kept and nothing more is published")
                continue
            published = complete_lines(dest / "anchor.jsonl")
            current = complete_lines(folder / "anchor.jsonl")
            if current[: len(published)] != published:
                report.alert(tenant, "the replica's anchor file no longer starts with what was published; nothing more is published")
                continue
            new = current[len(published):]
            added_key = key_src.exists() and not key_dst.exists()
            if not new and not added_key:
                continue
            dest.mkdir(parents=True, exist_ok=True)
            if added_key:
                key_dst.write_text(key_src.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")
            if new:
                with (dest / "anchor.jsonl").open("a", encoding="utf-8", newline="\n") as out:
                    for line in new:
                        out.write(line + "\n")
                head = json.loads(new[-1])
                lines.append(f"{tenant}: count {head.get('count')} head {str(head.get('head_receipt_id'))[:40]}")
                report.published[tenant] = len(new)
            if added_key:
                lines.append(f"{tenant}: public key")
        return lines

    def publish_once(self, attempts: int = 5) -> Report:
        for _ in range(attempts):
            report = Report()
            self.sync()
            lines = self.stage(report)
            if not lines:
                self.write_status(report)
                return report
            git("add", "--all", self.replica, cwd=self.work)
            message = f"anchors from {self.replica}\n\n" + "\n".join(lines)
            git(*self.identity, "commit", "--quiet", "-m", message, cwd=self.work)
            push = git("push", "--quiet", "origin", f"HEAD:refs/heads/{self.branch}", cwd=self.work, check=False)
            if push.returncode == 0:
                commit = git("rev-parse", "HEAD", cwd=self.work).stdout.strip()
                self.remember(commit)
                report.commit = commit
                for line in lines:
                    print(f"anchor-publisher: {self.replica}/{line}", flush=True)
                self.write_status(report)
                return report
            # Someone else pushed first. Start again from the remote's state;
            # never push over it.
            time.sleep(0.2)
        raise GitError(f"push to {self.repo} kept being refused after {attempts} attempts")

    def write_status(self, report: Report | None, error: str | None = None) -> None:
        """Atomically write a JSON summary of this pass to --status, if any.

        `report` is None when the pass failed before it had one; `error` then
        says why. The last push is carried over from passes that pushed
        nothing, so a quiet pass does not read as "never pushed".
        """
        if self.status_path is None:
            return
        checked_at = int(time.time())
        refused = report.refused if report is not None else {}
        if error is None and refused:
            error = "refused " + "; ".join(f"{tenant}: {why}" for tenant, why in sorted(refused.items()))
        last_push = (report.commit if report is not None else None) or self.last_pushed()
        if report is not None and report.commit:
            last_push_at = checked_at
        else:
            try:
                previous = json.loads(self.status_path.read_text(encoding="utf-8"))
            except (FileNotFoundError, ValueError):
                previous = {}
            same = isinstance(previous, dict) and previous.get("last_push") == last_push
            last_push_at = previous.get("last_push_at", 0) if same else 0
        status = {
            "checked_at": checked_at,
            "ok": error is None,
            "last_push": last_push,
            "last_push_at": last_push_at,
            "published": report.published if report is not None else {},
            "refused": refused,
            "error": " ".join(error.split()) if error is not None else None,
        }
        self.status_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.status_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(status), encoding="utf-8")
        os.replace(tmp, self.status_path)


def verify(repo: str, replica: str, tenant: str, log: Path, verifier: Path, branch: str = DEFAULT_BRANCH) -> int:
    """Check a tenant's log against the published anchor and key, with the unchanged verifier."""
    if not (safe_name(replica) and safe_name(tenant)):
        print("replica and tenant must be safe names", file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory(prefix="anchors-") as tmp:
        clone = Path(tmp) / "repo"
        git("clone", "--quiet", "--depth", "1", "--branch", branch, repo, str(clone))
        folder = clone / replica / tenant
        anchor, key = folder / "anchor.jsonl", folder / "signing.pub"
        if not anchor.exists() or not key.exists():
            print(f"nothing published for {replica}/{tenant}", file=sys.stderr)
            return 2
        run = subprocess.run([sys.executable, str(verifier), str(log), "--anchor", str(anchor),
                              "--trusted-keys", str(key), "--require-signatures"])
        return run.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    pub = sub.add_parser("publish", help="publish new anchors and keys")
    pub.add_argument("--source", type=Path, required=True, help="the replica's receipts root (read only)")
    pub.add_argument("--repo", required=True, help="git URL or path of the anchor repository")
    pub.add_argument("--replica", required=True, help="this replica's name: the top folder in the repository")
    pub.add_argument("--state", type=Path, required=True, help="where this publisher remembers its last push")
    pub.add_argument("--work", type=Path, help="working copy (default: next to --state)")
    pub.add_argument("--branch", default=DEFAULT_BRANCH)
    pub.add_argument("--watch", type=float, metavar="SECONDS", help="keep publishing at this interval")
    pub.add_argument("--status", type=Path, help="write a JSON summary of each pass to this file")
    ver = sub.add_parser("verify", help="check a tenant's log against the published anchor and key")
    ver.add_argument("--repo", required=True)
    ver.add_argument("--replica", required=True)
    ver.add_argument("--tenant", required=True)
    ver.add_argument("--log", type=Path, required=True)
    ver.add_argument("--branch", default=DEFAULT_BRANCH)
    ver.add_argument("--verifier", type=Path,
                     default=Path(__file__).resolve().parent.parent / "vendor" / "wicket" / "verifier" / "ickverify.py")
    args = parser.parse_args(argv)

    if args.command == "verify":
        return verify(args.repo, args.replica, args.tenant, args.log, args.verifier, args.branch)

    publisher = Publisher(args.source, args.repo, args.replica, args.state,
                          args.work or args.state.parent / "anchor-repo", args.branch,
                          status=args.status)
    while True:
        try:
            report = publisher.publish_once()
        except RewriteDetected as rewrite:
            print(f"anchor-publisher: ALERT {rewrite}; nothing is published until a person looks", file=sys.stderr, flush=True)
            publisher.write_status(None, str(rewrite))
            return 2
        except GitError as error:
            print(f"anchor-publisher: {error}", file=sys.stderr, flush=True)
            publisher.write_status(None, str(error))
            if args.watch is None:
                return 1
            time.sleep(args.watch)
            continue
        if args.watch is None:
            return 2 if report.refused else 0
        time.sleep(args.watch)


if __name__ == "__main__":
    sys.exit(main())
