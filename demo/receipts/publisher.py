"""Copy a gateway replica's anchors somewhere the gateway cannot write.

A local anchor sits next to the log it protects, so whoever can edit the log
can edit the anchor too. This process stands in for the separate account in a
real deployment (Wicket's anchor publisher pushes to a git repository the
gateway has no credentials for). It reads the replica's directory, appends any
new anchor lines to its own copy, and copies the public key the first time it
sees it.

It never rewrites what it already published. If the replica's anchor file
stops starting with what was published, or its key changes, it says so and
keeps the published copy as it is: that copy is the evidence.

    python publisher.py --source <replica receipts root> --dest <published dir>

Each tenant's chain is a folder under the replica's receipts root, and is
published to the folder of the same name under `--dest`.

Standard library only.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path


def complete_lines(path: Path) -> list[str]:
    """The file's finished lines. A last line without a newline is still being written."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    lines = text.split("\n")
    return [line for line in lines[:-1] if line.strip()]


def append(path: Path, lines: list[str]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as out:
        for line in lines:
            out.write(line + "\n")
        out.flush()
        os.fsync(out.fileno())


class Publisher:
    def __init__(self, source: Path, dest: Path) -> None:
        self.source = source
        self.dest = dest
        self.dest.mkdir(parents=True, exist_ok=True)
        self.warned: set[str] = set()

    def warn(self, key: str, message: str) -> None:
        if key not in self.warned:
            self.warned.add(key)
            print(f"publisher: ALERT {message}", flush=True)

    def sync(self) -> int:
        """Publish anything new. Returns how many anchor lines were added."""
        key_src = self.source / "signing.pub"
        key_dst = self.dest / "signing.pub"
        if key_src.exists():
            key = key_src.read_text(encoding="utf-8")
            if not key_dst.exists():
                key_dst.write_text(key, encoding="utf-8", newline="\n")
                print("publisher: copied the replica's public key", flush=True)
            elif key_dst.read_text(encoding="utf-8").strip() != key.strip():
                self.warn("key", "the replica's public key changed; the published key is kept")

        mine = complete_lines(self.dest / "anchor.jsonl")
        theirs = complete_lines(self.source / "anchor.jsonl")
        if theirs[: len(mine)] != mine:
            self.warn("rewrite", "the replica's anchor file no longer starts with what was published; nothing more is copied")
            return 0
        new = theirs[len(mine):]
        if new:
            append(self.dest / "anchor.jsonl", new)
            print(f"publisher: {self.source.name}: published {len(new)} anchor line(s), {len(mine) + len(new)} in total", flush=True)
        return len(new)


class Replica:
    """One publisher per tenant folder, found as tenants appear."""

    def __init__(self, source: Path, dest: Path) -> None:
        self.source = source
        self.dest = dest
        self.tenants: dict[str, Publisher] = {}

    def sync(self) -> int:
        added = 0
        if self.source.is_dir():
            for folder in sorted(self.source.iterdir()):
                if not folder.is_dir() or not ((folder / "signing.pub").exists() or (folder / "anchor.jsonl").exists()):
                    continue
                if folder.name not in self.tenants:
                    self.tenants[folder.name] = Publisher(folder, self.dest / folder.name)
                    print(f"publisher: publishing tenant {folder.name}", flush=True)
                added += self.tenants[folder.name].sync()
        return added


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--source", type=Path, required=True, help="the replica's receipts root (read only)")
    parser.add_argument("--dest", type=Path, required=True, help="where published anchors are kept")
    parser.add_argument("--interval", type=float, default=0.5, help="seconds between checks")
    parser.add_argument("--once", action="store_true", help="publish once and exit")
    args = parser.parse_args(argv)
    publisher = Replica(args.source, args.dest)
    print(f"publisher: watching {args.source}, publishing to {args.dest}", flush=True)
    while True:
        publisher.sync()
        if args.once:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
