"""Run the receipts demo on this machine, without Docker.

Builds the gateway and the mock provider with cargo, starts them with this
folder's config (ports, paths and the mock's address rewritten for a local
run), starts the anchor publisher as a separate process, runs the
walkthrough, and stops everything. Works on Windows, macOS and Linux.

    python demo/receipts/run_local.py

Everything it wrote stays in a temporary folder whose path it prints, so the
log, the anchors and the published copy can be inspected afterwards.
"""

from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
EXE = ".exe" if os.name == "nt" else ""
DEMO_KEY = "demo-receipts-key-not-a-secret"
SHOESTRING_KEY = "shoestring-receipts-key-not-a-secret"
FOLDING_KEY = "folding-receipts-key-not-a-secret"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def write_config(target: Path, data: Path, mock: int, port: int, ops: int) -> None:
    target.mkdir(parents=True)
    replacements = [
        ("http://mock:9000", f"http://127.0.0.1:{mock}"),
        ("/var/lib/llm-gateway", data.as_posix()),
        ('bind_addr = "0.0.0.0"', 'bind_addr = "127.0.0.1"'),
        ('ops_bind_addr = "0.0.0.0"', 'ops_bind_addr = "127.0.0.1"'),
        ("\nport = 8080", f"\nport = {port}"),
        ("ops_port = 9090", f"ops_port = {ops}"),
    ]
    for name in ("default.toml", "models.yaml", "tenants.yaml"):
        text = (HERE / "config" / name).read_text(encoding="utf-8")
        for old, new in replacements:
            text = text.replace(old, new)
        (target / name).write_text(text, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--release", action="store_true", help="build and run release binaries")
    args = parser.parse_args(argv)

    profile = "release" if args.release else "debug"
    build = ["cargo", "build", "--locked", "--bin", "llm-gateway", "--example", "mock_upstream"]
    if args.release:
        build.append("--release")
    print("building:", " ".join(build), flush=True)
    subprocess.run(build, cwd=ROOT, check=True)
    gateway_bin = ROOT / "target" / profile / f"llm-gateway{EXE}"
    mock_bin = ROOT / "target" / profile / "examples" / f"mock_upstream{EXE}"

    work = Path(tempfile.mkdtemp(prefix="llm-gateway-receipts-demo-"))
    data = work / "gateway-data"
    published = work / "published"
    data.mkdir()
    mock, port, ops = free_port(), free_port(), free_port()
    write_config(work / "config", data, mock, port, ops)

    env = dict(os.environ)
    env.update(
        LLM_GATEWAY_CONFIG_DIR=str(work / "config"),
        RECEIPTS_DEMO_KEY=DEMO_KEY,
        RECEIPTS_SHOESTRING_KEY=SHOESTRING_KEY,
        RECEIPTS_FOLDING_KEY=FOLDING_KEY,
        MOCK_ADDR=f"127.0.0.1:{mock}",
        MOCK_TOKEN_DELAY_MS="5",
    )
    processes: list[subprocess.Popen] = []
    logs = []
    try:
        for name, command in [
            ("mock", [str(mock_bin)]),
            ("gateway", [str(gateway_bin)]),
            ("publisher", [sys.executable, str(HERE / "publisher.py"),
                           "--source", str(data / "receipts"), "--dest", str(published)]),
        ]:
            log = open(work / f"{name}.log", "w", encoding="utf-8")
            logs.append(log)
            processes.append(subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT))
        walkthrough = subprocess.run(
            [sys.executable, str(HERE / "walkthrough.py"),
             "--gateway", f"http://127.0.0.1:{port}",
             "--published-dir", str(published),
             "--verifier", str(ROOT / "vendor" / "wicket" / "verifier" / "ickverify.py"),
             "--verify-fold", str(ROOT / "tools" / "verify_fold.py")],
            env=env,
        )
        code = walkthrough.returncode
        try:
            import openai  # noqa: F401
        except ImportError:
            print("openai package not installed: skipping the OpenAI SDK check (pip install openai)")
        else:
            if code == 0:
                code = subprocess.run(
                    [sys.executable, str(HERE / "openai_sdk_check.py"), "--base-url", f"http://127.0.0.1:{port}/v1"],
                    env=env,
                ).returncode
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
        for log in logs:
            log.close()
    if code != 0:
        print(f"\ngateway log: {work / 'gateway.log'}")
    print(f"\nfiles kept in {work}")
    print("  gateway-data/receipts/  each tenant's log, anchor and key (the gateway's own disk)")
    print("  published/              the anchors and key the publisher copied out")
    return code


if __name__ == "__main__":
    shutil.which("cargo") or sys.exit("cargo is not on PATH")
    sys.exit(main())
