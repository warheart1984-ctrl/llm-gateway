"""Run the folding retention benchmark on this machine against real models.

    python tools/run_fold_retention_local.py --scenario fixed --profile lifecycle

Starts a release build of the gateway with the `bench/folding` configuration on
a free local port, runs `tools/fold_retention.py` against it, and stops the
gateway however the run ends.

Keys:

* The three gateway keys (plain, folded, admin) are made fresh for every run
  with `secrets.token_urlsafe` and exist only in the two child processes'
  environments. They are never printed or written to disk.
* `GROQ_API_KEY` is the only real credential. It is read from this process's
  environment, or on Windows from the user environment in the registry (set it
  with `[Environment]::SetEnvironmentVariable('GROQ_API_KEY', (Read-Host
  -MaskInput 'Groq key'), 'User')`). It is passed to the gateway only, and never
  printed.

Exit status is `tools/fold_retention.py`'s: 0 no regression, 1 regression, 2
incomplete (including a missing key or a gateway that did not start).
"""

from __future__ import annotations

import argparse
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "bench" / "folding"
MAIN_MODEL = "groq/gpt-oss-120b"
SUMMARIZER_MODEL = "groq/gpt-oss-20b"


def groq_key() -> str | None:
    key = os.environ.get("GROQ_API_KEY")
    if key:
        return key
    if sys.platform == "win32":
        import winreg

        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as env:
                value, _kind = winreg.QueryValueEx(env, "GROQ_API_KEY")
                return value or None
        except OSError:
            return None
    return None


def model_prices(model: str) -> tuple[float, float]:
    """The cost the gateway bills for `model`, read from the bench models.yaml."""
    text = (BENCH / "models.yaml").read_text(encoding="utf-8")
    block = text.split(f"  {model}:", 1)[1].split("\n  groq/", 1)[0]
    found = [
        float(re.search(rf"{field}:\s*([0-9.]+)", block).group(1))
        for field in ("input_per_mtok_usd", "output_per_mtok_usd")
    ]
    return found[0], found[1]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_live(base_url: str, gateway: subprocess.Popen, seconds: float = 120) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if gateway.poll() is not None:
            return False
        try:
            with urlopen(f"{base_url}/health/live", timeout=1) as response:
                if response.status == 200:
                    return True
        except OSError:
            time.sleep(0.5)
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--scenario", choices=["fixed", "mutating"], default="fixed")
    parser.add_argument("--profile", choices=["lifecycle", "economics"], default="economics")
    parser.add_argument("--max-calls", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--binary", type=Path, default=ROOT / "target" / "release" /
                        ("llm-gateway.exe" if sys.platform == "win32" else "llm-gateway"))
    args = parser.parse_args(argv)

    key = groq_key()
    if not key:
        print("run-fold-retention: GROQ_API_KEY is not set (environment or user environment)", file=sys.stderr)
        return 2
    if not args.binary.exists():
        print(f"run-fold-retention: build the gateway first: cargo build --locked --release ({args.binary} missing)",
              file=sys.stderr)
        return 2

    gateway_keys = {name: secrets.token_urlsafe(32)
                    for name in ("GATEWAY_KEY_PLAIN", "GATEWAY_KEY_FOLDED", "GATEWAY_KEY_BENCH_ADMIN")}
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    config = Path(tempfile.mkdtemp(prefix="fold-retention-config-"))
    log_path = config / "gateway.log"
    gateway = None
    try:
        for name in ("default.toml", "models.yaml"):
            shutil.copy(BENCH / name, config / name)
        tenants = "tenants.lifecycle.yaml" if args.profile == "lifecycle" else "tenants.yaml"
        shutil.copy(BENCH / tenants, config / "tenants.yaml")

        gateway_env = dict(os.environ, **gateway_keys, GROQ_API_KEY=key,
                           LLM_GATEWAY_CONFIG_DIR=str(config),
                           LLM_GATEWAY__SERVER__PORT=str(port))
        with log_path.open("wb") as log:
            gateway = subprocess.Popen([str(args.binary)], cwd=ROOT, env=gateway_env,
                                       stdout=log, stderr=subprocess.STDOUT)
        if not wait_live(base_url, gateway):
            print(f"run-fold-retention: the gateway did not become live; its log is {log_path}", file=sys.stderr)
            return 2

        main_in, main_out = model_prices(MAIN_MODEL)
        sum_in, sum_out = model_prices(SUMMARIZER_MODEL)
        output = args.output or (ROOT / "benchmark_results" /
                                 f"fold_retention_{args.scenario}_{args.profile}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
        command = [sys.executable, "-u", str(ROOT / "tools" / "fold_retention.py"),
                   "--base-url", base_url, "--model", MAIN_MODEL,
                   "--scenario", args.scenario, "--profile", args.profile,
                   "--main-input-per-mtok-usd", str(main_in), "--main-output-per-mtok-usd", str(main_out),
                   "--summarizer-input-per-mtok-usd", str(sum_in), "--summarizer-output-per-mtok-usd", str(sum_out),
                   "--output", str(output)]
        if args.max_calls:
            command += ["--max-calls", str(args.max_calls)]
        # The harness gets the gateway keys, never the Groq key.
        harness_env = {k: v for k, v in os.environ.items() if k != "GROQ_API_KEY"}
        harness_env.update(LLM_GATEWAY_PLAIN_KEY=gateway_keys["GATEWAY_KEY_PLAIN"],
                           LLM_GATEWAY_FOLDED_KEY=gateway_keys["GATEWAY_KEY_FOLDED"],
                           LLM_GATEWAY_ADMIN_KEY=gateway_keys["GATEWAY_KEY_BENCH_ADMIN"])
        return subprocess.run(command, cwd=ROOT, env=harness_env).returncode
    finally:
        if gateway is not None and gateway.poll() is None:
            gateway.terminate()
            try:
                gateway.wait(timeout=15)
            except subprocess.TimeoutExpired:
                gateway.kill()
        # The log may name request ids and errors; it never holds a key.
        print(f"run-fold-retention: gateway log kept at {log_path}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
