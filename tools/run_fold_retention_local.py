"""Run the folding retention benchmark on this machine against real models.

    python tools/run_fold_retention_local.py --scenario fixed --profile lifecycle

Starts a release build of the gateway with the `bench/folding` configuration on
a free local port, runs `tools/fold_retention.py` against it, and stops the
gateway however the run ends.

Keys:

* The three gateway keys (plain, folded, admin) are made fresh for every run
  with `secrets.token_urlsafe` and exist only in the two child processes'
  environments. They are never printed or written to disk.
* The vendor key (`GROQ_API_KEY`, or `OPENROUTER_API_KEY` with `--provider
  openrouter`) is the only real credential. It is read from this process's
  environment, or on Windows from the user environment in the registry (set it
  with `[Environment]::SetEnvironmentVariable('OPENROUTER_API_KEY', (Read-Host
  -MaskInput 'key'), 'User')`). It is passed to the gateway only, and never
  printed.

`--provider` picks the models: `groq` uses `bench/folding/models.yaml`, and
`openrouter` uses `bench/folding/models.openrouter.yaml`, with the same tenant
files and the model ids swapped.

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
#: The tenant files name the Groq models; another provider's ids replace them.
GROQ_MAIN, GROQ_SUMMARIZER = "groq/gpt-oss-120b", "groq/gpt-oss-20b"
PROVIDERS = {
    "groq": {"models": "models.yaml", "key": "GROQ_API_KEY",
             "main": GROQ_MAIN, "summarizer": GROQ_SUMMARIZER},
    "openrouter": {"models": "models.openrouter.yaml", "key": "OPENROUTER_API_KEY",
                   "main": "openrouter/gpt-4.1-mini", "summarizer": "openrouter/gpt-4.1-nano"},
}


def vendor_key(name: str) -> str | None:
    key = os.environ.get(name)
    if key:
        return key
    if sys.platform == "win32":
        import winreg

        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as env:
                value, _kind = winreg.QueryValueEx(env, name)
                return value or None
        except OSError:
            return None
    return None


def model_prices(models_file: str, model: str) -> tuple[float, float]:
    """The cost the gateway bills for `model`, read from the bench models file."""
    text = (BENCH / models_file).read_text(encoding="utf-8")
    block = text.split(f"  {model}:", 1)[1].split("\n\n", 1)[0]
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
    parser.add_argument("--provider", choices=sorted(PROVIDERS), default="groq")
    parser.add_argument("--max-calls", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--binary", type=Path, default=ROOT / "target" / "release" /
                        ("llm-gateway.exe" if sys.platform == "win32" else "llm-gateway"))
    args = parser.parse_args(argv)

    provider = PROVIDERS[args.provider]
    key = vendor_key(provider["key"])
    if not key:
        print(f"run-fold-retention: {provider['key']} is not set (environment or user environment)", file=sys.stderr)
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
        shutil.copy(BENCH / "default.toml", config / "default.toml")
        shutil.copy(BENCH / provider["models"], config / "models.yaml")
        tenants = "tenants.lifecycle.yaml" if args.profile == "lifecycle" else "tenants.yaml"
        text = (BENCH / tenants).read_text(encoding="utf-8")
        text = text.replace(GROQ_SUMMARIZER, provider["summarizer"]).replace(GROQ_MAIN, provider["main"])
        (config / "tenants.yaml").write_text(text, encoding="utf-8")

        gateway_env = dict(os.environ, **gateway_keys, **{provider["key"]: key},
                           LLM_GATEWAY_CONFIG_DIR=str(config),
                           LLM_GATEWAY__SERVER__PORT=str(port))
        with log_path.open("wb") as log:
            gateway = subprocess.Popen([str(args.binary)], cwd=ROOT, env=gateway_env,
                                       stdout=log, stderr=subprocess.STDOUT)
        if not wait_live(base_url, gateway):
            print(f"run-fold-retention: the gateway did not become live; its log is {log_path}", file=sys.stderr)
            return 2

        main_in, main_out = model_prices(provider["models"], provider["main"])
        sum_in, sum_out = model_prices(provider["models"], provider["summarizer"])
        stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
        output = args.output or (ROOT / "benchmark_results" /
                                 f"fold_retention_{args.provider}_{args.scenario}_{args.profile}_{stamp}.json")
        command = [sys.executable, "-u", str(ROOT / "tools" / "fold_retention.py"),
                   "--base-url", base_url, "--model", provider["main"],
                   "--scenario", args.scenario, "--profile", args.profile,
                   "--main-input-per-mtok-usd", str(main_in), "--main-output-per-mtok-usd", str(main_out),
                   "--summarizer-input-per-mtok-usd", str(sum_in), "--summarizer-output-per-mtok-usd", str(sum_out),
                   "--output", str(output)]
        if args.max_calls:
            command += ["--max-calls", str(args.max_calls)]
        # The harness gets the gateway keys, never the vendor key.
        harness_env = {k: v for k, v in os.environ.items() if k != provider["key"]}
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
