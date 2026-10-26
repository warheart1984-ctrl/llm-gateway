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

`--scenario constraints` runs `tools/fold_tasks.py`, the task-success
benchmark (standing rules given in the conversation, checked on every
answer), instead of the fact-recall one; `--max-success-drop` is passed to it.

`--corpus` replays JSONL conversations (`tools/corpus_replay.py`) instead of
a synthetic scenario. `--scrub` redacts emails, phones and URLs first.
`--extract-facts` writes the answer key; `--extract-only` stops so it can be
edited before a second run. `--probe-every` is passed through.

`--provider` picks the models: `groq` uses `bench/folding/models.yaml`, and
`openrouter` uses `bench/folding/models.openrouter.yaml`, with the same tenant
files and the model ids swapped. `--summarizer` replaces the provider's
summarizer with another model from the same file (`--summarizer
openrouter/gpt-4.1-mini`); the result file's name then ends in
`_sum-<model>`. `--amortization-requests N` sets the folded tenant's
`amortization_requests`, how many requests the cost check counts a fold's
saving over (the name then ends in `_amort-N`).

Receipts are on for the folded tenant, so the gateway returns the summary it
sent in `x-gateway-fold-summary` and the harness can keep it. The receipt
files stay in the run's temporary config folder. After the run, each folded
call's fold decision (outcome, reason code, turns, cost estimates; counts and
hashes, no text) is read from the gateway log and added to the result file as
`fold_decision`.

Exit status is `tools/fold_retention.py`'s: 0 no regression, 1 regression, 2
incomplete (including a missing key or a gateway that did not start).
"""

from __future__ import annotations

import argparse
import json
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


def cached_price(models_file: str, model: str) -> float | None:
    """The provider's price for cached prompt tokens, if the models file has one."""
    text = (BENCH / models_file).read_text(encoding="utf-8")
    block = text.split(f"  {model}:", 1)[1].split("\n\n", 1)[0]
    found = re.search(r"cached_input_per_mtok_usd:\s*([0-9.]+)", block)
    return float(found.group(1)) if found else None


def model_prices(models_file: str, model: str) -> tuple[float, float]:
    """The cost the gateway bills for `model`, read from the bench models file."""
    text = (BENCH / models_file).read_text(encoding="utf-8")
    block = text.split(f"  {model}:", 1)[1].split("\n\n", 1)[0]
    found = [
        float(re.search(rf"{field}:\s*([0-9.]+)", block).group(1))
        for field in ("input_per_mtok_usd", "output_per_mtok_usd")
    ]
    return found[0], found[1]


def fold_decisions(log_text: str) -> dict[str, dict]:
    """The fold decision the gateway logged for each request id."""
    decisions: dict[str, dict] = {}
    for line in log_text.splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        fields = entry.get("fields", entry) if isinstance(entry, dict) else {}
        fold, request_id = fields.get("fold"), fields.get("request_id")
        if not isinstance(fold, str) or not isinstance(request_id, str):
            continue
        try:
            decision = json.loads(fold)
        except json.JSONDecodeError:
            continue
        if isinstance(decision, dict):
            decisions[request_id] = decision
            client = fields.get("client_request_id")
            if isinstance(client, str) and client not in ("", "-"):
                # Replay sends its own x-request-id. The gateway logs that as
                # client_request_id and echoes it, which is what the harness stores.
                decisions.setdefault(client, decision)
    return decisions


def attach_fold_decisions(result: Path, log: Path) -> int:
    """Add each folded call's logged decision to the result file; returns how
    many calls got one."""
    report = json.loads(result.read_text(encoding="utf-8"))
    decisions = fold_decisions(log.read_text(encoding="utf-8", errors="replace"))
    attached = 0
    for row in report.get("calls", report.get("rows", [])):
        decision = decisions.get(row.get("folded", {}).get("request_id") or "")
        if decision is not None:
            row["folded"]["fold_decision"] = decision
            attached += 1
    result.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return attached


def stop(gateway: subprocess.Popen | None) -> None:
    if gateway is not None and gateway.poll() is None:
        gateway.terminate()
        try:
            gateway.wait(timeout=15)
        except subprocess.TimeoutExpired:
            gateway.kill()


def results_dir(corpus: bool) -> Path:
    """Where a run's report goes. A corpus report can hold real conversation
    text, so it goes under the gitignored `corpus/`, never `benchmark_results/`."""
    return ROOT / "corpus" / "results" if corpus else ROOT / "benchmark_results"


def result_name(provider: str, scenario: str, profile: str, stamp: str, summarizer: str | None = None,
                amortization: int | None = None) -> str:
    """The result file's name; a summarizer other than the provider's own, and
    an amortization other than the profile's, are named in it."""
    suffix = f"_sum-{summarizer.rsplit('/', 1)[-1]}" if summarizer else ""
    suffix += f"_amort-{amortization}" if amortization else ""
    prefix = {"constraints": "fold_tasks", "tools": "fold_tools"}.get(scenario, "fold_retention")
    return f"{prefix}_{provider}_{scenario}_{profile}_{stamp}{suffix}.json"


def tenants_file(scenario: str, profile: str, corpus: bool = False) -> str:
    """The bench tenants file a run starts from. The tools scenario has its own,
    so the summarizer budget it needs does not change the other scenarios."""
    if scenario == "tools" and not corpus:
        return "tenants.tools.yaml"
    return "tenants.lifecycle.yaml" if profile == "lifecycle" else "tenants.yaml"


def with_amortization(tenants: str, requests: int) -> str:
    """The tenants file with the folded tenant's `amortization_requests` set."""
    lines = tenants.splitlines(keepends=True)
    hits = [i for i, line in enumerate(lines) if line.strip().startswith("amortization_requests:")]
    if len(hits) != 1:
        raise ValueError("the tenants file must set amortization_requests once")
    line = lines[hits[0]]
    lines[hits[0]] = f"{line[:len(line) - len(line.lstrip())]}amortization_requests: {requests}\n"
    return "".join(lines)


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
    parser.add_argument("--scenario", choices=["fixed", "mutating", "constraints", "tools"], default="fixed")
    parser.add_argument("--max-success-drop", type=float,
                        help="constraints only: how far the folded tenant's task success may fall")
    parser.add_argument("--profile", choices=["lifecycle", "economics"], default="economics")
    parser.add_argument("--provider", choices=sorted(PROVIDERS), default="groq")
    parser.add_argument("--summarizer", help="a model id from the provider's models file, in place of its summarizer")
    parser.add_argument("--amortization-requests", type=int,
                        help="the folded tenant's amortization_requests, in place of the profile's")
    parser.add_argument("--max-calls", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--corpus", type=Path,
                        help="replay this directory of JSONL conversations instead of a synthetic scenario")
    parser.add_argument("--extract-facts", action="store_true")
    parser.add_argument("--extract-only", action="store_true")
    parser.add_argument("--probe-every", type=int)
    parser.add_argument("--max-tokens", type=int)
    parser.add_argument("--scrub", action="store_true")
    parser.add_argument("--binary", type=Path, default=ROOT / "target" / "release" /
                        ("llm-gateway.exe" if sys.platform == "win32" else "llm-gateway"))
    args = parser.parse_args(argv)

    provider = PROVIDERS[args.provider]
    summarizer = args.summarizer or provider["summarizer"]
    if f"  {summarizer}:" not in (BENCH / provider["models"]).read_text(encoding="utf-8"):
        print(f"run-fold-retention: {summarizer} is not in {provider['models']}", file=sys.stderr)
        return 2
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
        tenants = tenants_file(args.scenario, args.profile, corpus=bool(args.corpus))
        text = (BENCH / tenants).read_text(encoding="utf-8")
        text = text.replace(GROQ_SUMMARIZER, summarizer).replace(GROQ_MAIN, provider["main"])
        if args.amortization_requests:
            text = with_amortization(text, args.amortization_requests)
        (config / "tenants.yaml").write_text(text, encoding="utf-8")

        gateway_env = dict(os.environ, **gateway_keys, **{provider["key"]: key},
                           LLM_GATEWAY_CONFIG_DIR=str(config),
                           LLM_GATEWAY__SERVER__PORT=str(port),
                           LLM_GATEWAY__RECEIPTS__ENABLED="true",
                           LLM_GATEWAY__RECEIPTS__DIRECTORY=str(config / "receipts"))
        with log_path.open("wb") as log:
            gateway = subprocess.Popen([str(args.binary)], cwd=ROOT, env=gateway_env,
                                       stdout=log, stderr=subprocess.STDOUT)
        if not wait_live(base_url, gateway):
            print(f"run-fold-retention: the gateway did not become live; its log is {log_path}", file=sys.stderr)
            return 2

        main_in, main_out = model_prices(provider["models"], provider["main"])
        sum_in, sum_out = model_prices(provider["models"], summarizer)
        stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
        scenario_name = "corpus" if args.corpus else args.scenario
        output = args.output or (results_dir(corpus=bool(args.corpus)) /
                                 result_name(args.provider, scenario_name, args.profile, stamp, args.summarizer,
                                             args.amortization_requests))
        harness = {"constraints": "fold_tasks.py", "tools": "fold_tools.py"}.get(args.scenario, "fold_retention.py") if not args.corpus else "fold_retention.py"
        command = [sys.executable, "-u", str(ROOT / "tools" / harness),
                   "--base-url", base_url, "--model", provider["main"],
                   "--profile", args.profile,
                   "--main-input-per-mtok-usd", str(main_in), "--main-output-per-mtok-usd", str(main_out),
                   "--summarizer-input-per-mtok-usd", str(sum_in), "--summarizer-output-per-mtok-usd", str(sum_out),
                   "--output", str(output)]
        if args.corpus:
            command += ["--corpus", str(args.corpus)]
            if args.extract_facts:
                command.append("--extract-facts")
            if args.extract_only:
                command.append("--extract-only")
            if args.scrub:
                command.append("--scrub")
            if args.probe_every:
                command += ["--probe-every", str(args.probe_every)]
        else:
            command += ["--scenario", args.scenario]
        cached = cached_price(provider["models"], provider["main"])
        if cached is not None:
            command += ["--main-cached-input-per-mtok-usd", str(cached)]
        if args.max_tokens:
            command += ["--max-tokens", str(args.max_tokens)]
        if args.max_calls and not args.corpus:
            command += ["--max-calls", str(args.max_calls)]
        if args.max_success_drop is not None and args.scenario == "constraints" and not args.corpus:
            command += ["--max-success-drop", str(args.max_success_drop)]
        # The harness gets the gateway keys, never the vendor key.
        harness_env = {k: v for k, v in os.environ.items() if k != provider["key"]}
        harness_env.update(LLM_GATEWAY_PLAIN_KEY=gateway_keys["GATEWAY_KEY_PLAIN"],
                           LLM_GATEWAY_FOLDED_KEY=gateway_keys["GATEWAY_KEY_FOLDED"],
                           LLM_GATEWAY_ADMIN_KEY=gateway_keys["GATEWAY_KEY_BENCH_ADMIN"])
        code = subprocess.run(command, cwd=ROOT, env=harness_env).returncode
        stop(gateway)
        if output.exists():
            attached = attach_fold_decisions(output, log_path)
            print(f"run-fold-retention: fold decisions attached to {attached} calls", file=sys.stderr)
            report = json.loads(output.read_text(encoding="utf-8"))
            if report.get("conversations"):
                from corpus_replay import refresh_conversation_totals
                import fold_retention as harness_mod
                try:
                    refresh_conversation_totals(output)
                except harness_mod.BenchmarkError as exc:
                    print(f"run-fold-retention: {exc}", file=sys.stderr)
                    return 2
        return code
    finally:
        stop(gateway)
        # The log may name request ids and errors; it never holds a key.
        print(f"run-fold-retention: gateway log kept at {log_path}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
