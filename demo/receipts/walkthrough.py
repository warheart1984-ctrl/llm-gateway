"""Scripted walkthrough of the gateway's receipts, checked from the outside.

It plays a client and then an auditor. Every step says what it shows and
checks it; a failed check exits non-zero, which is what CI keys off.

    python walkthrough.py --gateway URL --gateway-dir DIR --published-dir DIR --verifier ickverify.py

`--gateway-dir` is the replica's receipts directory (read only here).
`--published-dir` is where the publisher copies anchors and the public key.
Keys come from RECEIPTS_DEMO_KEY and RECEIPTS_SHOESTRING_KEY.

Standard library only. The digest recipes are the ones in the README.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

PROMPT = "Explain receipts in one breath. PROMPT-MARKER-7f3a"
ANSWER_FRAGMENT = "reserved this answer"  # a phrase from the mock model's reply

USE_COLOR = sys.stdout.isatty() or os.environ.get("FORCE_COLOR")


def paint(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if USE_COLOR else text


def step(title: str) -> None:
    print(f"\n{paint('1', '== ' + title)}", flush=True)


def ok(text: str) -> None:
    print(f"   {paint('32', 'ok')}  {text}", flush=True)


def say(text: str) -> None:
    print(f"       {text}", flush=True)


class Failed(Exception):
    pass


def check(condition: bool, text: str) -> None:
    if not condition:
        raise Failed(text)
    ok(text)


# ---- digests, exactly as the README gives them ------------------------------------------------

def request_digest(salt: bytes, body: bytes) -> str:
    return "sha256:" + hashlib.sha256(salt + b"llm-gateway/request/v1\n" + body).hexdigest()


def reply_digest(salt: bytes, content: str, reasoning: str, tools: dict[int, dict[str, str]]) -> str:
    tool_bytes = b""
    for index in sorted(tools):
        call = tools[index]
        tool_bytes += str(index).encode()
        for part in (call["id"], call["name"], call["arguments"]):
            tool_bytes += b"\x1f" + part.encode()
        tool_bytes += b"\x1e"
    inner = (
        hashlib.sha256(content.encode()).digest()
        + hashlib.sha256(reasoning.encode()).digest()
        + hashlib.sha256(tool_bytes).digest()
    )
    return "sha256:" + hashlib.sha256(salt + b"llm-gateway/reply/v1\n" + inner).hexdigest()


# ---- the gateway, as a client sees it ---------------------------------------------------------

def post(gateway: str, key: str, body: bytes) -> tuple[int, dict[str, str], str]:
    request = urllib.request.Request(
        f"{gateway}/v1/chat/stream",
        data=body,
        headers={"x-api-key": key, "content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, {k.lower(): v for k, v in response.headers.items()}, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, {k.lower(): v for k, v in error.headers.items()}, error.read().decode()


def chat_body(model: str) -> bytes:
    return json.dumps(
        {"model": model, "stream": True, "messages": [{"role": "user", "content": PROMPT}], "params": {"max_tokens": 64}},
        separators=(",", ":"),
    ).encode()


def parse_sse(text: str) -> tuple[str, str, dict[int, dict[str, str]], list[str]]:
    """Rebuild what the client received: content, reasoning, tool calls, event names."""
    content, reasoning, tools, events = [], [], {}, []
    for block in text.replace("\r\n", "\n").split("\n\n"):
        name, data = None, []
        for line in block.split("\n"):
            if line.startswith("event:"):
                name = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].lstrip())
        if not data:
            continue
        events.append(name or "token")
        payload = "\n".join(data)
        if payload == "[DONE]":
            continue
        value = json.loads(payload)
        if name is None and "token" in value:
            content.append(value["token"])
        elif name == "reasoning":
            reasoning.append(value["reasoning"])
        elif name == "tool":
            call = tools.setdefault(value["index"], {"id": "", "name": "", "arguments": ""})
            if not call["id"] and value.get("id"):
                call["id"] = value["id"]
            call["name"] += value.get("name") or ""
            call["arguments"] += value.get("arguments") or ""
    return "".join(content), "".join(reasoning), tools, events


# ---- the log, as an auditor sees it -----------------------------------------------------------

def read_log(directory: Path) -> list[dict]:
    path = directory / "receipts.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def anchor_count(path: Path) -> int:
    if not path.exists():
        return 0
    counts = [json.loads(line)["count"] for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return max(counts, default=0)


def wait_for(description: str, condition, seconds: float = 20.0) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.2)
    raise Failed(f"timed out waiting for {description}")


def ickverify(verifier: Path, log: Path, keys: Path, anchor: Path | None) -> tuple[int, str]:
    command = [sys.executable, str(verifier), str(log), "--trusted-keys", str(keys), "--require-signatures"]
    if anchor is not None:
        command += ["--anchor", str(anchor)]
    run = subprocess.run(command, capture_output=True, text=True)
    return run.returncode, (run.stdout + run.stderr).strip()


def first_lines(text: str, n: int = 3) -> None:
    for line in text.splitlines()[:n]:
        say(paint("2", line))


# ---- the walkthrough --------------------------------------------------------------------------

def run(args: argparse.Namespace) -> None:
    gateway = args.gateway.rstrip("/")
    replica = args.gateway_dir
    published = args.published_dir
    demo_key = os.environ["RECEIPTS_DEMO_KEY"]
    shoestring_key = os.environ["RECEIPTS_SHOESTRING_KEY"]

    step("Waiting for the gateway")
    def ready() -> bool:
        try:
            with urllib.request.urlopen(f"{gateway}/health/ready", timeout=2) as response:
                return response.status == 200
        except OSError:
            return False
    wait_for("the gateway to report ready", ready, 60)
    ok("ready")

    step("1. A streamed answer gets an allow receipt and an outcome")
    body = chat_body("mock/chat")
    status, headers, text = post(gateway, demo_key, body)
    check(status == 200, f"HTTP {status}")
    content, reasoning, tools, events = parse_sse(text)
    say(f"answer: {content[:70]}...")
    check("end" in events, "the stream ended cleanly with an `end` event")
    receipt_id = headers.get("x-receipt-id", "")
    salt_hex = headers.get("x-receipt-salt", "")
    check(receipt_id.startswith("receipt:sha3-256:"), f"x-receipt-id: {receipt_id[:40]}...")
    check(len(salt_hex) == 64, "x-receipt-salt: 32 random bytes, given only to this caller")
    salt = bytes.fromhex(salt_hex)

    log = read_log(replica)
    by_id = {entry["receipt_id"]: entry for entry in log}
    allow = by_id.get(receipt_id)
    check(allow is not None and allow.get("verdict") == "allow", "the log has that allow receipt, signed by this replica")
    outcome = next((e for e in log if e.get("decision_receipt_id") == receipt_id), None)
    check(outcome is not None and outcome["status"] == "completed", "and an outcome that closes it: completed")

    step("2. The client can prove which request and reply the receipts are about")
    mine = request_digest(salt, body)
    check(allow.get("call_digest") == mine, "request digest recomputed from the bytes sent matches the allow")
    check(outcome["request_sha256"] == mine, "and the outcome")
    theirs = reply_digest(salt, content, reasoning, tools)
    check(outcome["response_sha256"] == theirs, "reply digest recomputed from the stream received matches the outcome")
    raw = (replica / "receipts.jsonl").read_text(encoding="utf-8")
    check(PROMPT not in raw and ANSWER_FRAGMENT not in raw, "the log holds neither the prompt nor the answer")
    plain = "sha256:" + hashlib.sha256(body).hexdigest()
    check(plain not in raw, "nor a plain hash of the prompt: without the salt, a guess cannot be tested against it")

    step("3. A refusal is recorded with its reason")
    before = len(read_log(replica))
    refusal = None
    for attempt in range(1, 30):
        status, _, text = post(gateway, shoestring_key, chat_body("mock/chat"))
        if status == 402:
            refusal = json.loads(text)["error"]["code"]
            say(f"request {attempt}: 402 {refusal}")
            break
        check(status == 200, f"request {attempt}: 200 (within budget)")
    check(refusal == "budget_exhausted", "the tiny budget ran out: 402 before the model was called")
    newest = read_log(replica)[-1]
    check(newest.get("verdict") == "deny", "the newest receipt is a deny")
    check(newest.get("reason_codes") == ["EXTERNAL_DENY", "BUDGET_EXHAUSTED"],
          f"reason_codes {newest.get('reason_codes')}: decided by the gateway, and why")
    say(f"{len(read_log(replica)) - before} receipts for this tenant's requests, refusal included")

    step("4. A request that needs a person waits, and that is recorded too")
    status, headers, text = post(gateway, demo_key, chat_body("mock/approval"))
    check(status == 202, f"HTTP {status}: held for approval, model not called")
    newest = read_log(replica)[-1]
    check(newest.get("verdict") == "await_human_approval", "the newest receipt is await_human_approval")

    step("5. A failure part-way through is recorded with exactly what the client got")
    status, headers, text = post(gateway, demo_key, chat_body("mock/broken"))
    check(status == 200, f"HTTP {status}: the stream started")
    content, reasoning, tools, events = parse_sse(text)
    check("error" in events, f"then an `error` event after: {content!r}")
    salt = bytes.fromhex(headers["x-receipt-salt"])
    log = read_log(replica)
    outcome = next((e for e in log if e.get("decision_receipt_id") == headers["x-receipt-id"]), None)
    check(outcome is not None and outcome["status"] == "failed", "its outcome is `failed`")
    check(outcome["response_sha256"] == reply_digest(salt, content, reasoning, tools),
          "and its reply digest matches the partial answer the client received")

    step("6. The anchor leaves the gateway")
    total = len(read_log(replica))
    wait_for("the publisher to copy an anchor covering every entry",
             lambda: anchor_count(published / "anchor.jsonl") >= total and (published / "signing.pub").exists())
    check(True, f"published anchor covers all {total} entries; the public key was copied once")
    say("in production this copy lives where the gateway has no write access (a separate account or repository)")

    step("7. An auditor checks the log with the unchanged Wicket verifier")
    keys = published / "signing.pub"
    code, report = ickverify(args.verifier, replica / "receipts.jsonl", keys, published / "anchor.jsonl")
    first_lines(report)
    check(code == 0, "VERIFIED against the published anchor and key")

    work = Path(tempfile.mkdtemp(prefix="receipts-audit-"))
    try:
        step("8. Editing a refusal into an allow is caught")
        raw_lines = (replica / "receipts.jsonl").read_text(encoding="utf-8").splitlines()
        index = max(i for i, line in enumerate(raw_lines) if '"verdict":"deny"' in line)
        edited = list(raw_lines)
        edited[index] = edited[index].replace('"verdict":"deny"', '"verdict":"allow"')
        (work / "edited.jsonl").write_text("\n".join(edited) + "\n", encoding="utf-8")
        code, report = ickverify(args.verifier, work / "edited.jsonl", keys, published / "anchor.jsonl")
        first_lines(report, 2)
        check(code == 1, f"NOT VERIFIED: entry {index + 1} no longer matches its id")

        step("9. Quietly deleting the newest receipts")
        keep = len(raw_lines) - 3
        (work / "rolled-back.jsonl").write_text("\n".join(raw_lines[:keep]) + "\n", encoding="utf-8")
        local = [line for line in (replica / "anchor.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        doctored = [line for line in local if json.loads(line)["count"] <= keep]
        (work / "doctored-anchor.jsonl").write_text("".join(line + "\n" for line in doctored), encoding="utf-8")
        say(f"someone with the gateway's disk drops the last 3 of {len(raw_lines)} entries, and the local anchors that mention them")
        code, report = ickverify(args.verifier, work / "rolled-back.jsonl", keys,
                                 work / "doctored-anchor.jsonl" if doctored else None)
        first_lines(report, 1)
        check(code == 0, "against the gateway's own (doctored) files it still verifies: the deletion is invisible there")
        code, report = ickverify(args.verifier, work / "rolled-back.jsonl", keys, published / "anchor.jsonl")
        first_lines(report, 2)
        check(code == 1, "against the published anchor: NOT VERIFIED, the log is shorter than what was anchored")
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print(f"\n{paint('1;32', 'All checks passed.')}")
    print("What this shows: every model call, refusal, hold and failure left a signed, chained receipt; the")
    print("caller can prove which request and reply each one covers; edits are caught; and deleting the newest entries is")
    print("caught as long as the anchor was copied somewhere the gateway cannot write.")
    print("What it does not show: that the model's answer was correct, or anything the gateway was never asked.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--gateway", default="http://gateway:8080")
    parser.add_argument("--gateway-dir", type=Path, required=True)
    parser.add_argument("--published-dir", type=Path, required=True)
    parser.add_argument("--verifier", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        run(args)
    except Failed as failure:
        print(f"   {paint('31', 'FAIL')} {failure}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
