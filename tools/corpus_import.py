"""Convert a ChatGPT or Claude export into replay JSONL.

ChatGPT's `conversations.json` is a list of trees (`mapping` plus
`current_node`). Claude's export is a list of conversations, or
`{"conversations": [...]}`, each with `chat_messages`. One JSONL file is
written per conversation:

    {"role": "user"|"assistant"|"system", "content": "..."}

The default output directory is `corpus/`, which is gitignored. Raw exports
are never committed. A file that contains a key-like string is refused, by
the same scanner the replay uses, and nothing is written.

    python tools/corpus_import.py conversations.json --out corpus
    python tools/corpus_import.py claude.json --format claude --out corpus
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
from typing import Any

from corpus_replay import assert_no_key_like, write_jsonl
import fold_retention as fr


def message_text_chatgpt(message: dict[str, Any]) -> str | None:
    content = message.get("content")
    if isinstance(content, str):
        text = content.strip()
        return text or None
    if not isinstance(content, dict):
        return None
    parts = content.get("parts")
    if not isinstance(parts, list):
        return None
    text = "\n".join(part for part in parts if isinstance(part, str)).strip()
    return text or None


def chatgpt_messages(conversation: dict[str, Any]) -> list[dict[str, str]]:
    """The current branch only. A side branch off an earlier node is left out."""
    mapping = conversation.get("mapping")
    if not isinstance(mapping, dict):
        return []
    node_id = conversation.get("current_node")
    if not isinstance(node_id, str):
        parents = {
            node.get("parent")
            for node in mapping.values()
            if isinstance(node, dict) and isinstance(node.get("parent"), str)
        }
        leaves = [node_id for node_id in mapping if node_id not in parents]
        node_id = leaves[-1] if leaves else None
    chain: list[dict[str, Any]] = []
    seen: set[str] = set()
    while isinstance(node_id, str) and node_id not in seen and node_id in mapping:
        seen.add(node_id)
        node = mapping[node_id]
        node = node if isinstance(node, dict) else {}
        chain.append(node)
        parent = node.get("parent")
        node_id = parent if isinstance(parent, str) else None
    messages: list[dict[str, str]] = []
    for node in reversed(chain):
        message = node.get("message")
        if not isinstance(message, dict):
            continue
        author = message.get("author") if isinstance(message.get("author"), dict) else {}
        role = author.get("role")
        if role not in ("user", "assistant", "system"):
            continue
        text = message_text_chatgpt(message)
        if text:
            messages.append({"role": role, "content": text})
    return messages


def claude_text(message: dict[str, Any]) -> str | None:
    text = message.get("text")
    if isinstance(text, str) and text.strip():
        return text
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict) and isinstance(part.get("text"), str):
                parts.append(part["text"])
        joined = "\n".join(parts).strip()
        return joined or None
    return None


_CLAUDE_ROLES = {"human": "user", "user": "user", "assistant": "assistant", "system": "system"}


def claude_messages(conversation: dict[str, Any]) -> list[dict[str, str]]:
    raw = conversation.get("chat_messages")
    if not isinstance(raw, list):
        return []
    messages: list[dict[str, str]] = []
    for message in raw:
        if not isinstance(message, dict):
            continue
        role = _CLAUDE_ROLES.get(message.get("sender") or message.get("role"))
        text = claude_text(message)
        if role and text:
            messages.append({"role": role, "content": text})
    return messages


def detect_format(payload: Any) -> str:
    sample: Any = payload
    if isinstance(payload, dict):
        if "mapping" in payload:
            return "chatgpt"
        if "chat_messages" in payload:
            return "claude"
        if isinstance(payload.get("conversations"), list):
            sample = payload["conversations"]
    if isinstance(sample, list) and sample and isinstance(sample[0], dict):
        if "mapping" in sample[0]:
            return "chatgpt"
        if "chat_messages" in sample[0]:
            return "claude"
    raise fr.BenchmarkError("unrecognized export: expected ChatGPT mapping or Claude chat_messages")


def _items(payload: Any, fmt: str) -> list[dict[str, Any]]:
    if fmt == "chatgpt":
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict) and "mapping" in payload:
            return [payload]
        raise fr.BenchmarkError("ChatGPT export is not a conversation list")
    if isinstance(payload, dict) and isinstance(payload.get("conversations"), list):
        return [item for item in payload["conversations"] if isinstance(item, dict)]
    if isinstance(payload, dict) and "chat_messages" in payload:
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    raise fr.BenchmarkError("Claude export is not a conversation list")


def _slug(title: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "-", title).strip("-").lower()[:40]
    return cleaned or "conversation"


def import_export(path: Path, fmt: str, out_dir: Path) -> list[Path]:
    """Write one JSONL per conversation. Refuses a key-like export before writing any file."""
    text = path.read_text(encoding="utf-8-sig")
    assert_no_key_like(text, path.name)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        raise fr.BenchmarkError(f"{path.name} is not JSON") from None
    chosen = detect_format(payload) if fmt == "auto" else fmt
    convert = chatgpt_messages if chosen == "chatgpt" else claude_messages
    written: list[Path] = []
    out_dir.mkdir(parents=True, exist_ok=True)
    index = 0
    for conversation in _items(payload, chosen):
        messages = convert(conversation)
        if not messages:
            continue
        index += 1
        title = conversation.get("title") or conversation.get("name") or "conversation"
        title = title if isinstance(title, str) else "conversation"
        dest = out_dir / f"{index:04d}-{_slug(title)}.jsonl"
        write_jsonl(dest, messages)
        written.append(dest)
    if not written:
        raise fr.BenchmarkError("export contained no conversations")
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("export", type=Path, help="ChatGPT conversations.json or a Claude export")
    parser.add_argument("--format", choices=["auto", "chatgpt", "claude"], default="auto")
    parser.add_argument("--out", type=Path, default=Path("corpus"),
                        help="directory for JSONL conversations (default: corpus/, gitignored)")
    args = parser.parse_args(argv)
    try:
        written = import_export(args.export, args.format, args.out)
    except fr.BenchmarkError as exc:
        print(f"corpus-import: {exc}", file=sys.stderr)
        return 2
    print(f"corpus-import: wrote {len(written)} conversations into {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
