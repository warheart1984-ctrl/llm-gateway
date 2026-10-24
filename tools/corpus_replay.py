"""Replay real conversations through the folding retention harness.

Each file in `--corpus` is one conversation as JSONL, one message per line:
`{"role": "user"|"assistant"|"system", "content": "..."}`. Recorded user
turns are sent to both tenants. Recorded assistant turns are not sent as
model output; each tenant regenerates its own reply, and that reply is what
later turns of that tenant see. Calls use the same model, `temperature` 0,
alternating which arm goes first, caching-aware prices, and an
`x-request-id` on every chat call.

`--extract-facts` sends the full recorded transcript once to the unfolded
tenant (the bench model, gpt-4.1-mini on OpenRouter) and writes
`<conversation>.facts.json`. A file that already exists is the answer key:
it is read and never regenerated, so a hand edit sticks. `--probe-every N`
asks, at every Nth user turn, only the fact questions whose `source_turn`
has been folded away. A fact counts only when the unfolded tenant got it
right, scored with the harness's strict typed equality.

The corpus directory is gitignored (`/corpus/`). Raw exports stay there.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import uuid
from typing import Any, Callable

import fold_retention as fr


DEFAULT_PROBE_EVERY = 10

# Same rules as `redact_secrets` in src/observability/logging.rs: a token of
# ASCII letters, digits, `-` and `_` is key-like when it starts with a known
# prefix, follows `Bearer`, or is a long opaque mix of letters and digits.
# A hyphenated UUID (36) stays readable, matching that scanner's tests.
KEY_PREFIXES = ("sk-", "sk_", "nvapi-", "gsk_", "gwk_", "xai-", "pk-", "rk-", "AIza", "eyJ")
OPAQUE_MIN = 40

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
URL_RE = re.compile(r"(?:https?://|www\.)[^\s<>\"']+", re.IGNORECASE)
PHONE_RE = re.compile(
    r"(?<![\w/])(?:"
    r"\+\d{1,3}[\s.\-]?\(?\d{1,4}\)?(?:[\s.\-]?\d{2,4}){2,3}"
    r"|\(\d{3}\)[\s.\-]?\d{3}[\s.\-]?\d{4}"
    r"|\d{3}[\s.\-]\d{3}[\s.\-]\d{4}"
    r")(?!\d)"
)
ROLES = {"user", "assistant", "system"}
# Fold decisions that mean "nothing was attempted", not a refusal.
NOT_AN_ATTEMPT = {"no_head", "not_enough_new_turns", "max_folds", "shutdown"}


def _token_char(char: str) -> bool:
    return char.isascii() and (char.isalnum() or char in "-_")


def key_like_kind(text: str) -> str | None:
    """`prefix`, `bearer` or `opaque` when `text` contains a key-like token.

    The return value names the rule that matched. It never includes the token.
    """
    after_bearer = False
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if after_bearer and not char.isspace():
            end = index
            while end < length and not text[end].isspace() and text[end] not in "\"',}]":
                end += 1
            if end > index:
                return "bearer"
            after_bearer = False
            continue
        if not _token_char(char):
            index += 1
            continue
        end = index
        while end < length and _token_char(text[end]):
            end += 1
        token = text[index:end]
        opaque = (
            len(token) >= OPAQUE_MIN
            and any(item.isascii() and item.isdigit() for item in token)
            and any(item.isascii() and item.isalpha() for item in token)
        )
        keyed = any(len(token) > len(prefix) + 8 and token.startswith(prefix) for prefix in KEY_PREFIXES)
        if keyed:
            return "prefix"
        if opaque:
            return "opaque"
        after_bearer = token.lower() == "bearer"
        index = end
    return None


def assert_no_key_like(text: str, where: str) -> None:
    kind = key_like_kind(text)
    if kind:
        raise fr.BenchmarkError(f"refusing to run: {where} contains a {kind} key-like string")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _text_to_scan(report: dict[str, Any]) -> str:
    """Serialized report, minus hashes the harness records that are not credentials.

    A git commit is 40 hex characters and a fold decision's prefix hash is 64.
    Both match the opaque-token rule. They are identifiers, not keys, so they
    are blanked before the scan. Every other string is still checked.
    """
    cloned = json.loads(json.dumps(report))

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if "git_commit" in node:
                node["git_commit"] = None
            if "prefix_hash" in node:
                node["prefix_hash"] = ""
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(cloned)
    return json.dumps(cloned)


def write_json(path: Path, report: dict[str, Any]) -> None:
    """Write a report, or refuse when the serialized text looks like it holds a key."""
    text = json.dumps(report, indent=2, ensure_ascii=False)
    kind = key_like_kind(_text_to_scan(report))
    if kind:
        raise fr.BenchmarkError(f"refusing to write {path.name}: it contains a {kind} key-like string")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text + "\n", encoding="utf-8")


class ScrubMemo:
    """Stable stand-ins for one conversation: the same address keeps the same token."""

    def __init__(self) -> None:
        self._emails: dict[str, str] = {}
        self._urls: dict[str, str] = {}
        self._phones: dict[str, str] = {}

    def scrub(self, text: str) -> str:
        text = self._replace(EMAIL_RE, text, self._emails, "EMAIL")
        text = self._replace(URL_RE, text, self._urls, "URL", trim=".,;:!?)]}")
        text = self._replace(PHONE_RE, text, self._phones, "PHONE")
        return text

    def _replace(
        self,
        pattern: re.Pattern[str],
        text: str,
        store: dict[str, str],
        label: str,
        trim: str = "",
    ) -> str:
        def token(match: re.Match[str]) -> str:
            value = match.group(0)
            suffix = ""
            if trim:
                stripped = value.rstrip(trim)
                suffix = value[len(stripped):]
                value = stripped
            if value not in store:
                store[value] = f"[{label}_{len(store) + 1}]"
            return store[value] + suffix

        return pattern.sub(token, text)


def scrub_text(text: str) -> str:
    return ScrubMemo().scrub(text)


def facts_path(conversation: Path) -> Path:
    return conversation.with_name(conversation.stem + ".facts.json")


def load_jsonl_conversation(path: Path) -> list[dict[str, str]]:
    """One conversation. Blank lines are ignored. Anything else must be a message."""
    messages: list[dict[str, str]] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            raise fr.BenchmarkError(f"{path.name}:{line_no} is not JSON") from None
        role = obj.get("role") if isinstance(obj, dict) else None
        content = obj.get("content") if isinstance(obj, dict) else None
        if role not in ROLES or not isinstance(content, str):
            raise fr.BenchmarkError(
                f"{path.name}:{line_no} must have role user|assistant|system and string content"
            )
        messages.append({"role": role, "content": content})
    if not messages:
        raise fr.BenchmarkError(f"{path.name} has no messages")
    return messages


def write_jsonl(path: Path, messages: list[dict[str, str]]) -> None:
    lines = [
        json.dumps({"role": message["role"], "content": message["content"]}, ensure_ascii=False)
        for message in messages
    ]
    text = "\n".join(lines) + "\n"
    assert_no_key_like(text, path.name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def scrub_corpus(directory: Path) -> None:
    """Rewrite each conversation, and its fact file, with email, phone and URL tokens.

    One memo covers a conversation and its facts, so a value and the question
    that names it keep the same stand-in.
    """
    for path in conversation_files(directory):
        memo = ScrubMemo()
        messages = load_jsonl_conversation(path)
        for message in messages:
            message["content"] = memo.scrub(message["content"])
        write_jsonl(path, messages)
        fact_file = facts_path(path)
        if not fact_file.exists():
            continue
        raw = json.loads(fact_file.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise fr.BenchmarkError(f"{fact_file.name} is not a list of facts")
        for fact in raw:
            if not isinstance(fact, dict):
                continue
            for key in ("field", "question"):
                if isinstance(fact.get(key), str):
                    fact[key] = memo.scrub(fact[key])
            if isinstance(fact.get("value"), str):
                fact["value"] = memo.scrub(fact["value"])
        text = json.dumps(raw, indent=2, ensure_ascii=False) + "\n"
        assert_no_key_like(text, fact_file.name)
        fact_file.write_text(text, encoding="utf-8")


def conversation_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        raise fr.BenchmarkError("corpus directory does not exist")
    return sorted(path for path in directory.iterdir() if path.is_file() and path.suffix == ".jsonl")


def assert_corpus_clean(directory: Path) -> None:
    """Refuse before any model call when a file in the corpus looks like it holds a key."""
    files = [path for path in directory.iterdir() if path.is_file()] if directory.is_dir() else []
    if not files:
        raise fr.BenchmarkError("corpus directory is empty")
    for path in sorted(files):
        assert_no_key_like(read_text(path), path.name)


def normalize_fact(fact: Any, where: str = "facts") -> dict[str, Any]:
    if not isinstance(fact, dict):
        raise fr.BenchmarkError(f"{where} has an entry that is not an object")
    field = fact.get("field")
    if not isinstance(field, str) or not field.strip() or any(char in field for char in "\r\n"):
        raise fr.BenchmarkError(f"{where} has a fact with no field name")
    value = fact.get("value")
    if isinstance(value, (dict, list)) or not isinstance(value, (str, int, float, bool, type(None))):
        raise fr.BenchmarkError(f"{where} has a fact whose value is not a scalar")
    source = fact.get("source_turn")
    if isinstance(source, bool) or not isinstance(source, int) or source < 0:
        raise fr.BenchmarkError(f"{where} has a fact with a bad source_turn")
    question = fact.get("question")
    if not isinstance(question, str) or not question.strip():
        raise fr.BenchmarkError(f"{where} has a fact with no question")
    return {"field": field, "value": value, "source_turn": source, "question": question}


def latest_facts(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One entry per field: the one with the greatest source_turn, in first-seen order."""
    best: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for fact in facts:
        field = fact["field"]
        if field not in best:
            order.append(field)
            best[field] = fact
            continue
        if fact["source_turn"] >= best[field]["source_turn"]:
            best[field] = fact
    return [best[field] for field in order]


def load_facts(path: Path) -> list[dict[str, Any]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        raise fr.BenchmarkError(f"{path.name} is not JSON") from None
    if not isinstance(raw, list):
        raise fr.BenchmarkError(f"{path.name} is not a list of facts")
    return latest_facts([normalize_fact(item, path.name) for item in raw])


def write_facts(path: Path, facts: list[dict[str, Any]]) -> None:
    text = json.dumps(latest_facts(facts), indent=2, ensure_ascii=False) + "\n"
    assert_no_key_like(text, path.name)
    path.write_text(text, encoding="utf-8")


def extract_with(
    conversation: Path,
    messages: list[dict[str, str]],
    extract: bool,
    extractor: Callable[[list[dict[str, str]]], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Read `<conversation>.facts.json` when it exists; otherwise call `extractor` once and write it."""
    path = facts_path(conversation)
    if path.exists():
        return load_facts(path)
    if not extract:
        raise fr.BenchmarkError(f"{path.name} is missing; rerun with --extract-facts and review it")
    facts = latest_facts([normalize_fact(fact) for fact in extractor(messages)])
    write_facts(path, facts)
    return facts


def checkpoint_user_turns(user_turns: int, probe_every: int) -> list[int]:
    """1-based user-turn numbers at which to probe. Every `probe_every` turns, and no others."""
    if probe_every <= 0:
        raise fr.BenchmarkError("--probe-every must be positive")
    if user_turns < 0:
        raise fr.BenchmarkError("user_turns must be non-negative")
    return list(range(probe_every, user_turns + 1, probe_every))


def facts_folded_away(facts: list[dict[str, Any]], folded_turns: int) -> list[dict[str, Any]]:
    """Facts whose source turn lies in the prefix the summary replaced.

    `source_turn` is the 1-based user/assistant turn, the same numbering the
    gateway folds. Zero marks a fact that came from a system message, which
    is never replaced.
    """
    return [fact for fact in facts if fact["source_turn"] > 0 and folded_turns >= fact["source_turn"]]


def json_type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if value is None:
        return "null"
    return "string"


def probe_question(facts: list[dict[str, Any]]) -> str:
    lines = [
        "Answer factual questions about the conversation. Use the latest explicit correction for current facts.",
        "Return only a JSON object with exactly the keys listed below, and no explanation or Markdown.",
        "Use JSON null for information that has never been supplied. Do not guess.",
    ]
    for fact in facts:
        lines.append(f"- {fact['field']} ({json_type_name(fact['value'])}): {fact['question']}")
    return "\n".join(lines)


def score_checkpoint(
    facts: list[dict[str, Any]],
    folded_turns: int,
    plain_content: Any,
    folded_content: Any,
    plain_finish: str | None,
    folded_finish: str | None,
) -> dict[str, Any]:
    """Score only facts folded away. A fact is eligible only when the unfolded arm is right."""
    away = facts_folded_away(facts, folded_turns)
    expected = {fact["field"]: fact["value"] for fact in away}
    plain_score = fr.score_answer(plain_content, expected, plain_finish)
    folded_score = fr.score_answer(folded_content, expected, folded_finish)
    fields: dict[str, Any] = {}
    wrong: list[dict[str, Any]] = []
    for fact in away:
        key = fact["field"]
        plain_ok = plain_score["fields"].get(key, False)
        folded_ok = folded_score["fields"].get(key, False)
        eligible = fr.retention_eligible(fact["source_turn"], folded_turns, plain_ok)
        field: dict[str, Any] = {
            "expected": fact["value"],
            "introduced_at": fact["source_turn"],
            "eligible": eligible,
            "plain_correct": plain_ok,
            "folded_correct": folded_ok,
            "plain_containment_correct": plain_score["fields_containment"].get(key, False),
            "folded_containment_correct": folded_score["fields_containment"].get(key, False),
        }
        if not plain_ok:
            field["plain_answer"] = fr.answered(plain_score, key)
        if not folded_ok:
            field["folded_answer"] = fr.answered(folded_score, key)
        fields[key] = field
        if not plain_ok or not folded_ok:
            wrong.append({
                "field": key,
                "expected": fact["value"],
                "plain_text": plain_content if isinstance(plain_content, str) else None,
                "folded_text": folded_content if isinstance(folded_content, str) else None,
                "plain_answer": fr.answered(plain_score, key),
                "folded_answer": fr.answered(folded_score, key),
                "eligible": eligible,
            })
    return {
        "fields": fields,
        "wrong_answers": wrong,
        "plain_score": plain_score,
        "folded_score": folded_score,
    }


def _main_prices(prices: dict[str, float | None]) -> tuple[float, float, float] | None:
    names = ("main_input_per_mtok_usd", "main_cached_input_per_mtok_usd", "main_output_per_mtok_usd")
    if any(prices.get(name) is None for name in names):
        return None
    return (prices["main_input_per_mtok_usd"], prices["main_cached_input_per_mtok_usd"], prices["main_output_per_mtok_usd"])


def _is_backoff(row: dict[str, Any]) -> bool:
    folded = row["folded"]
    outcomes = folded.get("fold_outcomes") or {}
    if outcomes.get("backoff"):
        return True
    decision = folded.get("fold_decision")
    return isinstance(decision, dict) and (decision.get("reason") == "backoff" or decision.get("outcome") == "backoff")


def fold_tally(rows: list[dict[str, Any]], prices: dict[str, float | None] | None = None) -> dict[str, Any]:
    """Attempted, applied and refused folds, with reasons, and what backoff calls cost.

    Backoff cost is the folded tenant's ledger spend on calls the gateway
    skipped because an earlier rejection was still holding that conversation
    off. `backoff_cost_usd` is those calls priced the way the provider bills
    a prompt cache, when the main-model prices are known.
    """
    prices = prices or {}
    attempted = 0
    applied = 0
    refused: dict[str, int] = {}
    reasons: dict[str, int] = {}
    backoff_cost = 0
    backoff_rows: list[dict[str, Any]] = []

    def add_refused(outcome: str, reason: str, count: int = 1) -> None:
        refused[outcome] = refused.get(outcome, 0) + count
        reasons[reason] = reasons.get(reason, 0) + count

    for row in rows:
        folded = row["folded"]
        outcomes = {name: count for name, count in (folded.get("fold_outcomes") or {}).items() if count}
        decision = folded.get("fold_decision") if isinstance(folded.get("fold_decision"), dict) else None
        if outcomes:
            reason = str(decision.get("reason")) if decision and len(outcomes) == 1 and decision.get("reason") else None
            for name, count in outcomes.items():
                if name == "committed":
                    attempted += count
                    applied += count
                elif name == "backoff":
                    add_refused("backoff", reason or "backoff", count)
                else:
                    attempted += count
                    add_refused(name, reason or name, count)
        elif decision:
            outcome = str(decision.get("outcome") or "")
            reason = str(decision.get("reason") or outcome or "unknown")
            if outcome in ("applied", "committed"):
                attempted += 1
                applied += 1
            elif reason == "backoff" or outcome == "backoff":
                add_refused("backoff", reason)
            elif reason in NOT_AN_ATTEMPT or outcome == "not_due":
                continue
            else:
                attempted += 1
                add_refused(reason, reason)
        if _is_backoff(row):
            backoff_cost += folded.get("spend_delta_nano_usd") or 0
            backoff_rows.append(row)
    rates = _main_prices(prices)
    backoff_usd = None
    if rates is not None and backoff_rows:
        backoff_usd = sum(fr.provider_cost_usd(row["folded"]["usage"], *rates) for row in backoff_rows)
    elif rates is not None:
        backoff_usd = 0.0
    return {
        "attempted": attempted,
        "applied": applied,
        "refused": dict(sorted(refused.items())),
        "reasons": dict(sorted(reasons.items())),
        "backoff_cost_nano_usd": backoff_cost,
        "backoff_cost_usd": backoff_usd,
    }


def conversation_totals(rows: list[dict[str, Any]], prices: dict[str, float | None] | None = None) -> dict[str, Any]:
    prices = prices or {}
    eligible = [field for row in rows for field in row["fields"].values() if field.get("eligible")]
    wrong: list[dict[str, Any]] = []
    for row in rows:
        for item in row.get("wrong_answers") or []:
            entry = dict(item)
            entry["call"] = row.get("call")
            entry["conversation"] = row.get("conversation")
            wrong.append(entry)
    return {
        "facts_kept": {
            "correct": sum(1 for field in eligible if field.get("folded_correct")),
            "eligible": len(eligible),
        },
        "saving_with_caching": fr._caching_aggregates(rows, prices).get("provider_cost_usd") if rows else None,
        "folds": fold_tally(rows, prices),
        "wrong_answers": wrong,
    }


def transcript(messages: list[dict[str, str]]) -> str:
    """Number user and assistant turns the way `source_turn` is counted. System text is not a turn."""
    lines: list[str] = []
    turn = 0
    for message in messages:
        if message["role"] == "system":
            lines.append(f"System (not a turn): {message['content']}")
            continue
        turn += 1
        lines.append(f"Turn {turn} ({message['role']}): {message['content']}")
    return "\n".join(lines)


EXTRACT_INSTRUCTIONS = (
    "You extract facts a later question could check. Return only a JSON object "
    '{"facts":[{"field":string,"value":string|number|boolean|null,"source_turn":integer,"question":string}]}. '
    "field is a short snake_case name. value is the latest explicit value, with JSON types "
    "(integers as numbers, booleans as true or false, not strings). "
    "source_turn is the 1-based turn number printed beside the message that states that latest value. "
    "Turns are user and assistant messages; system messages are not turns and use source_turn 0 when a fact comes only from one. "
    "question is one question whose answer is that value. "
    "Omit small talk. Do not guess. Keep only one entry per field, the latest."
)


def parse_facts(content: str) -> list[dict[str, Any]]:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[A-Za-z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        starts = [pos for pos in (text.find("{"), text.find("[")) if pos >= 0]
        if not starts:
            raise fr.BenchmarkError("fact extraction was not JSON") from None
        try:
            payload = json.loads(text[min(starts):])
        except json.JSONDecodeError:
            raise fr.BenchmarkError("fact extraction was not JSON") from None
    raw = payload.get("facts") if isinstance(payload, dict) else payload
    if not isinstance(raw, list):
        raise fr.BenchmarkError("fact extraction did not return a list")
    return latest_facts([normalize_fact(item, "fact extraction") for item in raw])


def chat_body(model: str, messages: list[dict[str, str]], max_tokens: int) -> dict[str, Any]:
    return {
        "model": model,
        "messages": messages,
        "temperature": 0,
        "max_tokens": max_tokens,
        "stream": False,
    }


def _blank_score() -> dict[str, Any]:
    return {
        "correct": 0,
        "total": 0,
        "correct_containment": 0,
        "error": None,
        "extra_keys": [],
        "fields": {},
        "fields_containment": {},
        "parsed": None,
    }


def _arm_view(result: dict[str, Any], score: dict[str, Any]) -> dict[str, Any]:
    return {
        "correct": score["correct"],
        "total": score["total"],
        "containment_correct": score["correct_containment"],
        "finish_reason": result["finish_reason"],
        "spend_delta_nano_usd": result["spend_delta_nano_usd"],
        "usage": result["usage"],
        "retries": result["retries"],
        "request_id": result["request_id"],
        "error": score["error"],
        "extra_keys": score["extra_keys"],
    }


def _folded_view(
    result: dict[str, Any],
    score: dict[str, Any],
    metrics_before: dict,
    metrics_after: dict,
) -> dict[str, Any]:
    view = _arm_view(result, score)
    view.update({
        "folded_turns": result["folded_turns"],
        "fold_outcomes": fr.fold_outcome_deltas(metrics_before, metrics_after),
        "failed_checks": fr.metric_deltas(metrics_before, metrics_after, "gw_fold_checks_failed_total", "check"),
        "corrective_retries": fr.metric_deltas(metrics_before, metrics_after, "gw_fold_corrective_retries_total", "result"),
        "fold_count": fr._metric(metrics_after, "gw_folds_total", {"tenant": "folded", "outcome": "committed"}),
        "summarizer_prompt_tokens": fr._metric(metrics_after, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "prompt"})
        - fr._metric(metrics_before, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "prompt"}),
        "summarizer_completion_tokens": fr._metric(metrics_after, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "completion"})
        - fr._metric(metrics_before, "gw_fold_summarizer_tokens_total", {"tenant": "folded", "kind": "completion"}),
    })
    return view


def _paired(
    *,
    base_url: str,
    keys: dict[str, str],
    bodies: dict[str, dict[str, Any]],
    timeout: float,
    call_number: int,
) -> tuple[bool, dict[str, dict[str, Any]], dict, dict]:
    plain_first = call_number % 2 == 1
    order = ("plain", "folded") if plain_first else ("folded", "plain")
    metrics_before = fr._metrics(base_url, keys["admin"], timeout)
    results: dict[str, dict[str, Any]] = {}
    for arm in order:
        results[arm] = fr._arm(
            base_url,
            keys[arm],
            bodies[arm],
            timeout,
            request_id=str(uuid.uuid4()),
        )
    metrics_after = fr._metrics(base_url, keys["admin"], timeout)
    return plain_first, results, metrics_before, metrics_after


def _assistant_text(result: dict[str, Any]) -> str:
    content = result.get("content")
    return content if isinstance(content, str) else ""


def _note_summary(summaries: list[dict[str, Any]], last: list[str | None], call: int, folded: dict[str, Any]) -> None:
    text = folded.get("fold_summary")
    if isinstance(text, str) and text != last[0]:
        last[0] = text
        summaries.append({
            "call": call,
            "folded_turns": folded["folded_turns"],
            "chars": len(text),
            "text": text,
        })


def replay_conversation(
    *,
    name: str,
    messages: list[dict[str, str]],
    facts: list[dict[str, Any]],
    base_url: str,
    keys: dict[str, str],
    model: str,
    max_tokens: int,
    timeout: float,
    probe_every: int,
    call_start: int,
    summaries: list[dict[str, Any]],
    last_summary: list[str | None],
) -> tuple[list[dict[str, Any]], int]:
    """Feed user turns, regenerate assistant turns per tenant, probe on the checkpoint turns."""
    histories: dict[str, list[dict[str, str]]] = {"plain": [], "folded": []}
    user_total = sum(1 for message in messages if message["role"] == "user")
    checkpoints = set(checkpoint_user_turns(user_total, probe_every))
    rows: list[dict[str, Any]] = []
    call = call_start
    user_turns = 0
    last_folded_turns = 0
    index = 0
    while index < len(messages):
        message = messages[index]
        if message["role"] == "system":
            for history in histories.values():
                history.append({"role": "system", "content": message["content"]})
            index += 1
            continue
        if message["role"] == "assistant":
            # The recorded reply is the export's model output. Replay does not send it.
            index += 1
            continue
        for history in histories.values():
            history.append({"role": "user", "content": message["content"]})
        user_turns += 1
        index += 1
        if index < len(messages) and messages[index]["role"] == "assistant":
            call += 1
            bodies = {arm: chat_body(model, list(history), max_tokens) for arm, history in histories.items()}
            plain_first, results, before, after = _paired(
                base_url=base_url, keys=keys, bodies=bodies, timeout=timeout, call_number=call,
            )
            for arm, result in results.items():
                histories[arm].append({"role": "assistant", "content": _assistant_text(result)})
            blank = _blank_score()
            row = {
                "call": call,
                "conversation": name,
                "kind": "turn",
                "user_turn": user_turns,
                "order": "plain-first" if plain_first else "folded-first",
                "plain": _arm_view(results["plain"], blank),
                "folded": _folded_view(results["folded"], blank, before, after),
                "fields": {},
                "wrong_answers": [],
            }
            rows.append(row)
            last_folded_turns = results["folded"]["folded_turns"]
            _note_summary(summaries, last_summary, call, results["folded"])
            index += 1
        if user_turns not in checkpoints:
            continue
        away = facts_folded_away(facts, last_folded_turns)
        if not away:
            continue
        call += 1
        question = probe_question(away)
        bodies = {
            arm: chat_body(model, [*history, {"role": "user", "content": question}], max_tokens)
            for arm, history in histories.items()
        }
        plain_first, results, before, after = _paired(
            base_url=base_url, keys=keys, bodies=bodies, timeout=timeout, call_number=call,
        )
        scored = score_checkpoint(
            away,
            results["folded"]["folded_turns"],
            results["plain"]["content"],
            results["folded"]["content"],
            results["plain"]["finish_reason"],
            results["folded"]["finish_reason"],
        )
        row = {
            "call": call,
            "conversation": name,
            "kind": "probe",
            "user_turn": user_turns,
            "order": "plain-first" if plain_first else "folded-first",
            "plain": _arm_view(results["plain"], scored["plain_score"]),
            "folded": _folded_view(results["folded"], scored["folded_score"], before, after),
            "fields": scored["fields"],
            "wrong_answers": scored["wrong_answers"],
        }
        rows.append(row)
        last_folded_turns = results["folded"]["folded_turns"]
        _note_summary(summaries, last_summary, call, results["folded"])
    return rows, call


def _prices_view(prices: dict[str, float | None] | None) -> dict[str, float | None]:
    return dict(prices or {})


def _aggregate(rows: list[dict[str, Any]], profile: dict[str, Any], prices: dict[str, float | None], final_metrics: dict) -> dict[str, Any]:
    eligible = [field for row in rows for field in row["fields"].values() if field.get("eligible")]
    plain_fields = [field for row in rows for field in row["fields"].values()]
    folded_correct = sum(1 for field in eligible if field.get("folded_correct"))
    plain_correct = sum(1 for field in plain_fields if field.get("plain_correct"))
    total = len(plain_fields)
    plain_spend = sum(row["plain"]["spend_delta_nano_usd"] for row in rows)
    folded_spend = sum(row["folded"]["spend_delta_nano_usd"] for row in rows)
    max_fold_count = max((row["folded"]["fold_count"] for row in rows), default=0)
    reached = "re-folded" if max_fold_count > profile["max_folds"] else (
        "frozen" if max_fold_count >= profile["max_folds"] else "warm"
    )
    tally = fold_tally(rows, prices)
    folds = {
        "committed": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "committed"}),
        "rejected": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "rejected"}),
        "failed": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "failed"}),
        "unprofitable": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "unprofitable"}),
        "refused_admission": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "refused_admission"}),
        "pinned_overflow": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "pinned_overflow"}),
        "backoff": fr._metric(final_metrics, "gw_folds_total", {"tenant": "folded", "outcome": "backoff"}),
        "failed_checks": fr.metric_deltas({}, final_metrics, "gw_fold_checks_failed_total", "check"),
        "corrective_retries": fr.metric_deltas({}, final_metrics, "gw_fold_corrective_retries_total", "result"),
    }
    folds.update(tally)
    return {
        "retention_rate": folded_correct / len(eligible) if eligible else None,
        "eligible_fields": len(eligible),
        "folded_correct_eligible_fields": folded_correct,
        "accuracy": {
            "plain": plain_correct / total if total else None,
            "folded": sum(1 for field in plain_fields if field.get("folded_correct")) / total if total else None,
        },
        "total_spend_nano_usd": {"plain": plain_spend, "folded": folded_spend},
        "net_saving_nano_usd": plain_spend - folded_spend,
        "folds": folds,
        "reached": reached,
        "caching": fr._caching_aggregates(rows, prices) if rows else {"cached_share": {"plain": None, "folded": None}, "cached_tokens": {"plain": 0, "folded": 0}, "provider_cost_usd": None},
    }


def run_corpus(
    *,
    base_url: str,
    plain_key: str,
    folded_key: str,
    admin_key: str,
    model: str,
    corpus: Path,
    profile: str,
    output: Path,
    probe_every: int = DEFAULT_PROBE_EVERY,
    extract_facts: bool = False,
    extract_only: bool = False,
    scrub: bool = False,
    max_tokens: int = 1024,
    timeout: float = 240.0,
    prices: dict[str, float | None] | None = None,
) -> tuple[dict[str, Any], int]:
    if profile not in fr.PROFILES:
        raise fr.BenchmarkError(f"unknown profile {profile!r}")
    if probe_every <= 0 or max_tokens <= 0 or timeout <= 0:
        raise fr.BenchmarkError("probe_every, max_tokens and timeout must be positive")
    prices = _prices_view(prices)
    profile_data = fr.PROFILES[profile]
    if scrub:
        scrub_corpus(corpus)
    assert_corpus_clean(corpus)
    if output.exists():
        assert_no_key_like(read_text(output), output.name)
    files = conversation_files(corpus)
    if not files:
        raise fr.BenchmarkError("corpus directory has no JSONL conversations")
    keys = {"plain": plain_key, "folded": folded_key, "admin": admin_key}
    started = datetime.now(timezone.utc).isoformat()

    def extractor(messages: list[dict[str, str]]) -> list[dict[str, Any]]:
        # One call, unfolded tenant, the whole recorded transcript. Folding is off
        # for this tenant, so the answer key is taken from the uncut conversation.
        body = chat_body(model, [
            {"role": "system", "content": EXTRACT_INSTRUCTIONS},
            {"role": "user", "content": transcript(messages)},
        ], max_tokens)
        result = fr._arm(base_url, plain_key, body, timeout, request_id=str(uuid.uuid4()))
        content = result.get("content")
        if not isinstance(content, str):
            raise fr.BenchmarkError("fact extraction returned no text")
        return parse_facts(content)

    prepared: list[tuple[Path, list[dict[str, str]], list[dict[str, Any]]]] = []
    for path in files:
        messages = load_jsonl_conversation(path)
        print(f"corpus: {path.name}: {'facts on disk' if facts_path(path).exists() else 'extracting facts'}", file=sys.stderr)
        facts = extract_with(path, messages, extract_facts, extractor)
        prepared.append((path, messages, facts))

    settings = {
        "model": model,
        "scenario": "corpus",
        "profile": profile,
        "label": profile_data["label"],
        "corpus": corpus.name,
        "probe_every": probe_every,
        "extract_facts": extract_facts,
        "scrubbed": scrub,
        "max_tokens": max_tokens,
        "temperature": 0,
        "fold_cap": profile_data["max_folds"],
        "refold_threshold": profile_data["refold_threshold"],
        "secrets_written": False,
        "git_commit": fr._git_commit(),
        "prices": prices,
    }
    if extract_only:
        report = {
            "schema": "folding.retention.v1",
            "started_utc": started,
            "finished_utc": datetime.now(timezone.utc).isoformat(),
            "settings": {**settings, "mode": "extract-only"},
            "conversations": [
                {"name": path.stem, "file": path.name, "facts": len(facts), "facts_file": facts_path(path).name}
                for path, _messages, facts in prepared
            ],
            "calls": [],
            "fold_summaries": [],
            "aggregates": _aggregate([], profile_data, prices, {}),
        }
        write_json(output, report)
        return report, 0

    rows: list[dict[str, Any]] = []
    conversations: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    last_summary: list[str | None] = [None]
    call = 0
    try:
        for path, messages, facts in prepared:
            print(f"corpus: {path.name}: replaying", file=sys.stderr)
            conv_rows, call = replay_conversation(
                name=path.stem,
                messages=messages,
                facts=facts,
                base_url=base_url,
                keys=keys,
                model=model,
                max_tokens=max_tokens,
                timeout=timeout,
                probe_every=probe_every,
                call_start=call,
                summaries=summaries,
                last_summary=last_summary,
            )
            rows.extend(conv_rows)
            totals = conversation_totals(conv_rows, prices)
            conversations.append({
                "name": path.stem,
                "file": path.name,
                "user_turns": sum(1 for message in messages if message["role"] == "user"),
                "facts": len(facts),
                "facts_file": facts_path(path).name,
                "totals": totals,
            })
            kept = totals["facts_kept"]
            print(
                f"corpus: {path.name}: kept {kept['correct']}/{kept['eligible']}",
                file=sys.stderr,
            )
        final_metrics = fr._metrics(base_url, admin_key, timeout) if rows else {}
    except fr.BenchmarkError as exc:
        report = {
            "schema": "folding.retention.v1",
            "started_utc": started,
            "finished_utc": datetime.now(timezone.utc).isoformat(),
            "settings": settings,
            "conversations": conversations,
            "calls": rows,
            "fold_summaries": summaries,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
        _emit(output, report)
        return report, 2

    report = {
        "schema": "folding.retention.v1",
        "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "settings": settings,
        "conversations": conversations,
        "calls": rows,
        "fold_summaries": summaries,
        "aggregates": _aggregate(rows, profile_data, prices, final_metrics),
    }
    _emit(output, report)
    regressed = any(
        field.get("eligible") and field.get("plain_correct") and not field.get("folded_correct")
        for row in rows for field in row["fields"].values()
    )
    eligible = report["aggregates"]["eligible_fields"]
    return report, 2 if not rows or not eligible else (1 if regressed else 0)


def _emit(path: Path, report: dict[str, Any]) -> None:
    try:
        write_json(path, report)
    except fr.BenchmarkError as exc:
        if "key-like" not in str(exc):
            raise
        # The calls carried a key-like string. Keep the settings and the refusal, drop the text.
        write_json(path, {
            "schema": report.get("schema"),
            "started_utc": report.get("started_utc"),
            "finished_utc": report.get("finished_utc"),
            "settings": report.get("settings"),
            "conversations": [],
            "calls": [],
            "fold_summaries": [],
            "error": {"type": "KeyLike", "message": str(exc)},
        })
        raise


def refresh_conversation_totals(path: Path) -> None:
    """Recompute per-conversation fold reasons after the runner attaches log decisions."""
    report = json.loads(path.read_text(encoding="utf-8"))
    prices = (report.get("settings") or {}).get("prices") or {}
    calls = report.get("calls") or []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in calls:
        grouped.setdefault(row.get("conversation"), []).append(row)
    for conversation in report.get("conversations") or []:
        if "totals" not in conversation:
            continue
        conversation["totals"] = conversation_totals(grouped.get(conversation.get("name"), []), prices)
    if calls and isinstance(report.get("aggregates"), dict):
        report["aggregates"]["folds"].update(fold_tally(calls, prices))
        report["aggregates"]["caching"] = fr._caching_aggregates(calls, prices)
    write_json(path, report)
