"""Replay-corpus tests. Each one fails when the behaviour it names is removed."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import corpus_import  # noqa: E402
import corpus_replay as cr  # noqa: E402
import fold_retention as fr  # noqa: E402
import run_fold_retention_local as runner  # noqa: E402


CHATGPT = {
    "title": "Cedar",
    "current_node": "c",
    "mapping": {
        "root": {"id": "root", "parent": None, "children": ["a"], "message": None},
        "a": {
            "id": "a",
            "parent": "root",
            "children": ["b"],
            "message": {
                "author": {"role": "user"},
                "content": {"content_type": "text", "parts": ["The owner is Mira."]},
            },
        },
        "side": {
            "id": "side",
            "parent": "a",
            "children": [],
            "message": {
                "author": {"role": "user"},
                "content": {"content_type": "text", "parts": ["IGNORE THIS BRANCH"]},
            },
        },
        "b": {
            "id": "b",
            "parent": "a",
            "children": ["c"],
            "message": {
                "author": {"role": "assistant"},
                "content": {"content_type": "text", "parts": ["Noted, Mira owns it."]},
            },
        },
        "c": {
            "id": "c",
            "parent": "b",
            "children": [],
            "message": {
                "author": {"role": "user"},
                "content": {"content_type": "text", "parts": ["The city is Oslo."]},
            },
        },
    },
}

CLAUDE = {
    "conversations": [{
        "name": "Harbor",
        "chat_messages": [
            {"sender": "human", "text": "Budget is 2400 USD."},
            {
                "sender": "assistant",
                "text": "",
                "content": [
                    {"type": "text", "text": "Recorded 2400."},
                    {"type": "text", "text": "USD."},
                ],
            },
        ],
    }],
}


def test_chatgpt_export_follows_the_current_branch(tmp_path: Path) -> None:
    export = tmp_path / "conversations.json"
    export.write_text(json.dumps([CHATGPT]), encoding="utf-8")
    written = corpus_import.import_export(export, "auto", tmp_path / "out")
    assert [path.name for path in written] == ["0001-cedar.jsonl"]
    messages = cr.load_jsonl_conversation(written[0])
    assert messages == [
        {"role": "user", "content": "The owner is Mira."},
        {"role": "assistant", "content": "Noted, Mira owns it."},
        {"role": "user", "content": "The city is Oslo."},
    ]
    assert "IGNORE THIS BRANCH" not in written[0].read_text(encoding="utf-8")


def test_claude_export_reads_sender_text_and_content_parts(tmp_path: Path) -> None:
    export = tmp_path / "claude.json"
    export.write_text(json.dumps(CLAUDE), encoding="utf-8")
    written = corpus_import.import_export(export, "claude", tmp_path / "wrapped")
    messages = cr.load_jsonl_conversation(written[0])
    assert messages[0] == {"role": "user", "content": "Budget is 2400 USD."}
    assert messages[1]["role"] == "assistant"
    assert messages[1]["content"] == "Recorded 2400.\nUSD."

    bare = tmp_path / "bare.json"
    bare.write_text(json.dumps(CLAUDE["conversations"]), encoding="utf-8")
    bare_written = corpus_import.import_export(bare, "auto", tmp_path / "bare-out")
    assert cr.load_jsonl_conversation(bare_written[0]) == messages


def test_jsonl_parsing_keeps_roles_and_rejects_a_bad_line(tmp_path: Path) -> None:
    path = tmp_path / "ok.jsonl"
    path.write_text("\n".join([
        json.dumps({"role": "system", "content": "Be brief."}),
        "",
        json.dumps({"role": "user", "content": "Hello"}),
        json.dumps({"role": "assistant", "content": "Hi"}),
    ]) + "\n", encoding="utf-8")
    assert cr.load_jsonl_conversation(path) == [
        {"role": "system", "content": "Be brief."},
        {"role": "user", "content": "Hello"},
        {"role": "assistant", "content": "Hi"},
    ]
    bad = tmp_path / "bad.jsonl"
    bad.write_text(json.dumps({"role": "tool", "content": "nope"}) + "\n", encoding="utf-8")
    with pytest.raises(fr.BenchmarkError):
        cr.load_jsonl_conversation(bad)


def test_checkpoints_are_every_n_user_turns(tmp_path: Path) -> None:
    del tmp_path
    assert cr.checkpoint_user_turns(25, 10) == [10, 20]
    assert cr.checkpoint_user_turns(10, 10) == [10]
    assert cr.checkpoint_user_turns(9, 10) == []
    assert cr.checkpoint_user_turns(3, 1) == [1, 2, 3]
    args = fr.build_parser().parse_args([
        "--model", "m", "--corpus", "c", "--plain-key", "p", "--folded-key", "f", "--admin-key", "a",
    ])
    assert args.probe_every == 10


def test_only_folded_away_facts_the_unfolded_tenant_got_right_are_scored() -> None:
    facts = [
        {"field": "owner", "value": "Mira", "source_turn": 2, "question": "Who owns it?"},
        {"field": "city", "value": "Oslo", "source_turn": 8, "question": "Which city?"},
        {"field": "rule", "value": "be brief", "source_turn": 0, "question": "What is the rule?"},
    ]
    plain = json.dumps({"owner": "Mira", "city": "Bergen", "rule": "be brief"})
    folded = json.dumps({"owner": "Noor", "city": "Oslo", "rule": "be brief"})
    result = cr.score_checkpoint(facts, 4, plain, folded, "stop", "stop")
    assert set(result["fields"]) == {"owner"}
    assert result["fields"]["owner"]["eligible"] is True
    assert result["fields"]["owner"]["folded_correct"] is False
    wrong = result["wrong_answers"][0]
    assert wrong["field"] == "owner"
    assert wrong["plain_text"] == plain
    assert wrong["folded_text"] == folded
    assert wrong["eligible"] is True

    missed = cr.score_checkpoint(
        facts, 4, json.dumps({"owner": "Ana"}), folded, "stop", "stop",
    )
    assert missed["fields"]["owner"]["plain_correct"] is False
    assert missed["fields"]["owner"]["eligible"] is False

    typed = cr.score_checkpoint(
        [{"field": "budget_usd", "value": 1800, "source_turn": 1, "question": "What is the budget?"}],
        2,
        json.dumps({"budget_usd": "1800"}),
        json.dumps({"budget_usd": 1800}),
        "stop",
        "stop",
    )
    assert typed["fields"]["budget_usd"]["plain_correct"] is False
    assert typed["fields"]["budget_usd"]["eligible"] is False
    assert "city" not in typed["fields"]


def test_latest_fact_is_kept_and_an_existing_file_is_not_regenerated(tmp_path: Path) -> None:
    kept = cr.latest_facts([
        cr.normalize_fact({"field": "owner", "value": "Mira", "source_turn": 1, "question": "Who?"}),
        cr.normalize_fact({"field": "owner", "value": "Noor", "source_turn": 4, "question": "Who now?"}),
        cr.normalize_fact({"field": "city", "value": "Oslo", "source_turn": 2, "question": "Where?"}),
    ])
    assert [(fact["field"], fact["value"]) for fact in kept] == [("owner", "Noor"), ("city", "Oslo")]

    conversation = tmp_path / "c.jsonl"
    conversation.write_text(json.dumps({"role": "user", "content": "Hi"}) + "\n", encoding="utf-8")
    fact_file = cr.facts_path(conversation)
    fact_file.write_text(json.dumps([{
        "field": "owner", "value": "Edited", "source_turn": 1, "question": "Who?",
    }]), encoding="utf-8")
    calls: list[int] = []
    loaded = cr.extract_with(conversation, [{"role": "user", "content": "Hi"}], True, lambda _messages: calls.append(1))
    assert calls == []
    assert loaded[0]["value"] == "Edited"
    assert "Edited" in fact_file.read_text(encoding="utf-8")


def test_conversation_totals_count_kept_facts_caching_folds_and_backoff() -> None:
    prices = {
        "main_input_per_mtok_usd": 0.40,
        "main_cached_input_per_mtok_usd": 0.10,
        "main_output_per_mtok_usd": 1.60,
        "summarizer_input_per_mtok_usd": 0.10,
        "summarizer_output_per_mtok_usd": 0.40,
    }
    plain_usage = {"prompt_tokens": 1_000_000, "cached_tokens": 750_000, "completion_tokens": 0}
    folded_usage = {"prompt_tokens": 100_000, "cached_tokens": 0, "completion_tokens": 0}
    zero = {"prompt_tokens": 0, "cached_tokens": 0, "completion_tokens": 0}

    def arm(usage: dict, spend: int) -> dict:
        return {"usage": usage, "spend_delta_nano_usd": spend, "summarizer_prompt_tokens": 0, "summarizer_completion_tokens": 0}

    rows = [
        {
            "call": 1,
            "conversation": "cedar",
            "fields": {"owner": {"eligible": True, "folded_correct": True, "plain_correct": True}},
            "wrong_answers": [],
            "plain": arm(plain_usage, 100),
            "folded": {**arm(folded_usage, 40), "fold_outcomes": {"committed": 1}},
        },
        {
            "call": 2,
            "conversation": "cedar",
            "fields": {
                "owner": {"eligible": True, "folded_correct": False, "plain_correct": True},
                "city": {"eligible": False, "folded_correct": True, "plain_correct": False},
            },
            "wrong_answers": [{
                "field": "owner",
                "expected": "Noor",
                "plain_text": "plain said Noor",
                "folded_text": "folded said Mira",
            }],
            "plain": arm(zero, 0),
            "folded": {
                **arm(zero, 80),
                "fold_outcomes": {"backoff": 1},
                "fold_decision": {"outcome": "skipped", "reason": "backoff"},
            },
        },
        {
            "call": 3,
            "conversation": "cedar",
            "fields": {},
            "wrong_answers": [],
            "plain": arm(zero, 0),
            "folded": {
                **arm(zero, 10),
                "fold_outcomes": {"unprofitable": 1},
                "fold_decision": {"outcome": "refused", "reason": "cost_check"},
            },
        },
    ]
    totals = cr.conversation_totals(rows, prices)
    assert totals["facts_kept"] == {"correct": 1, "eligible": 2}
    assert totals["wrong_answers"][0]["plain_text"] == "plain said Noor"
    assert totals["wrong_answers"][0]["folded_text"] == "folded said Mira"
    assert totals["folds"]["attempted"] == 2
    assert totals["folds"]["applied"] == 1
    assert totals["folds"]["refused"]["unprofitable"] == 1
    assert totals["folds"]["refused"]["backoff"] == 1
    assert totals["folds"]["reasons"]["cost_check"] == 1
    assert totals["folds"]["reasons"]["backoff"] == 1
    assert totals["folds"]["backoff_cost_nano_usd"] == 80
    assert totals["saving_with_caching"]["saving"] == pytest.approx(0.135)


def test_the_key_scanner_refuses_a_corpus_and_an_output(tmp_path: Path) -> None:
    assert cr.key_like_kind("sk-or-v1-0123456789abcdef") == "prefix"
    assert cr.key_like_kind("Bearer abc.def") == "bearer"
    assert cr.key_like_kind("id " + ("a1" * 30) + " end") == "opaque"
    assert cr.key_like_kind("7f3a9c1e-1234-4d5e-9abc-0123456789ab") is None
    assert cr.key_like_kind("The owner is Mira and the port is 8443.") is None

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "a.jsonl").write_text(
        json.dumps({"role": "user", "content": "token sk-or-v1-0123456789abcdef"}) + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.json"
    with pytest.raises(fr.BenchmarkError, match="key-like"):
        cr.run_corpus(
            base_url="http://127.0.0.1:1",
            plain_key="plain-key",
            folded_key="folded-key",
            admin_key="admin-secret",
            model="test/model",
            corpus=corpus,
            profile="lifecycle",
            output=out,
            timeout=1,
        )
    assert not out.exists()

    with pytest.raises(fr.BenchmarkError, match="key-like"):
        cr.write_json(out, {"note": "sk-or-v1-0123456789abcdef"})
    assert not out.exists()

    export = tmp_path / "conversations.json"
    export.write_text(json.dumps([{
        "title": "Leak",
        "current_node": "a",
        "mapping": {"a": {"parent": None, "message": {
            "author": {"role": "user"},
            "content": {"parts": ["sk-or-v1-0123456789abcdef"]},
        }}},
    }]), encoding="utf-8")
    dest = tmp_path / "exported"
    with pytest.raises(fr.BenchmarkError, match="key-like"):
        corpus_import.import_export(export, "chatgpt", dest)
    assert not list(dest.glob("*.jsonl"))


def test_scrub_rewrites_emails_phones_and_urls(tmp_path: Path) -> None:
    raw = "Email mira@example.com and mira@example.com, phone 415-555-1212 and +44 20 7946 0958, see https://example.com/a."
    scrubbed = cr.scrub_text(raw)
    for secret in ("mira@example.com", "415-555-1212", "+44 20 7946 0958", "https://example.com/a"):
        assert secret not in scrubbed
    assert scrubbed.count("[EMAIL_1]") == 2
    assert "[PHONE_1]" in scrubbed and "[PHONE_2]" in scrubbed
    assert "[URL_1]" in scrubbed

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    path = corpus / "chat.jsonl"
    path.write_text(
        json.dumps({"role": "user", "content": "Email mira@example.com about the plan."}) + "\n",
        encoding="utf-8",
    )
    cr.facts_path(path).write_text(json.dumps([{
        "field": "contact",
        "value": "other@example.com",
        "source_turn": 1,
        "question": "Which address?",
    }]), encoding="utf-8")
    cr.scrub_corpus(corpus)
    text = path.read_text(encoding="utf-8")
    assert "mira@example.com" not in text
    assert "[EMAIL_1]" in text
    saved = json.loads(cr.facts_path(path).read_text(encoding="utf-8"))
    assert saved[0]["value"] == "[EMAIL_2]"
    assert "other@example.com" not in cr.facts_path(path).read_text(encoding="utf-8")


def test_a_client_request_id_finds_the_logged_fold_decision() -> None:
    decision = {"outcome": "applied", "reason": "batch_ready"}
    log = json.dumps({
        "message": "stream complete",
        "request_id": "11111111-1111-4111-8111-111111111111",
        "client_request_id": "22222222-2222-4222-8222-222222222222",
        "fold": json.dumps(decision),
    })
    found = runner.fold_decisions(log)
    assert found["22222222-2222-4222-8222-222222222222"]["reason"] == "batch_ready"
    assert found["11111111-1111-4111-8111-111111111111"]["outcome"] == "applied"


class _ReplayState:
    def __init__(self) -> None:
        self.posts: list[tuple[str, dict, str | None]] = []
        self.spend = {"plain-key": 0, "folded-key": 0}
        self.committed = 0


def test_replay_regenerates_assistant_turns_and_probes_on_checkpoints(tmp_path: Path) -> None:
    state = _ReplayState()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *_args: object) -> None:
            return

        def _key(self) -> str:
            return self.headers.get("x-api-key", "")

        def _send(self, status: int, payload: object, headers: dict[str, str] | None = None) -> None:
            data = json.dumps(payload).encode("utf-8") if not isinstance(payload, bytes) else payload
            self.send_response(status)
            self.send_header("content-type", "application/json" if not isinstance(payload, bytes) else "text/plain")
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/v1/usage":
                self._send(200, {"spent_nano_usd": state.spend.get(self._key(), 0)})
                return
            if self.path == "/metrics":
                body = "\n".join([
                    f'gw_folds_total{{tenant="folded",outcome="committed"}} {state.committed}',
                    'gw_folds_total{tenant="folded",outcome="rejected"} 0',
                    'gw_folds_total{tenant="folded",outcome="failed"} 0',
                    'gw_folds_total{tenant="folded",outcome="unprofitable"} 0',
                    'gw_folds_total{tenant="folded",outcome="backoff"} 0',
                    'gw_fold_summarizer_tokens_total{tenant="folded",kind="prompt"} 0',
                    'gw_fold_summarizer_tokens_total{tenant="folded",kind="completion"} 0',
                    "",
                ]).encode("utf-8")
                self._send(200, body)
                return
            self._send(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("content-length", "0"))
            body = json.loads(self.rfile.read(length))
            key = self._key()
            state.posts.append((key, body, self.headers.get("x-request-id")))
            state.spend[key] = state.spend.get(key, 0) + 100
            last = body["messages"][-1]["content"]
            if "Who owns it?" in last:
                answer = {"owner": "Noor"} if key == "plain-key" else {"owner": "Mira"}
                text = json.dumps(answer)
            else:
                text = "generated-by-plain" if key == "plain-key" else "generated-by-folded"
            headers = {"x-request-id": self.headers.get("x-request-id", "")}
            if key == "folded-key":
                state.committed += 1
                headers["x-gateway-folded"] = "5"
            self._send(200, {"choices": [{"message": {"role": "assistant", "content": text}, "finish_reason": "stop"}]}, headers)

    handler = type("Handler", (Handler,), {})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    conversation = corpus / "cedar.jsonl"
    recorded = "RECORDED-ASSISTANT-TURN"
    conversation.write_text("\n".join([
        json.dumps({"role": "user", "content": "The owner is Mira."}),
        json.dumps({"role": "assistant", "content": recorded}),
        json.dumps({"role": "user", "content": "The owner is now Noor."}),
        json.dumps({"role": "assistant", "content": recorded}),
    ]) + "\n", encoding="utf-8")
    fact_file = cr.facts_path(conversation)
    fact_file.write_text(json.dumps([{
        "field": "owner", "value": "Noor", "source_turn": 1, "question": "Who owns it?",
    }]), encoding="utf-8")
    before = fact_file.read_bytes()
    output = tmp_path / "result.json"
    try:
        report, code = cr.run_corpus(
            base_url=f"http://127.0.0.1:{server.server_port}",
            plain_key="plain-key",
            folded_key="folded-key",
            admin_key="admin-secret",
            model="test/model",
            corpus=corpus,
            profile="lifecycle",
            output=output,
            probe_every=2,
            extract_facts=True,
            max_tokens=64,
            timeout=5,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert code == 1
    assert fact_file.read_bytes() == before
    assert len(state.posts) == 6
    assert [post[0] for post in state.posts] == [
        "plain-key", "folded-key", "folded-key", "plain-key", "plain-key", "folded-key",
    ]
    for _key, body, request_id in state.posts:
        assert body["temperature"] == 0
        assert body["model"] == "test/model"
        assert request_id
        blob = json.dumps(body)
        assert recorded not in blob
    second_turn = [
        body for _key, body, _request_id in state.posts
        if body["messages"][-1]["content"] == "The owner is now Noor."
    ]
    assert len(second_turn) == 2
    assert any("generated-by-plain" in json.dumps(body) for body in second_turn)
    assert any("generated-by-folded" in json.dumps(body) for body in second_turn)
    probes = [row for row in report["calls"] if row["kind"] == "probe"]
    assert len(probes) == 1
    assert probes[0]["order"] == "plain-first"
    assert report["conversations"][0]["totals"]["facts_kept"] == {"correct": 0, "eligible": 1}
    wrong = report["conversations"][0]["totals"]["wrong_answers"][0]
    assert "Noor" in wrong["plain_text"] and "Mira" in wrong["folded_text"]
    serialized = output.read_text(encoding="utf-8")
    for secret in ("plain-key", "folded-key", "admin-secret", recorded):
        assert secret not in serialized
