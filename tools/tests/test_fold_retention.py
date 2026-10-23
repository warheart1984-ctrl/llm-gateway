"""Unit and local HTTP tests for tools/fold_retention.py."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fold_retention as fr  # noqa: E402


class StubState:
    def __init__(self) -> None:
        self.spend = {"plain-key": 0, "folded-key": 0}
        self.calls: list[tuple[str, dict]] = []
        self.committed = 0


class StubHandler(BaseHTTPRequestHandler):
    state: StubState

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _key(self) -> str:
        return self.headers.get("x-api-key", "")

    def _json(self, status: int, payload: object, headers: dict[str, str] | None = None) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/v1/usage":
            self._json(200, {"spent_nano_usd": self.state.spend.get(self._key(), 0)})
            return
        if self.path == "/metrics":
            body = "\n".join([
                '# TYPE gw_folds_total counter',
                f'gw_folds_total{{tenant="folded",outcome="committed"}} {self.state.committed}',
                'gw_folds_total{tenant="folded",outcome="rejected"} 0',
                'gw_folds_total{tenant="folded",outcome="failed"} 0',
                'gw_fold_summarizer_tokens_total{tenant="folded",kind="prompt"} 0',
                'gw_fold_summarizer_tokens_total{tenant="folded",kind="completion"} 0',
                '',
            ]).encode("utf-8")
            self.send_response(200)
            self.send_header("content-type", "text/plain")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("content-length", "0"))
        body = json.loads(self.rfile.read(length))
        key = self._key()
        self.state.calls.append((key, body))
        self.state.spend[key] = self.state.spend.get(key, 0) + 100
        folded = key == "folded-key"
        if folded:
            self.state.committed += 1
        self._json(
            200,
            {
                "choices": [{
                    "message": {"role": "assistant", "content": json.dumps(fr.INITIAL)},
                    "finish_reason": "stop",
                }],
            },
            {"x-gateway-folded": "1"} if folded else {},
        )


class RetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state = StubState()
        handler = type("Handler", (StubHandler,), {"state": self.state})
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.tmp = tempfile.TemporaryDirectory(prefix="fold-retention-")

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.tmp.cleanup()

    def test_exact_match(self) -> None:
        result = fr.score_answer(json.dumps({"a": 1}), {"a": 1}, "stop")
        self.assertTrue(result["all_correct"])

    def test_type_mismatch(self) -> None:
        result = fr.score_answer(json.dumps({"a": "8443"}), {"a": 8443}, "stop")
        self.assertFalse(result["fields"]["a"])

    def test_duplicate_key_is_rejected(self) -> None:
        result = fr.score_answer('{"a": 1, "a": 2}', {"a": 2}, "stop")
        self.assertIn("duplicate", result["error"])

    def test_length_finish_is_rejected(self) -> None:
        result = fr.score_answer('{"a": 1}', {"a": 1}, "length")
        self.assertIn("finish", result["error"])

    def test_containment_is_recorded_separately(self) -> None:
        result = fr.score_answer(json.dumps({"a": "value with context"}), {"a": "value"}, "stop")
        self.assertFalse(result["fields"]["a"])
        self.assertTrue(result["fields_containment"]["a"])

    def test_unicode_dash_is_normalized(self) -> None:
        result = fr.score_answer(json.dumps({"a": "OPS\u201120"}), {"a": "OPS-20"}, "stop")
        self.assertTrue(result["fields"]["a"])

    def test_extra_key_is_reported(self) -> None:
        result = fr.score_answer(json.dumps({"a": 1, "extra": 2}), {"a": 1}, "stop")
        self.assertEqual(result["extra_keys"], ["extra"])
        self.assertFalse(result["all_correct"])

    def test_eligibility_requires_fold_prefix_and_plain_accuracy(self) -> None:
        self.assertTrue(fr.retention_eligible(3, 3, True))
        self.assertFalse(fr.retention_eligible(4, 3, True))
        self.assertFalse(fr.retention_eligible(3, 3, False))

    def test_scenarios_match_the_committed_5_fold_engine_fixture(self) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "folding_retention_calls_v1.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        self.assertEqual(fixture["fold_cap"], 5)
        for name, rows in fixture["calls"].items():
            actual = fr.fixture_digest(fr.SCENARIOS[name](), calls=50, fold_cap=5)
            self.assertEqual(actual, rows, name)

    def test_local_stub_run_is_paired_and_secret_free(self) -> None:
        output = Path(self.tmp.name) / "result.json"
        report, code = fr.run_benchmark(
            base_url=f"http://127.0.0.1:{self.server.server_port}",
            plain_key="plain-key",
            folded_key="folded-key",
            admin_key="admin-secret",
            model="test/model",
            scenario_name="fixed",
            profile="lifecycle",
            output=output,
            max_calls=1,
            timeout=2,
        )
        self.assertEqual(code, 0)
        self.assertTrue(output.exists())
        self.assertEqual(self.state.calls[0][1], self.state.calls[1][1])
        serialized = output.read_text(encoding="utf-8")
        self.assertNotIn("plain-key", serialized)
        self.assertNotIn("folded-key", serialized)
        self.assertNotIn("admin-secret", serialized)
        self.assertFalse(report["settings"]["secrets_written"])
        self.assertEqual(report["calls"][0]["folded"]["folded_turns"], 1)


class Retrying(unittest.TestCase):
    """A vendor rate limit is waited out; any other refusal ends the run."""

    def arm(self, answers: list, waits: list) -> dict:
        def request(*_args, **_kwargs):
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer
        original = fr._request, fr._usage
        fr._request, fr._usage = request, lambda *_args: 0
        try:
            return fr._arm("http://stub", "key", {}, 1.0, sleep=waits.append)
        finally:
            fr._request, fr._usage = original

    def test_a_rate_limited_call_is_retried_and_the_retries_recorded(self) -> None:
        ok = (200, {}, {"choices": [{"message": {"content": "{}"}, "finish_reason": "stop"}]})
        waits: list = []
        result = self.arm([fr.TransientError("HTTP 502"), fr.TransientError("HTTP 429"), ok], waits)
        self.assertEqual(result["retries"], 2)
        self.assertEqual(waits, list(fr.RETRY_WAITS_SECONDS[:2]))

    def test_a_call_still_refused_after_every_wait_ends_the_run(self) -> None:
        refusals = [fr.TransientError("HTTP 502")] * (len(fr.RETRY_WAITS_SECONDS) + 1)
        with self.assertRaises(fr.TransientError):
            self.arm(refusals, [])

    def test_a_plain_refusal_is_not_retried(self) -> None:
        waits: list = []
        with self.assertRaises(fr.BenchmarkError) as caught:
            self.arm([fr.BenchmarkError("HTTP 400")], waits)
        self.assertNotIsInstance(caught.exception, fr.TransientError)
        self.assertEqual(waits, [])


if __name__ == "__main__":
    unittest.main()
