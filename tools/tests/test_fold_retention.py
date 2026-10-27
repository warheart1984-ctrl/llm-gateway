"""Unit and local HTTP tests for tools/fold_retention.py."""

from __future__ import annotations

import base64
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fold_retention as fr  # noqa: E402
import run_fold_retention_local as runner  # noqa: E402


class StubState:
    def __init__(self) -> None:
        self.spend = {"plain-key": 0, "folded-key": 0}
        self.calls: list[tuple[str, dict]] = []
        self.committed = 0
        # What the folded arm answers instead of the facts, and the summary it
        # is sent on each of its calls (none: no x-gateway-fold-summary).
        self.folded_answer: dict | None = None
        self.summaries: list[str] = []


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
                f'gw_fold_checks_failed_total{{tenant="folded",check="pinned_facts_present"}} {self.state.committed // 2}',
                'gw_fold_checks_failed_total{tenant="folded",check="non_empty"} 0',
                f'gw_fold_corrective_retries_total{{tenant="folded",result="committed"}} {self.state.committed // 3}',
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
        headers: dict[str, str] = {}
        answer = fr.INITIAL
        if folded:
            self.state.committed += 1
            headers = {"x-gateway-folded": "1", "x-request-id": f"req-{self.state.committed}"}
            if self.state.summaries:
                text = self.state.summaries[self.state.committed - 1]
                headers["x-gateway-fold-summary"] = base64.urlsafe_b64encode(text.encode("utf-8")).decode().rstrip("=")
            if self.state.folded_answer is not None:
                answer = self.state.folded_answer
        self._json(
            200,
            {
                "choices": [{
                    "message": {"role": "assistant", "content": json.dumps(answer)},
                    "finish_reason": "stop",
                }],
                # An unfolded conversation is mostly served from the provider's
                # prompt cache; a folded one, whose start changes, is not.
                "usage": ({"prompt_tokens": 300, "completion_tokens": 10,
                           "prompt_tokens_details": {"cached_tokens": 0}}
                          if folded else
                          {"prompt_tokens": 1000, "completion_tokens": 10,
                           "prompt_tokens_details": {"cached_tokens": 800}}),
            },
            headers,
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

    def test_the_arms_alternate_and_caching_is_priced(self) -> None:
        output = Path(self.tmp.name) / "caching.json"
        prices = {
            "main_input_per_mtok_usd": 0.40, "main_cached_input_per_mtok_usd": 0.10,
            "main_output_per_mtok_usd": 1.60,
            "summarizer_input_per_mtok_usd": 0.10, "summarizer_output_per_mtok_usd": 0.40,
        }
        report, code = fr.run_benchmark(
            base_url=f"http://127.0.0.1:{self.server.server_port}",
            plain_key="plain-key", folded_key="folded-key", admin_key="admin-secret",
            model="test/model", scenario_name="fixed", profile="lifecycle",
            output=output, max_calls=2, timeout=2, prices=prices,
        )
        self.assertEqual(code, 0)
        order = [key for key, _body in self.state.calls]
        self.assertEqual(order, ["plain-key", "folded-key", "folded-key", "plain-key"])
        self.assertEqual([row["order"] for row in report["calls"]], ["plain-first", "folded-first"])
        self.assertEqual(report["calls"][0]["plain"]["usage"],
                         {"prompt_tokens": 1000, "cached_tokens": 800, "completion_tokens": 10})

        caching = report["aggregates"]["caching"]
        self.assertAlmostEqual(caching["cached_share"]["plain"], 0.8)
        self.assertAlmostEqual(caching["cached_share"]["folded"], 0.0)
        # Per call: plain 200 fresh x 0.40 + 800 cached x 0.10 + 10 out x 1.60,
        # folded 300 x 0.40 + 10 x 1.60; no summarizer tokens in the stub.
        cost = caching["provider_cost_usd"]
        self.assertAlmostEqual(cost["plain"], 2 * (200 * 0.40 + 800 * 0.10 + 10 * 1.60) / 1e6)
        self.assertAlmostEqual(cost["folded"], 2 * (300 * 0.40 + 10 * 1.60) / 1e6)
        self.assertAlmostEqual(cost["saving"], cost["plain"] - cost["folded"])

    def test_without_every_price_there_is_no_provider_cost(self) -> None:
        report, _code = fr.run_benchmark(
            base_url=f"http://127.0.0.1:{self.server.server_port}",
            plain_key="plain-key", folded_key="folded-key", admin_key="admin-secret",
            model="test/model", scenario_name="fixed", profile="lifecycle",
            output=Path(self.tmp.name) / "partial.json", max_calls=1, timeout=2,
            prices={"main_input_per_mtok_usd": 0.40},
        )
        self.assertIsNone(report["aggregates"]["caching"]["provider_cost_usd"])
        self.assertAlmostEqual(report["aggregates"]["caching"]["cached_share"]["plain"], 0.8)


    def test_wrong_answers_summaries_and_fold_outcomes_are_recorded(self) -> None:
        wrong = dict(fr.INITIAL, owner="Ana")
        del wrong["city"]
        self.state.folded_answer = wrong
        # The second call is sent the same summary as the first; it is kept once.
        self.state.summaries = ["Owner: Mira. City: Oslo.", "Owner: Mira. City: Oslo.", "Owner: Noor \u00e9."]
        report, _code = fr.run_benchmark(
            base_url=f"http://127.0.0.1:{self.server.server_port}",
            plain_key="plain-key", folded_key="folded-key", admin_key="admin-secret",
            model="test/model", scenario_name="fixed", profile="lifecycle",
            output=Path(self.tmp.name) / "diagnostics.json", max_calls=3, timeout=2,
        )
        fields = report["calls"][0]["fields"]
        self.assertEqual(fields["owner"]["folded_answer"], {"value": "Ana"})
        self.assertEqual(fields["city"]["folded_answer"], {"missing": True})
        self.assertNotIn("folded_answer", fields["project_id"])
        self.assertTrue(all("plain_answer" not in field for field in fields.values()))

        self.assertEqual(
            [(entry["call"], entry["text"]) for entry in report["fold_summaries"]],
            [(1, "Owner: Mira. City: Oslo."), (3, "Owner: Noor \u00e9.")],
        )
        self.assertEqual(report["fold_summaries"][1]["chars"], len("Owner: Noor \u00e9."))
        folded = report["calls"][1]["folded"]
        self.assertEqual(folded["request_id"], "req-2")
        self.assertEqual(folded["fold_outcomes"], {"committed": 1})
        # The stub counts a failed pinned-value check on every second call.
        self.assertEqual(folded["failed_checks"], {"pinned_facts_present": 1})
        self.assertEqual(report["calls"][0]["folded"]["failed_checks"], {})
        self.assertEqual(report["aggregates"]["folds"]["failed_checks"], {"pinned_facts_present": 1})
        # And a committed corrective retry on every third.
        self.assertEqual(report["calls"][2]["folded"]["corrective_retries"], {"committed": 1})
        self.assertEqual(report["calls"][1]["folded"]["corrective_retries"], {})
        self.assertEqual(report["aggregates"]["folds"]["corrective_retries"], {"committed": 1})

    def test_a_plain_arm_that_is_wrong_is_recorded_too(self) -> None:
        self.assertEqual(fr.answered(fr.score_answer("not json", {"a": 1}, "stop"), "a"), {"missing": True})
        self.assertEqual(fr.answered(fr.score_answer('{"a": null}', {"a": 1}, "stop"), "a"), {"value": None})


class Diagnostics(unittest.TestCase):
    def test_a_summary_header_decodes_without_padding(self) -> None:
        text = "Owner is now Ines \u2014 caf\u00e9"
        header = base64.urlsafe_b64encode(text.encode("utf-8")).decode().rstrip("=")
        self.assertEqual(fr.decode_fold_summary(header), text)
        with self.assertRaises(ValueError):
            fr.decode_fold_summary(base64.urlsafe_b64encode(b"\xff\xfe").decode())

    def test_fold_outcome_deltas_name_only_what_moved_for_the_tenant(self) -> None:
        def key(outcome: str, tenant: str = "folded") -> tuple:
            return ("gw_folds_total", tuple(sorted({"tenant": tenant, "outcome": outcome}.items())))
        before = {key("committed"): 2, key("unprofitable"): 5, key("committed", "plain"): 0}
        after = {key("committed"): 2, key("unprofitable"): 6, key("backoff"): 1, key("committed", "plain"): 9}
        self.assertEqual(fr.fold_outcome_deltas(before, after), {"backoff": 1, "unprofitable": 1})

    def test_a_replaced_summarizer_is_named_in_the_result_file(self) -> None:
        self.assertEqual(runner.result_name("openrouter", "mutating", "lifecycle", "T1"),
                         "fold_retention_openrouter_mutating_lifecycle_T1.json")
        self.assertEqual(runner.result_name("openrouter", "mutating", "lifecycle", "T1", "openrouter/gpt-4.1-mini"),
                         "fold_retention_openrouter_mutating_lifecycle_T1_sum-gpt-4.1-mini.json")
        self.assertEqual(runner.result_name("openrouter", "mutating", "lifecycle", "T1", "openrouter/gpt-4.1-mini", 16),
                         "fold_retention_openrouter_mutating_lifecycle_T1_sum-gpt-4.1-mini_amort-16.json")
        self.assertEqual(runner.result_name("openrouter", "constraints", "lifecycle", "T1"),
                         "fold_tasks_openrouter_constraints_lifecycle_T1.json")

    def test_the_amortization_is_set_in_the_tenants_file(self) -> None:
        for name in ("tenants.yaml", "tenants.lifecycle.yaml", "tenants.tools.yaml"):
            text = (runner.BENCH / name).read_text(encoding="utf-8")
            changed = runner.with_amortization(text, 16)
            self.assertIn("      amortization_requests: 16\n", changed)
            self.assertEqual(changed.count("amortization_requests:"), 1)
            self.assertEqual(len(changed.splitlines()), len(text.splitlines()))
        with self.assertRaises(ValueError):
            runner.with_amortization("tenants: []\n", 16)

    def test_only_the_tools_scenario_gets_the_larger_summarizer_budget(self) -> None:
        self.assertEqual(runner.tenants_file("tools", "lifecycle"), "tenants.tools.yaml")
        self.assertEqual(runner.tenants_file("tools", "economics"), "tenants.tools.yaml")
        self.assertEqual(runner.tenants_file("fixed", "lifecycle"), "tenants.lifecycle.yaml")
        self.assertEqual(runner.tenants_file("constraints", "economics"), "tenants.yaml")
        self.assertEqual(runner.tenants_file("tools", "lifecycle", corpus=True), "tenants.lifecycle.yaml")
        for name in ("tenants.yaml", "tenants.lifecycle.yaml"):
            text = (runner.BENCH / name).read_text(encoding="utf-8")
            self.assertIn("summarizer_max_tokens: 512\n", text)
            self.assertNotIn("fold_tool_turns", text)
        tools = (runner.BENCH / "tenants.tools.yaml").read_text(encoding="utf-8")
        self.assertIn("summarizer_max_tokens: 1024\n", tools)
        self.assertIn("fold_tool_turns: true\n", tools)

    def test_the_runner_attaches_each_logged_fold_decision_by_request_id(self) -> None:
        decision = {"outcome": "refused", "reason": "cost_check", "fold_count": 8}
        log = "\n".join([
            "not json",
            json.dumps({"message": "stream complete", "request_id": "req-1", "tenant": "folded",
                        "fold": json.dumps(decision)}),
            json.dumps({"fields": {"message": "stream complete", "request_id": "req-2",
                                   "fold": json.dumps(dict(decision, reason="max_folds"))}}),
            json.dumps({"message": "stream complete", "request_id": "req-3"}),
        ])
        with tempfile.TemporaryDirectory() as tmp:
            result, log_path = Path(tmp) / "result.json", Path(tmp) / "gateway.log"
            log_path.write_text(log, encoding="utf-8")
            result.write_text(json.dumps({"calls": [
                {"folded": {"request_id": "req-1"}},
                {"folded": {"request_id": "req-2"}},
                {"folded": {"request_id": "req-3"}},
                {"folded": {"request_id": None}},
            ]}), encoding="utf-8")
            self.assertEqual(runner.attach_fold_decisions(result, log_path), 2)
            calls = json.loads(result.read_text(encoding="utf-8"))["calls"]
        self.assertEqual(calls[0]["folded"]["fold_decision"], decision)
        self.assertEqual(calls[1]["folded"]["fold_decision"]["reason"], "max_folds")
        self.assertNotIn("fold_decision", calls[2]["folded"])
        self.assertNotIn("fold_decision", calls[3]["folded"])

    def test_capture_off_adds_nothing_and_leaves_the_result_untouched(self) -> None:
        base = {"LLM_GATEWAY__SERVER__PORT": "9", "OPENROUTER_API_KEY": "not-a-key"}
        merged = dict(base)
        merged.update(runner.capture_env(False, Path("benchmark_results/out.forwarded.jsonl")))
        self.assertEqual(merged, base)
        self.assertEqual(runner.tools_cost_limit(False, None), None)
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "result.json"
            original = json.dumps({"calls": [{"folded": {"request_id": "req-1", "answer": 39}}]}, indent=2)
            result.write_text(original, encoding="utf-8")
            self.assertEqual(runner.attach_forwarded_captures(result, None), 0)
            self.assertEqual(result.read_text(encoding="utf-8"), original)
            missing = Path(tmp) / "absent.forwarded.jsonl"
            self.assertEqual(runner.attach_forwarded_captures(result, missing), 0)
            self.assertEqual(result.read_text(encoding="utf-8"), original)

    def test_capture_on_joins_provider_usage_and_leaves_the_body_in_the_sidecar(self) -> None:
        body = {"model": "openai/gpt-4.1-mini", "messages": [{"role": "user", "content": "39"}], "stream": False}
        usage = {"prompt_tokens": 1722, "completion_tokens": 12, "cost": 0.000812, "reported": {"cost": 0.000812}}
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "fold_tools_run.json"
            capture = runner.capture_path_for(result)
            self.assertEqual(capture.name, "fold_tools_run.forwarded.jsonl")
            capture.write_text(json.dumps({
                "request_id": "req-1", "tenant_id": "folded", "key_id": "ak_bench_folded",
                "body": body, "usage": usage,
            }) + "\n", encoding="utf-8")
            result.write_text(json.dumps({"calls": [
                {"folded": {"request_id": "req-1", "answer": "39"}},
                {"folded": {"request_id": "req-2", "answer": 39}},
            ]}), encoding="utf-8")
            env = runner.capture_env(True, capture)
            self.assertEqual(env["LLM_GATEWAY_CAPTURE_FORWARDED"], "1")
            self.assertEqual(env["LLM_GATEWAY_CAPTURE_FORWARDED_TENANT"], "folded")
            self.assertEqual(env["LLM_GATEWAY_CAPTURE_FORWARDED_PATH"], str(capture))
            self.assertEqual(runner.attach_forwarded_captures(result, capture), 1)
            report = json.loads(result.read_text(encoding="utf-8"))
        self.assertEqual(report["forwarded_capture"], capture.name)
        self.assertEqual(report["calls"][0]["folded"]["provider_usage"], usage)
        self.assertNotIn("body", report["calls"][0]["folded"])
        self.assertNotIn("provider_usage", report["calls"][1]["folded"])
        self.assertEqual(runner.tools_cost_limit(True, None), 1.0)
        self.assertEqual(runner.tools_cost_limit(True, 0.25), 0.25)
        with self.assertRaises(ValueError):
            runner.capture_env(True, None)


class Usage(unittest.TestCase):
    def test_usage_without_cache_details_counts_none_cached(self) -> None:
        self.assertEqual(fr._usage_of({"usage": {"prompt_tokens": 5, "completion_tokens": 2}}),
                         {"prompt_tokens": 5, "cached_tokens": 0, "completion_tokens": 2})
        self.assertEqual(fr._usage_of({}), {"prompt_tokens": 0, "cached_tokens": 0, "completion_tokens": 0})

    def test_provider_cost_prices_cached_tokens_at_the_cached_rate(self) -> None:
        usage = {"prompt_tokens": 1_000_000, "cached_tokens": 750_000, "completion_tokens": 0}
        self.assertAlmostEqual(fr.provider_cost_usd(usage, 0.40, 0.10, 1.60), 0.25 * 0.40 + 0.75 * 0.10)


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


class Redirects(unittest.TestCase):
    """A redirect is refused, so the API key never reaches the place it points to."""

    def test_a_redirect_is_not_followed_and_the_key_goes_nowhere_else(self) -> None:
        seen: list[str | None] = []

        class Elsewhere(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: object) -> None:
                return

            def do_GET(self) -> None:  # noqa: N802
                seen.append(self.headers.get("x-api-key"))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"spent_nano_usd": 0}')

        elsewhere = ThreadingHTTPServer(("127.0.0.1", 0), Elsewhere)
        target = f"http://127.0.0.1:{elsewhere.server_address[1]}/v1/usage"

        class Redirector(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: object) -> None:
                return

            def do_GET(self) -> None:  # noqa: N802
                self.send_response(302)
                self.send_header("Location", target)
                self.end_headers()

        redirector = ThreadingHTTPServer(("127.0.0.1", 0), Redirector)
        servers = [elsewhere, redirector]
        for server in servers:
            threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            with self.assertRaises(fr.BenchmarkError) as caught:
                fr._request(f"http://127.0.0.1:{redirector.server_address[1]}", "/v1/usage", "secret-key", None, 5.0)
            self.assertIn("302", str(caught.exception))
            self.assertEqual(seen, [], "nothing, and so no key, reached the redirect target")
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()


if __name__ == "__main__":
    unittest.main()
