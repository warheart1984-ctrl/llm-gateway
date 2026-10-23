"""Boot the benchmark configuration with stub environment values only."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
from urllib.request import urlopen


class FoldingConfigBoot(unittest.TestCase):
    def test_manual_workflow_is_dispatch_only_and_makes_its_own_gateway_keys(self) -> None:
        workflow = (Path(__file__).resolve().parents[2] / ".github" / "workflows" / "fold-retention.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("\n  push:", workflow)
        # The vendor key is the only repository secret.
        self.assertIn("secrets.GROQ_API_KEY", workflow)
        # The gateway lives for one job, so its client keys are made per run
        # and masked, never stored as secrets.
        for name in ("GATEWAY_KEY_PLAIN", "GATEWAY_KEY_FOLDED", "GATEWAY_KEY_BENCH_ADMIN"):
            self.assertNotIn(f"secrets.{name}", workflow)
            self.assertIn(name, workflow)
        self.assertIn("secrets.token_urlsafe", workflow)
        self.assertIn("::add-mask::", workflow)
        self.assertNotIn("gsk_", workflow)
        self.assertNotIn("gwk_", workflow)

    def test_benchmark_config_boots_with_stubbed_keys(self) -> None:
        root = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory(prefix="folding-config-") as directory:
            config = Path(directory) / "config"
            shutil.copytree(root / "bench" / "folding", config)
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            env = dict(os.environ)
            env.update({
                "LLM_GATEWAY_CONFIG_DIR": str(config),
                "LLM_GATEWAY__SERVER__PORT": str(port),
                "GROQ_API_KEY": "stub-groq-key",
                "GATEWAY_KEY_PLAIN": "stub-plain-key",
                "GATEWAY_KEY_FOLDED": "stub-folded-key",
                "GATEWAY_KEY_BENCH_ADMIN": "stub-admin-key",
                "RUST_LOG": "error",
            })
            build = subprocess.run(
                ["cargo", "build", "--locked", "--bin", "llm-gateway"],
                cwd=root,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                # CI builds the gateway in an earlier step, so this is a no-op
                # there; a cold local debug build can take several minutes.
                timeout=900,
            )
            self.assertEqual(build.returncode, 0, "gateway build failed")
            binary = root / "target" / "debug" / "llm-gateway"
            if os.name == "nt":
                binary = binary.with_suffix(".exe")
            process = subprocess.Popen(
                [str(binary)],
                cwd=root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        self.fail("gateway exited before the benchmark configuration became live")
                    try:
                        with urlopen(f"http://127.0.0.1:{port}/health/live", timeout=1) as response:
                            if response.status == 200:
                                return
                    except OSError:
                        time.sleep(0.25)
                self.fail("gateway did not become live with benchmark configuration")
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
                # Never include captured process output in an assertion: it may
                # contain provider diagnostics even though keys are not printed.
                process.stdout.close()
                process.stderr.close()


if __name__ == "__main__":
    unittest.main()
