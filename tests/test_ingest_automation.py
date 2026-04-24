import importlib
import os
from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]


class IngestAutomationTest(unittest.TestCase):
    def test_public_runner_excludes_discord(self):
        run_public = importlib.import_module("crawlers.run_public")

        self.assertIn("hn.py", run_public.PUBLIC_CRAWLERS)
        self.assertIn("reddit.py", run_public.PUBLIC_CRAWLERS)
        self.assertNotIn("discord.py", run_public.PUBLIC_CRAWLERS)
        self.assertNotIn("discord", " ".join(run_public.PUBLIC_CRAWLERS).lower())

    def test_crawl_workflow_runs_public_ingest_at_0700_kst(self):
        workflow = ROOT / ".github" / "workflows" / "crawl.yml"

        self.assertTrue(workflow.exists())
        text = workflow.read_text(encoding="utf-8")

        self.assertIn("cron: '0 22 * * *'", text)
        self.assertIn("python3 crawlers/run_public.py", text)
        self.assertIn("python3 db/supabase_ingest.py data/crawled/*.jsonl", text)
        self.assertIn("SUPABASE_SERVICE_ROLE_KEY", text)
        self.assertNotIn("DISCORD_TOKEN", text)
        self.assertNotIn("discord.py", text)

    def test_local_discord_ingest_refuses_github_actions(self):
        mod = importlib.import_module("scripts.local_discord_ingest")

        old_value = os.environ.get("GITHUB_ACTIONS")
        os.environ["GITHUB_ACTIONS"] = "true"
        try:
            with self.assertRaises(SystemExit) as raised:
                mod.ensure_local_only()
        finally:
            if old_value is None:
                os.environ.pop("GITHUB_ACTIONS", None)
            else:
                os.environ["GITHUB_ACTIONS"] = old_value

        self.assertNotEqual(0, raised.exception.code)

    def test_full_runner_refuses_github_actions_because_it_includes_discord(self):
        run_all = importlib.import_module("crawlers.run_all")

        old_value = os.environ.get("GITHUB_ACTIONS")
        os.environ["GITHUB_ACTIONS"] = "true"
        try:
            with self.assertRaises(SystemExit) as raised:
                run_all.ensure_not_github_actions()
        finally:
            if old_value is None:
                os.environ.pop("GITHUB_ACTIONS", None)
            else:
                os.environ["GITHUB_ACTIONS"] = old_value

        self.assertNotEqual(0, raised.exception.code)

    def test_supabase_ingest_cli_is_directly_executable(self):
        result = subprocess.run(
            [sys.executable, "db/supabase_ingest.py", "--help"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=10,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("usage:", result.stdout.lower())


if __name__ == "__main__":
    unittest.main()
