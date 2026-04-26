import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class IngestAutomationTest(unittest.TestCase):
    def test_public_runner_excludes_discord(self):
        run_public = importlib.import_module("crawlers.run_public")

        self.assertIn("hn.py", run_public.PUBLIC_CRAWLERS)
        self.assertIn("reddit.py", run_public.PUBLIC_CRAWLERS)
        self.assertNotIn("discord.py", run_public.PUBLIC_CRAWLERS)
        self.assertNotIn("discord", " ".join(run_public.PUBLIC_CRAWLERS).lower())

    def test_github_actions_has_no_crawl_workflow(self):
        workflow = ROOT / ".github" / "workflows" / "crawl.yml"

        self.assertFalse(workflow.exists())

    def test_pages_workflow_deploys_dev_publish_commits_only(self):
        workflow = ROOT / ".github" / "workflows" / "deploy-pages.yml"
        text = workflow.read_text(encoding="utf-8")

        self.assertIn("branches: [dev]", text)
        self.assertIn("startsWith(github.event.head_commit.message, 'chore: publish First Light AI')", text)
        self.assertNotIn("environment:", text)
        self.assertIn("mkdir -p _site", text)
        self.assertIn("cp index.html archive.html articles.json .nojekyll _site/", text)
        self.assertIn("cp -R exports _site/exports", text)

    def test_local_all_source_ingest_refuses_github_actions(self):
        mod = importlib.import_module("scripts.local_crawl_ingest")

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

    def test_local_all_source_ingest_includes_public_and_discord_crawlers(self):
        mod = importlib.import_module("scripts.local_crawl_ingest")

        self.assertIn("crawlers/run_public.py", mod.CRAWLER_COMMANDS)
        self.assertIn("crawlers/discord.py", mod.CRAWLER_COMMANDS)

    def test_discord_crawler_uses_linux_exporter_when_powershell_is_unavailable(self):
        mod = importlib.import_module("crawlers.discord")

        with patch.object(
            mod.subprocess,
            "run",
            return_value=subprocess.CompletedProcess(["which", "powershell.exe"], 1, "", ""),
        ):
            cmd = mod.export_command("2026-04-27 00:00:00")

        self.assertIn("discord_export_linux.py", cmd[1])
        self.assertNotIn("discord_export_text_only.py", " ".join(cmd))
        self.assertNotIn("--no-upload", cmd)
        self.assertIn("--channel", cmd)
        self.assertIn("--after-kst", cmd)

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

    def test_readme_names_supabase_ingest_destination(self):
        text = (ROOT / "README.md").read_text(encoding="utf-8")

        self.assertIn("## Supabase 적재 대상", text)
        self.assertIn("Project ref: `qyckjkidscpiyrdzqxoc`", text)
        self.assertIn("Table: `public.posts`", text)
        self.assertIn("환경변수 `SUPABASE_URL`이 실제 적재 프로젝트를 결정", text)
        self.assertIn("service_role key의 소유 Supabase 계정/조직 권한", text)

    def test_local_handoff_workflow_ingests_bundle_without_crawling(self):
        workflow = ROOT / ".github" / "workflows" / "local-crawl-handoff.yml"
        text = workflow.read_text(encoding="utf-8")

        self.assertIn("branches: [dev]", text)
        self.assertIn(".github/local-crawl-handoff-trigger", text)
        self.assertIn("startsWith(github.event.head_commit.message, 'chore: trigger local crawl handoff')", text)
        self.assertIn("workflow_dispatch:", text)
        self.assertIn("bundle_url:", text)
        self.assertIn("LOCAL_CRAWL_BUNDLE_URL", text)
        self.assertIn("curl", text)
        self.assertIn("python3 db/supabase_ingest.py", text)
        self.assertIn("data/crawled/*.jsonl", text)
        self.assertIn("SUPABASE_URL: ${{ secrets.SUPABASE_URL }}", text)
        self.assertIn("SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}", text)
        self.assertNotIn("crawlers/run_all.py", text)
        self.assertNotIn("crawlers/discord.py", text)
        self.assertNotIn("DISCORD_TOKEN", text)

    def test_local_handoff_dispatcher_contract(self):
        mod = importlib.import_module("scripts.dispatch_local_crawl_handoff")

        self.assertIn("crawlers/run_public.py", mod.CRAWLER_COMMANDS)
        self.assertIn("crawlers/discord.py", mod.CRAWLER_COMMANDS)
        self.assertEqual("local-crawl-handoff.yml", mod.DEFAULT_WORKFLOW)
        self.assertEqual("THEGREATCJPark/signal-ai", mod.DEFAULT_REPO)
        self.assertEqual("dev", mod.DEFAULT_REF)
        self.assertEqual("secret-push", mod.DEFAULT_TRIGGER_MODE)
        self.assertEqual("LOCAL_CRAWL_BUNDLE_URL", mod.BUNDLE_URL_SECRET)
        self.assertEqual(ROOT / ".github" / "local-crawl-handoff-trigger", mod.TRIGGER_FILE)
        self.assertEqual("https://example.trycloudflare.com", mod.parse_tunnel_url(
            "2026 INF TryCloudflare: https://example.trycloudflare.com"
        ))

        old_value = os.environ.get("GITHUB_ACTIONS")
        os.environ["GITHUB_ACTIONS"] = "true"
        try:
            with self.assertRaises(SystemExit):
                mod.ensure_local_only()
        finally:
            if old_value is None:
                os.environ.pop("GITHUB_ACTIONS", None)
            else:
                os.environ["GITHUB_ACTIONS"] = old_value

    def test_supabase_ingest_deduplicates_conflicts_within_batch(self):
        ingest = importlib.import_module("db.ingest")

        rows = [
            {
                "source": "discord",
                "source_id": "same-id",
                "content": "old",
                "timestamp": "2026-04-25T00:00:00+00:00",
            },
            {
                "source": "discord",
                "source_id": "same-id",
                "content": "new",
                "timestamp": "2026-04-25T00:01:00+00:00",
            },
            {
                "source": "hackernews",
                "source_id": "hn-1",
                "content": "hn",
                "timestamp": "2026-04-25T00:02:00+00:00",
            },
        ]

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "posts.jsonl"
            path.write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )
            flushed = []

            with patch.object(ingest, "upsert_posts", side_effect=lambda batch: flushed.append(batch) or len(batch)):
                result = ingest.ingest_paths([path], batch_size=500)

        self.assertEqual(2, result["inserted"])
        self.assertEqual({"discord": 1, "hackernews": 1}, result["by_source"])
        self.assertEqual(1, len(flushed))
        self.assertEqual(2, len(flushed[0]))
        discord_row = next(row for row in flushed[0] if row["source"] == "discord")
        self.assertEqual("new", discord_row["content"])


if __name__ == "__main__":
    unittest.main()
