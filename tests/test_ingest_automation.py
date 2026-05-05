from __future__ import annotations

import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class IngestAutomationTests(unittest.TestCase):
    def test_public_crawler_runner_excludes_discord(self) -> None:
        import crawlers.run_public as run_public

        crawlers = list(run_public.PUBLIC_CRAWLERS)
        self.assertNotIn("discord.py", crawlers)
        self.assertIn("hn.py", crawlers)
        self.assertIn("x_watch.py", crawlers)

    def test_local_handoff_workflow_targets_main_and_ingests_bundle_only(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "local-crawl-handoff.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn("branches: [main]", workflow)
        self.assertIn("LOCAL_CRAWL_BUNDLE_URL", workflow)
        self.assertIn("python3 db/supabase_ingest.py", workflow)
        self.assertNotIn("branches: [dev]", workflow)
        self.assertNotIn("python crawlers/", workflow)
        self.assertNotIn("DISCORD_TOKEN", workflow)

    def test_handoff_dispatcher_defaults_to_main(self) -> None:
        import scripts.dispatch_local_crawl_handoff as dispatcher

        self.assertEqual(dispatcher.DEFAULT_REF, "main")
        self.assertIn("ai-frontier-handoff-", Path(dispatcher.__file__).read_text(encoding="utf-8"))

    def test_local_handoff_gate_imports_and_records_success(self) -> None:
        module = importlib.import_module("scripts.local_crawl_handoff_gate")

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            calls: list[list[str]] = []

            def runner(command: list[str], **_: object) -> int:
                calls.append(command)
                return 0

            rc = module.run_command_if_due(
                root=ROOT,
                state_path=tmp_path / "state.json",
                log_path=tmp_path / "handoff.log",
                lock_path=tmp_path / "handoff.lock",
                now=module.datetime(2026, 5, 5, 7, 30, tzinfo=module.KST),
                command=["python", "handoff.py"],
                runner=runner,
            )

            self.assertEqual(rc, 0)
            self.assertEqual(calls, [["python", "handoff.py"]])
            state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "success")
            self.assertEqual(state["task"], "local_crawl_handoff")

    def test_local_ingest_scripts_use_supabase_ingest(self) -> None:
        crawl_script = (ROOT / "scripts" / "local_crawl_ingest.py").read_text(encoding="utf-8")
        discord_script = (ROOT / "scripts" / "local_discord_ingest.py").read_text(encoding="utf-8")

        self.assertIn("from db.supabase_ingest import ingest_paths", crawl_script)
        self.assertIn("from db.supabase_ingest import ingest_paths", discord_script)
        self.assertNotIn("from db.ingest import ingest_paths", crawl_script)
        self.assertNotIn("from db.ingest import ingest_paths", discord_script)

    def test_local_ingest_refuses_github_actions_by_default(self) -> None:
        import scripts.local_crawl_ingest as local_crawl_ingest

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}):
            with self.assertRaises(SystemExit):
                local_crawl_ingest.ensure_local_only()

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}):
            local_crawl_ingest.ensure_local_only(allow_ci=True)

    def test_discord_crawler_uses_linux_exporter_without_powershell(self) -> None:
        import crawlers.discord as discord_crawler

        completed = type("Completed", (), {"returncode": 1})()
        with patch.object(discord_crawler.subprocess, "run", return_value=completed):
            command = discord_crawler.export_command("2026-05-05")

        self.assertEqual(command[0], "python3")
        self.assertTrue(command[1].endswith("discord_export_linux.py"))
        self.assertIn("--after-kst", command)

    def test_supabase_ingest_dedupes_duplicate_source_ids_within_batch(self) -> None:
        import db.supabase_ingest as supabase_ingest

        rows = [
            {"source": "x", "source_id": "1", "content": "old", "timestamp": "2026-05-05T00:00:00Z"},
            {"source": "x", "source_id": "1", "content": "new", "timestamp": "2026-05-05T00:01:00Z"},
            {"source": "rss", "source_id": "2", "content": "rss", "timestamp": "2026-05-05T00:02:00Z"},
        ]

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "posts.jsonl"
            path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
            captured: list[list[dict]] = []

            def fake_upsert(batch: list[dict]) -> int:
                captured.append(batch)
                return len(batch)

            with patch.object(supabase_ingest, "upsert_posts", side_effect=fake_upsert):
                result = supabase_ingest.ingest_paths([path], batch_size=10)

        self.assertEqual(result["inserted"], 2)
        self.assertEqual(result["by_source"], {"x": 1, "rss": 1})
        self.assertEqual(captured[0][0]["content"], "new")


if __name__ == "__main__":
    unittest.main()
