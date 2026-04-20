import importlib
import json
import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class SupabaseClientTest(unittest.TestCase):
    def setUp(self):
        self._old_supabase = sys.modules.get("supabase")
        self.calls = []

        fake = types.ModuleType("supabase")

        def create_client(url, key):
            self.calls.append((url, key))
            return {"url": url, "key": key}

        fake.create_client = create_client
        sys.modules["supabase"] = fake

    def tearDown(self):
        if self._old_supabase is None:
            sys.modules.pop("supabase", None)
        else:
            sys.modules["supabase"] = self._old_supabase

    def test_get_client_uses_anon_for_reads_and_service_role_for_writes(self):
        with patch.dict(
            os.environ,
            {
                "SUPABASE_URL": "https://example.supabase.co",
                "SUPABASE_ANON_KEY": "anon-key",
                "SUPABASE_SERVICE_ROLE_KEY": "service-key",
            },
            clear=False,
        ):
            import db.client as client

            importlib.reload(client)
            read_client = client.get_client()
            write_client = client.get_client(service=True)

        self.assertEqual(read_client["key"], "anon-key")
        self.assertEqual(write_client["key"], "service-key")
        self.assertEqual(
            self.calls,
            [
                ("https://example.supabase.co", "anon-key"),
                ("https://example.supabase.co", "service-key"),
            ],
        )

    def test_get_client_requires_service_role_for_writes(self):
        with patch.dict(
            os.environ,
            {"SUPABASE_URL": "https://example.supabase.co", "SUPABASE_ANON_KEY": "anon-key"},
            clear=True,
        ):
            import db.client as client

            importlib.reload(client)
            with self.assertRaisesRegex(RuntimeError, "SUPABASE_SERVICE_ROLE_KEY"):
                client.get_client(service=True)


class MigrationContractTest(unittest.TestCase):
    def test_posts_and_state_migrations_define_the_supabase_contract(self):
        root = Path(__file__).resolve().parents[1]
        posts = (root / "migrations" / "007_create_posts.sql").read_text(encoding="utf-8")
        rls = (root / "migrations" / "008_tighten_rls.sql").read_text(encoding="utf-8")
        rpc = (root / "migrations" / "009_rpc_recent_posts.sql").read_text(encoding="utf-8")
        state = (root / "migrations" / "010_create_pipeline_state.sql").read_text(encoding="utf-8")

        self.assertIn("create table if not exists public.posts", posts.lower())
        self.assertIn("unique (source, source_id)", posts.lower())
        self.assertIn("idx_posts_source_timestamp", posts.lower())
        self.assertIn("idx_posts_fetched_at", posts.lower())
        self.assertIn("idx_posts_parent_id", posts.lower())
        self.assertIn("alter table public.posts enable row level security", rls.lower())
        self.assertIn("revoke all on public.posts from anon", rls.lower())
        self.assertIn("grant all on public.posts to service_role", rls.lower())
        self.assertIn("get_recent_posts_by_source", rpc)
        self.assertRegex(rpc.lower(), r"row_number\(\)\s+over\s*\(\s*partition by p\.source")
        self.assertIn("metadata->>'points'", rpc)
        self.assertIn("public.pipeline_state", state.lower())


class PostsIngestTest(unittest.TestCase):
    def test_ingest_paths_normalizes_jsonl_and_upserts_posts(self):
        import db.ingest as ingest

        captured_batches = []

        def fake_upsert(rows):
            captured_batches.append(rows)
            return len(rows)

        rows = [
            {
                "source": "discord",
                "source_id": 123,
                "source_url": "https://discord.test/message/123",
                "author": "user",
                "content": "새 모델 소식",
                "timestamp": "2026-04-20T08:00:00+09:00",
                "parent_id": None,
                "metadata": {"score": 7},
            },
            {"bad": "row"},
        ]
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "posts.jsonl"
            path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
            with patch.object(ingest, "upsert_posts", side_effect=fake_upsert):
                result = ingest.ingest_paths([path], batch_size=10)

        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(result["by_source"], {"discord": 1})
        self.assertEqual(captured_batches[0][0]["source_id"], "123")
        self.assertEqual(captured_batches[0][0]["metadata"], {"score": 7})
        self.assertEqual(captured_batches[0][0]["fetched_at"], "2026-04-20T08:00:00+09:00")

    def test_posts_rpc_returns_recent_rows(self):
        import db.posts as posts

        class ExecuteResult:
            data = [{"source": "discord", "content": "소식", "score": 10}]

        class RpcCall:
            def execute(self):
                return ExecuteResult()

        class FakeClient:
            def __init__(self):
                self.calls = []

            def rpc(self, name, params):
                self.calls.append((name, params))
                return RpcCall()

        fake = FakeClient()
        with patch.object(posts, "get_client", return_value=fake):
            rows = posts.list_recent_posts_by_source(days=2, per_source=5)

        self.assertEqual(rows, [{"source": "discord", "content": "소식", "score": 10}])
        self.assertEqual(fake.calls, [("get_recent_posts_by_source", {"days": 2, "per_source": 5})])


class PipelineUsesSupabaseTest(unittest.TestCase):
    def test_run_full_query_context_reads_posts_via_rpc(self):
        import run_full

        rows = [
            {"source": "discord", "content": "첫 번째 업데이트\n본문", "metadata": {"score": 9}, "timestamp": "2026-04-20T08:00:00+09:00", "author": "u", "source_url": "u"},
            {"source": "hackernews", "content": "두 번째 업데이트", "metadata": {"points": 12}, "timestamp": "2026-04-20T08:00:00+09:00", "author": "u", "source_url": "u"},
        ]
        with patch.object(run_full, "list_recent_posts_by_source", return_value=rows):
            context, counts = run_full.step_query_context(days=3, per_source=15)

        self.assertIn("[discord] (9) 첫 번째 업데이트 본문", context)
        self.assertIn("[hackernews] (12) 두 번째 업데이트", context)
        self.assertEqual(counts, {"discord": 1, "hackernews": 1})

    def test_run_hourly_load_and_save_state_use_supabase_public_state(self):
        import run_hourly

        state = {
            "schema_version": 2,
            "journal": "First Light AI",
            "last_run_at": "2026-04-20T08:00:00+09:00",
            "generated_at": "2026-04-20T08:00:00+09:00",
            "articles": [{"id": "a1", "headline": "기사", "body": "본문"}],
            "decision_log": [],
        }
        saved = []
        with tempfile.TemporaryDirectory() as td:
            docs_path = Path(td) / "docs" / "articles.json"
            pages_path = Path(td) / "articles.json"
            docs_path.parent.mkdir()
            with (
                patch.object(run_hourly, "ARTICLES_PATH", docs_path),
                patch.object(run_hourly, "PAGES_ARTICLES_PATH", pages_path),
                patch.object(run_hourly, "load_public_state", return_value=state),
                patch.object(run_hourly, "save_public_state", side_effect=lambda s: saved.append(s)),
            ):
                loaded = run_hourly.load_state()
                run_hourly.save_state(state)

            self.assertEqual(loaded, state)
            self.assertEqual(saved, [state])
            self.assertEqual(json.loads(docs_path.read_text(encoding="utf-8")), state)
            self.assertEqual(json.loads(pages_path.read_text(encoding="utf-8")), state)


class SQLiteRemovalTest(unittest.TestCase):
    def test_runtime_pipeline_no_longer_uses_sqlite_modules(self):
        root = Path(__file__).resolve().parents[1]
        self.assertFalse((root / "db" / "query.py").exists())
        self.assertFalse((root / "db" / "schema.sql").exists())
        for rel in ("run_full.py", "db/ingest.py"):
            text = (root / rel).read_text(encoding="utf-8")
            self.assertNotIn("sqlite3", text)
            self.assertNotIn("signal.db", text)


if __name__ == "__main__":
    unittest.main()
