import importlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class PublishPipelineTest(unittest.TestCase):
    def test_run_publish_can_load_articles_from_supabase_public_state(self):
        run_publish = importlib.import_module("scripts.run_publish")
        state = {
            "generated_at": "2026-04-25T08:00:00+09:00",
            "source": "first_light_ai",
            "articles": [
                {
                    "id": "art-1",
                    "headline": "Supabase에서 읽은 기사",
                    "body": "본문 전체",
                    "score": 7,
                }
            ],
        }

        with patch("db.articles.load_public_state", return_value=state):
            raw = run_publish.load_raw_articles("supabase", Path("unused.json"))

        articles = run_publish.normalize_articles(raw)
        self.assertEqual("art-1", articles[0]["id"])
        self.assertEqual("Supabase에서 읽은 기사", articles[0]["title"])

    def test_sync_articles_to_supabase_saves_full_public_state(self):
        sync = importlib.import_module("scripts.sync_articles_to_supabase")
        state = {
            "generated_at": "2026-04-25T08:00:00+09:00",
            "journal": "First Light AI",
            "articles": [{"id": "art-1", "headline": "기사", "body": "본문"}],
        }

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "articles.json"
            path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

            with patch.object(sync, "save_public_state") as save_public_state:
                count = sync.sync_articles(path, dry_run=False)

        self.assertEqual(1, count)
        save_public_state.assert_called_once_with(state)

    def test_sync_articles_to_supabase_dry_run_does_not_write(self):
        sync = importlib.import_module("scripts.sync_articles_to_supabase")
        state = {"articles": [{"id": "art-1"}]}

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "articles.json"
            path.write_text(json.dumps(state), encoding="utf-8")

            with patch.object(sync, "save_public_state") as save_public_state:
                count = sync.sync_articles(path, dry_run=True)

        self.assertEqual(1, count)
        save_public_state.assert_not_called()

    def test_scheduler_uses_supabase_state_facade_when_requested(self):
        scheduler = importlib.import_module("bot.scheduler")

        class FakeState:
            def __init__(self):
                self.requested = []

            def get_unpublished(self, articles, platform):
                self.requested.append((platform, [a["id"] for a in articles]))
                return articles[:1]

        fake_state = FakeState()

        with patch.object(scheduler, "get_state", return_value=fake_state):
            scheduler.publish(
                [
                    {"id": "a1", "title": "첫 기사", "score": 2},
                    {"id": "a2", "title": "둘째 기사", "score": 1},
                ],
                dry_run=True,
                platform="telegram",
            )

        self.assertEqual([("telegram", ["a1", "a2"])], fake_state.requested)

    def test_db_published_state_matches_json_state_interface(self):
        publish_log = importlib.import_module("db.publish_log")
        rows_by_platform = {
            "telegram": [{"article_id": "old"}],
            "x": [],
        }
        marks = []

        with (
            patch.object(publish_log, "list_published", side_effect=lambda platform=None: rows_by_platform.get(platform, [])),
            patch.object(publish_log, "mark_published", side_effect=lambda article_id, platform, message_id=None: marks.append((article_id, platform, message_id))),
        ):
            state = publish_log.DBPublishedState()
            articles = [{"id": "old", "title": "구 기사"}, {"id": "new", "title": "새 기사"}]

            self.assertTrue(state.is_published("old", "telegram"))
            self.assertFalse(state.is_published("old", "x"))
            self.assertEqual(["new"], [a["id"] for a in state.get_unpublished(articles, "telegram")])
            state.mark_published("new", "telegram", message_id="m1")
            state.save()

        self.assertEqual([("new", "telegram", "m1")], marks)

    def test_daily_publish_workflow_is_manual_dry_run_supabase_connection(self):
        text = (ROOT / ".github" / "workflows" / "daily_publish.yml").read_text(encoding="utf-8")

        self.assertIn("workflow_dispatch:", text)
        self.assertNotIn("schedule:", text)
        self.assertIn("default: 'true'", text)
        self.assertIn("source:", text)
        self.assertIn("default: 'supabase'", text)
        self.assertIn("python scripts/sync_articles_to_supabase.py", text)
        self.assertIn("--source ${{ github.event.inputs.source || 'supabase' }}", text)
        self.assertIn("SUPABASE_URL: ${{ secrets.SUPABASE_URL }}", text)
        self.assertIn("SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}", text)
        self.assertIn("SUPABASE_ANON_KEY: ${{ secrets.SUPABASE_ANON_KEY }}", text)
        self.assertIn('USE_DB: "true"', text)
        self.assertNotIn("Commit published state", text)
        self.assertNotIn("data/published.json", text)


if __name__ == "__main__":
    unittest.main()
