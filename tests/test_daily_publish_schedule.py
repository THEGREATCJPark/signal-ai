from pathlib import Path
import unittest
from datetime import datetime, timedelta, timezone


ROOT = Path(__file__).resolve().parents[1]
KST = timezone(timedelta(hours=9))


class DailyPublishScheduleTest(unittest.TestCase):
    def test_github_scheduled_publish_runs_backup_slots(self):
        workflow = (ROOT / ".github" / "workflows" / "daily_publish.yml").read_text(encoding="utf-8")

        self.assertIn('# 08:37/08:45 KST backup slots', workflow)
        self.assertNotIn('- cron: "30 23 * * *"', workflow)
        self.assertIn('- cron: "37 23 * * *"', workflow)
        self.assertIn('- cron: "45 23 * * *"', workflow)
        self.assertIn("PLATFORM=both", workflow)
        self.assertIn("DRY_RUN=false", workflow)
        self.assertIn("FORCE=false", workflow)
        self.assertIn("LIMIT=0", workflow)
        self.assertIn('--allow-partial', workflow)

    def test_scheduled_publish_syncs_generated_articles_to_supabase_before_publish(self):
        workflow = (ROOT / ".github" / "workflows" / "daily_publish.yml").read_text(encoding="utf-8")

        self.assertIn("github.event_name == 'schedule' ||", workflow)
        self.assertIn("python scripts/validate_articles.py --require-fresh-kst", workflow)
        self.assertIn("python scripts/sync_articles_to_supabase.py --input docs/articles.json", workflow)

    def test_daily_freshness_rejects_previous_day_articles(self):
        from scripts.validate_articles import is_fresh_for_daily_publish

        ok, reason = is_fresh_for_daily_publish(
            {"last_run_at": "2026-05-06T08:00:02+09:00", "generated_at": "2026-05-06T08:00:02+09:00"},
            now=datetime(2026, 5, 7, 8, 30, tzinfo=KST),
        )

        self.assertFalse(ok)
        self.assertIn("stale", reason)

    def test_daily_freshness_accepts_today_after_generation_slot(self):
        from scripts.validate_articles import is_fresh_for_daily_publish

        ok, reason = is_fresh_for_daily_publish(
            {"last_run_at": "2026-05-07T08:00:02+09:00", "generated_at": "2026-05-07T08:00:02+09:00"},
            now=datetime(2026, 5, 7, 8, 30, tzinfo=KST),
        )

        self.assertTrue(ok, reason)


if __name__ == "__main__":
    unittest.main()
