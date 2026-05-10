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
        self.assertIn("--platform telegram", workflow)
        self.assertIn("--platform x", workflow)
        self.assertIn('--allow-partial', workflow)

    def test_scheduled_publish_syncs_generated_articles_to_supabase_before_publish(self):
        workflow = (ROOT / ".github" / "workflows" / "daily_publish.yml").read_text(encoding="utf-8")

        self.assertIn("github.event_name == 'schedule' ||", workflow)
        self.assertIn("python scripts/validate_articles.py --require-fresh-kst", workflow)
        self.assertIn("python scripts/sync_articles_to_supabase.py --input docs/articles.json", workflow)

    def test_scheduled_publish_runs_telegram_even_when_validation_fails_and_x_is_optional(self):
        workflow = (ROOT / ".github" / "workflows" / "daily_publish.yml").read_text(encoding="utf-8")

        self.assertIn("continue-on-error: true", workflow)
        self.assertIn("id: validate", workflow)
        self.assertIn("Run scheduled Telegram publish", workflow)
        self.assertIn("--platform telegram", workflow)
        self.assertIn("Run scheduled X publish", workflow)
        self.assertIn("--platform x", workflow)

    def test_github_daily_articles_uses_cj_gemini_secret(self):
        workflow = (ROOT / ".github" / "workflows" / "daily_articles.yml").read_text(encoding="utf-8")

        self.assertIn('- cron: "0 23 * * *"', workflow)
        self.assertIn("GEMINI_API_KEYS_CJ: ${{ secrets.GEMINI_API_KEYS_CJ }}", workflow)
        self.assertIn("DISCORD_TOKEN: ${{ secrets.DISCORD_TOKEN }}", workflow)
        self.assertIn("python run_hourly.py", workflow)
        self.assertIn("git push", workflow)

    def test_manual_publish_smoke_keeps_telegram_required_and_x_optional(self):
        workflow = (ROOT / ".github" / "workflows" / "manual-publish-smoke.yml").read_text(encoding="utf-8")

        self.assertIn(".github/manual-daily-publish-smoke.txt", workflow)
        self.assertIn("Publish one Telegram article", workflow)
        self.assertIn("--platform telegram", workflow)
        self.assertIn("Publish one X summary optionally", workflow)
        self.assertIn("continue-on-error: true", workflow)
        self.assertIn("--platform x", workflow)
        self.assertIn("TELEGRAM_CHANNEL_ID: ${{ secrets.TELEGRAM_CHANNEL_ID }}", workflow)

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
