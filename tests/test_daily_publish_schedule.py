from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DailyPublishScheduleTest(unittest.TestCase):
    def test_scheduled_publish_runs_at_0830_kst(self):
        workflow = (ROOT / ".github" / "workflows" / "daily_publish.yml").read_text(encoding="utf-8")

        self.assertIn('# 08:30 KST', workflow)
        self.assertIn('- cron: "30 23 * * *"', workflow)
        self.assertNotIn('- cron: "37 23 * * *"', workflow)
        self.assertNotIn('- cron: "45 23 * * *"', workflow)
        self.assertIn("PLATFORM=both", workflow)
        self.assertIn("DRY_RUN=false", workflow)
        self.assertIn("FORCE=false", workflow)
        self.assertIn("LIMIT=0", workflow)
        self.assertIn('--allow-partial', workflow)

    def test_scheduled_publish_syncs_generated_articles_to_supabase_before_publish(self):
        workflow = (ROOT / ".github" / "workflows" / "daily_publish.yml").read_text(encoding="utf-8")

        self.assertIn("github.event_name == 'schedule' ||", workflow)
        self.assertIn("python scripts/sync_articles_to_supabase.py --input docs/articles.json", workflow)


if __name__ == "__main__":
    unittest.main()
