from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DailyPublishScheduleTest(unittest.TestCase):
    def test_scheduled_publish_runs_both_platforms_at_temporary_2030_kst(self):
        workflow = (ROOT / ".github" / "workflows" / "daily_publish.yml").read_text(encoding="utf-8")

        self.assertIn('# 20:30 KST (11:30 UTC) temporary daily publish test slot.', workflow)
        self.assertIn('- cron: "30 11 * * *"', workflow)
        self.assertIn("PLATFORM=both", workflow)
        self.assertIn("DRY_RUN=false", workflow)
        self.assertIn('--allow-partial', workflow)

    def test_scheduled_publish_syncs_generated_articles_to_supabase_before_publish(self):
        workflow = (ROOT / ".github" / "workflows" / "daily_publish.yml").read_text(encoding="utf-8")

        self.assertIn("github.event_name == 'schedule' ||", workflow)
        self.assertIn("python scripts/sync_articles_to_supabase.py --input docs/articles.json", workflow)


if __name__ == "__main__":
    unittest.main()
