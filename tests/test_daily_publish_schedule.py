from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DailyPublishScheduleTest(unittest.TestCase):
    def test_scheduled_publish_runs_both_platforms_at_0830_kst(self):
        workflow = (ROOT / ".github" / "workflows" / "daily_publish.yml").read_text(encoding="utf-8")

        self.assertIn('# 08:30 KST (23:30 UTC previous day) daily publish slot.', workflow)
        self.assertIn('- cron: "30 23 * * *"', workflow)
        self.assertIn("PLATFORM=both", workflow)
        self.assertIn("DRY_RUN=false", workflow)


if __name__ == "__main__":
    unittest.main()
