import unittest
from pathlib import Path


class CronWrapperTest(unittest.TestCase):
    def test_cron_wrapper_loads_local_secret_env(self):
        text = Path("run_cron.sh").read_text(encoding="utf-8")
        self.assertIn("discord_export_config.env", text)
        self.assertIn("set -a", text)
        self.assertIn("set +a", text)
        self.assertIn("run_hourly.py", text)

    def test_task_wrapper_uses_catchup_gate(self):
        text = Path("run_cron_task.sh").read_text(encoding="utf-8")
        self.assertIn("scripts/automation_gate.py", text)
        self.assertIn("./run_cron.sh", text)
        self.assertNotIn("/tmp/signal_daily.log", text)

    def test_daily_publish_task_runs_local_publisher(self):
        text = Path("run_daily_publish_task.sh").read_text(encoding="utf-8")
        self.assertIn("scripts/daily_publish_local.py", text)
        self.assertIn("--platform both", text)
        self.assertIn("--allow-partial", text)

    def test_windows_daily_publish_task_runs_python_publisher(self):
        text = Path("run_daily_publish_task.ps1").read_text(encoding="utf-8")
        self.assertIn("scripts\\daily_publish_local.py", text)
        self.assertIn("--platform both", text)
        self.assertIn("signal_daily_publish.log", text)

    def test_windows_daily_articles_task_runs_python_gate(self):
        text = Path("run_daily_articles_task.ps1").read_text(encoding="utf-8")
        self.assertIn("scripts\\automation_gate.py", text)
        self.assertIn("run_hourly.py", text)
        self.assertIn("signal_daily_articles.log", text)

    def test_windows_local_crawl_handoff_task_runs_python_gate(self):
        text = Path("run_local_crawl_handoff_task.ps1").read_text(encoding="utf-8")
        self.assertIn("scripts\\local_crawl_handoff_gate.py", text)
        self.assertIn("scripts\\dispatch_local_crawl_handoff.py", text)
        self.assertIn("DISCORD_TOKEN", text)


if __name__ == "__main__":
    unittest.main()
