import unittest
from pathlib import Path


class CronWrapperTest(unittest.TestCase):
    def test_cron_wrapper_loads_local_secret_env(self):
        text = Path("run_cron.sh").read_text(encoding="utf-8")
        self.assertIn("discord_export_config.env", text)
        self.assertIn("set -a", text)
        self.assertIn("set +a", text)
        self.assertIn("run_hourly.py", text)
        self.assertIn("SCRIPT_DIR", text)
        self.assertIn("FIRST_LIGHT_PYTHON", text)
        self.assertNotIn("cd /home/pineapple/bunjum2/signal", text)
        self.assertNotIn("/home/pineapple/miniconda3/bin/python3 run_hourly.py", text)

    def test_task_wrapper_uses_catchup_gate(self):
        text = Path("run_cron_task.sh").read_text(encoding="utf-8")
        self.assertIn("scripts/automation_gate.py", text)
        self.assertIn("./run_cron.sh", text)
        self.assertIn("FIRST_LIGHT_PYTHON", text)
        self.assertNotIn("/tmp/signal_daily.log", text)

    def test_local_handoff_task_wrapper_exposes_local_bin_for_cloudflared(self):
        text = Path("run_local_crawl_handoff_task.sh").read_text(encoding="utf-8")

        self.assertIn("/home/pineapple/bin", text)
        self.assertIn("PATH=", text)
        self.assertIn("FIRST_LIGHT_PYTHON", text)
        self.assertIn("scripts/local_crawl_handoff_gate.py", text)
        self.assertIn("scripts/dispatch_local_crawl_handoff.py", text)


if __name__ == "__main__":
    unittest.main()
