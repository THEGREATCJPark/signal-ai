import unittest
from pathlib import Path


class CronWrapperTest(unittest.TestCase):
    def test_cron_wrapper_loads_local_secret_env(self):
        text = Path("run_cron.sh").read_text(encoding="utf-8")
        self.assertIn("discord_export_config.env", text)
        self.assertIn("set -a", text)
        self.assertIn("set +a", text)
        self.assertIn("run_hourly.py", text)


if __name__ == "__main__":
    unittest.main()
