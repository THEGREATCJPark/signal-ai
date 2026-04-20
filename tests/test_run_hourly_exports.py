import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import run_hourly

KST = timezone(timedelta(hours=9))


class DailyExportsTest(unittest.TestCase):
    def test_write_daily_new_articles_export_uses_date_folder_and_metadata(self):
        run_at = datetime(2026, 4, 20, 12, 0, tzinfo=KST)
        articles = [
            {
                "id": "art-202604201200-01",
                "headline": "새 기사",
                "body": "본문",
                "category": "news",
                "trust": "high",
                "created_at": run_at.isoformat(),
                "placement": "side",
            }
        ]
        with tempfile.TemporaryDirectory() as td:
            with patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)):
                out = run_hourly.write_daily_new_articles_export(articles, run_at)
                self.assertEqual(out, Path(td) / "2026-04-20.json")
                payload = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["journal"], "First Light AI")
        self.assertEqual(payload["date"], "2026-04-20")
        self.assertEqual(payload["generated_at"], run_at.isoformat())
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["articles"][0]["headline"], "새 기사")

    def test_write_daily_new_articles_export_writes_empty_run(self):
        run_at = datetime(2026, 4, 20, 12, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as td:
            with patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)):
                out = run_hourly.write_daily_new_articles_export([], run_at)
                payload = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["articles"], [])


if __name__ == "__main__":
    unittest.main()
