import json
import subprocess
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

    def test_classify_and_save_keeps_existing_articles_without_expiry(self):
        run_at = datetime(2026, 4, 20, 12, 0, tzinfo=KST)
        old_created_at = (run_at - timedelta(days=4)).isoformat()
        state = {
            "schema_version": 2,
            "last_run_at": old_created_at,
            "generated_at": old_created_at,
            "journal": "First Light AI",
            "model": run_hourly.MODEL,
            "articles": [
                {
                    "id": "old-mythos",
                    "headline": "Claude Mythos 후폭풍",
                    "body": "오래된 기사도 아카이브에 남아야 한다.",
                    "created_at": old_created_at,
                    "placement": "side",
                    "placed_at": old_created_at,
                }
            ],
            "decision_log": [],
        }
        with tempfile.TemporaryDirectory() as td:
            with (
                patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)),
                patch.object(run_hourly, "save_state"),
                patch.object(run_hourly.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="gist ok\n", stderr="")),
            ):
                run_hourly._classify_and_save(state, [], run_at, sched=None)

        self.assertEqual(len(state["articles"]), 1)
        self.assertEqual(state["articles"][0]["id"], "old-mythos")


if __name__ == "__main__":
    unittest.main()
