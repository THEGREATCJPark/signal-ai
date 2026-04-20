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
    def test_build_daily_summary_payload_uses_fixed_title_and_metadata(self):
        run_at = datetime(2026, 4, 20, 8, 0, tzinfo=KST)
        articles = [{"id": "a1"}, {"id": "a2"}]
        payload = run_hourly.build_daily_summary_payload("오늘의 AI 흐름입니다.", articles, run_at)
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["title"], "Daily Don't Die Summary")
        self.assertEqual(payload["date"], "2026-04-20")
        self.assertEqual(payload["generated_at"], run_at.isoformat())
        self.assertEqual(payload["article_count"], 2)
        self.assertEqual(payload["body"], "오늘의 AI 흐름입니다.")

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
        daily_summary = run_hourly.build_daily_summary_payload("하루 요약", articles, run_at)
        with tempfile.TemporaryDirectory() as td:
            with patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)):
                out = run_hourly.write_daily_new_articles_export(articles, run_at, daily_summary)
                self.assertEqual(out, Path(td) / "2026-04-20.json")
                payload = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["journal"], "First Light AI")
        self.assertEqual(payload["date"], "2026-04-20")
        self.assertEqual(payload["generated_at"], run_at.isoformat())
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["articles"][0]["headline"], "새 기사")
        self.assertEqual(payload["daily_summary"]["title"], "Daily Don't Die Summary")
        self.assertEqual(payload["daily_summary"]["body"], "하루 요약")

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

    def test_classify_and_save_stores_daily_summary(self):
        run_at = datetime(2026, 4, 20, 8, 0, tzinfo=KST)
        state = {
            "schema_version": 2,
            "last_run_at": None,
            "generated_at": run_at.isoformat(),
            "journal": "First Light AI",
            "model": run_hourly.MODEL,
            "articles": [],
            "decision_log": [],
        }
        new_articles = [
            {
                "id": "new-1",
                "headline": "새 모델 공개",
                "body": "새 모델이 공개됐다는 소식입니다.",
                "category": "news",
                "trust": "high",
                "created_at": run_at.isoformat(),
                "placement": "side",
                "placed_at": run_at.isoformat(),
            }
        ]
        summary = run_hourly.build_daily_summary_payload("오늘은 새 모델 공개가 중심입니다.", new_articles, run_at)
        with tempfile.TemporaryDirectory() as td:
            with (
                patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)),
                patch.object(run_hourly, "save_state"),
                patch.object(run_hourly, "generate_daily_summary", return_value=summary),
                patch.object(run_hourly, "call_gemma", return_value='{"top": null, "main": [], "side": ["1"]}'),
                patch.object(run_hourly.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="gist ok\n", stderr="")),
            ):
                run_hourly._classify_and_save(state, new_articles, run_at, sched=None)

        self.assertEqual(state["daily_summary"]["title"], "Daily Don't Die Summary")
        self.assertEqual(state["daily_summary"]["body"], "오늘은 새 모델 공개가 중심입니다.")


if __name__ == "__main__":
    unittest.main()
