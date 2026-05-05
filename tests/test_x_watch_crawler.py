import importlib
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
KST = timezone(timedelta(hours=9))


SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0">
  <channel>
    <title>OpenAI / @OpenAI</title>
    <ttl>40</ttl>
    <item>
      <title>R to @OpenAI: Curious about Codex? It&apos;s time to switch.</title>
      <dc:creator>@OpenAI</dc:creator>
      <description><![CDATA[<p>Curious about <b>Codex</b>?<br>It&apos;s time to switch.</p>]]></description>
      <pubDate>Fri, 01 May 2026 19:05:50 GMT</pubDate>
      <guid isPermaLink="false">2050290619684393152</guid>
      <link>https://nitter.net/OpenAI/status/2050290619684393152#m</link>
    </item>
  </channel>
</rss>
"""


class XWatchCrawlerTest(unittest.TestCase):
    def test_requested_watch_accounts_are_grouped_and_complete(self):
        mod = importlib.import_module("crawlers.x_watch")

        by_handle = {account.handle.lower(): account.group for account in mod.WATCH_ACCOUNTS}

        self.assertEqual("official", by_handle["openai"])
        self.assertEqual("official", by_handle["openaidevs"])
        self.assertEqual("official", by_handle["anthropicai"])
        self.assertEqual("official", by_handle["claudeai"])
        self.assertEqual("official", by_handle["claudedevs"])
        self.assertEqual("official", by_handle["googledeepmind"])
        self.assertEqual("official", by_handle["googleai"])
        self.assertEqual("official", by_handle["xai"])

        self.assertEqual("rumor_detection", by_handle["testingcatalog"])
        self.assertEqual("rumor_detection", by_handle["btibor91"])
        self.assertEqual("rumor_detection", by_handle["arena"])
        self.assertEqual("rumor_detection", by_handle["artificialanlys"])

        self.assertEqual("key_people", by_handle["sama"])
        self.assertEqual("key_people", by_handle["karpathy"])
        self.assertEqual("key_people", by_handle["demishassabis"])
        self.assertEqual("key_people", by_handle["gdb"])
        self.assertEqual("key_people", by_handle["polynoamial"])
        self.assertEqual("key_people", by_handle["officiallogank"])

        self.assertEqual("scoops", by_handle["steph_palazzolo"])
        self.assertEqual("scoops", by_handle["alexeheath"])
        self.assertEqual("scoops", by_handle["aaronpholmes"])

    def test_parse_nitter_rss_items_into_normalized_posts(self):
        mod = importlib.import_module("crawlers.x_watch")
        account = mod.WatchedAccount("OpenAI", "official")

        posts = mod.parse_nitter_rss(SAMPLE_RSS, account, "https://nitter.net")

        self.assertEqual(1, len(posts))
        row = posts[0]
        self.assertEqual("x_watch", row["source"])
        self.assertEqual("2050290619684393152", row["source_id"])
        self.assertEqual("https://x.com/OpenAI/status/2050290619684393152", row["source_url"])
        self.assertEqual("@OpenAI", row["author"])
        self.assertEqual("Curious about Codex? It's time to switch.", row["content"])
        self.assertEqual("2026-05-01T19:05:50+00:00", row["timestamp"])
        self.assertEqual("OpenAI", row["metadata"]["watched_account"])
        self.assertEqual("official", row["metadata"]["watch_group"])
        self.assertEqual("reply", row["metadata"]["tweet_kind"])
        self.assertEqual("https://nitter.net/OpenAI/status/2050290619684393152#m", row["metadata"]["nitter_link"])

    def test_fetch_account_tries_next_nitter_instance_after_failure(self):
        mod = importlib.import_module("crawlers.x_watch")
        account = mod.WatchedAccount("OpenAI", "official")
        calls = []

        class Response:
            text = SAMPLE_RSS
            headers = {"content-type": "application/rss+xml"}

            def raise_for_status(self):
                return None

        class Session:
            def get(self, url, **kwargs):
                calls.append(url)
                if url.startswith("https://bad.example"):
                    raise mod.requests.RequestException("blocked")
                return Response()

        posts = mod.fetch_account(
            account,
            ["https://bad.example", "https://nitter.net"],
            session=Session(),
            request_delay_seconds=0,
        )

        self.assertEqual(2, len(calls))
        self.assertEqual(1, len(posts))
        self.assertTrue(calls[0].startswith("https://bad.example/OpenAI/rss"))
        self.assertTrue(calls[1].startswith("https://nitter.net/OpenAI/rss"))

    def test_fetch_account_can_fall_back_to_official_x_api_when_token_is_set(self):
        mod = importlib.import_module("crawlers.x_watch")
        account = mod.WatchedAccount("OpenAI", "official")
        calls = []

        class Response:
            def __init__(self, payload):
                self._payload = payload
                self.text = json.dumps(payload)
                self.headers = {"content-type": "application/json"}

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        class Session:
            def get(self, url, **kwargs):
                calls.append((url, kwargs.get("headers") or {}))
                if url.startswith("https://bad.example"):
                    raise mod.requests.RequestException("blocked")
                if "/2/users/by/username/OpenAI" in url:
                    return Response({"data": {"id": "42", "username": "OpenAI"}})
                if "/2/users/42/tweets" in url:
                    return Response({
                        "data": [{
                            "id": "999",
                            "text": "API fallback post",
                            "created_at": "2026-05-05T01:02:03.000Z",
                            "public_metrics": {"like_count": 7},
                        }]
                    })
                raise AssertionError(f"unexpected URL: {url}")

        posts = mod.fetch_account(
            account,
            ["https://bad.example"],
            session=Session(),
            request_delay_seconds=0,
            x_bearer_token="token-value",
        )

        self.assertEqual(1, len(posts))
        self.assertEqual("999", posts[0]["source_id"])
        self.assertEqual("https://x.com/OpenAI/status/999", posts[0]["source_url"])
        self.assertEqual("official_x_api", posts[0]["metadata"]["fetch_backend"])
        self.assertTrue(any(headers.get("Authorization") == "Bearer token-value" for _, headers in calls))

    def test_public_runner_includes_x_watch_without_discord(self):
        run_public = importlib.import_module("crawlers.run_public")

        self.assertIn("x_watch.py", run_public.PUBLIC_CRAWLERS)
        self.assertNotIn("discord.py", run_public.PUBLIC_CRAWLERS)

    def test_local_x_watch_ingest_refuses_github_actions(self):
        mod = importlib.import_module("scripts.x_watch_ingest")

        old_value = os.environ.get("GITHUB_ACTIONS")
        os.environ["GITHUB_ACTIONS"] = "true"
        try:
            with self.assertRaises(SystemExit):
                mod.ensure_local_only()
        finally:
            if old_value is None:
                os.environ.pop("GITHUB_ACTIONS", None)
            else:
                os.environ["GITHUB_ACTIONS"] = old_value

    def test_x_watch_handoff_runs_only_x_watch_then_existing_bundle_handoff(self):
        mod = importlib.import_module("scripts.dispatch_x_watch_handoff")

        self.assertEqual("crawlers/x_watch.py", mod.CRAWLER_COMMAND)
        self.assertEqual(ROOT / "data" / "crawled", mod.CRAWLED_DIR)

        calls = []

        def fake_run(command, **kwargs):
            calls.append(command)
            if "x_watch.py" in " ".join(map(str, command)):
                mod.today_jsonl_path().parent.mkdir(parents=True, exist_ok=True)
                mod.today_jsonl_path().write_text(
                    json.dumps({
                        "source": "x_watch",
                        "source_id": "1",
                        "content": "post",
                        "timestamp": "2026-05-05T00:00:00+00:00",
                    }) + "\n",
                    encoding="utf-8",
                )
            return subprocess.CompletedProcess(command, 0, "", "")

        with tempfile.TemporaryDirectory() as td:
            old_dir = mod.CRAWLED_DIR
            mod.CRAWLED_DIR = Path(td)
            try:
                with patch.object(mod.subprocess, "run", side_effect=fake_run):
                    mod.run_handoff(no_wait=True)
            finally:
                mod.CRAWLED_DIR = old_dir

        joined = [" ".join(map(str, call)) for call in calls]
        self.assertTrue(any("crawlers/x_watch.py" in call for call in joined))
        self.assertTrue(any("scripts/dispatch_local_crawl_handoff.py" in call for call in joined))
        self.assertTrue(any("--skip-crawl" in call for call in joined))
        self.assertFalse(any("crawlers/discord.py" in call for call in joined))
        self.assertFalse(any("crawlers/run_public.py" in call for call in joined))

    def test_hourly_gate_skips_when_last_success_is_recent(self):
        gate = importlib.import_module("scripts.x_watch_gate")

        with tempfile.TemporaryDirectory() as td:
            state = Path(td) / "x_watch_state.json"
            log = Path(td) / "x_watch.log"
            lock = Path(td) / "x_watch.lock"
            state.write_text(
                json.dumps({"last_run_at": "2026-05-05T10:10:00+09:00", "status": "success"}),
                encoding="utf-8",
            )
            called = []

            rc = gate.run_command_if_due(
                root=ROOT,
                state_path=state,
                log_path=log,
                lock_path=lock,
                now=datetime(2026, 5, 5, 11, 0, tzinfo=KST),
                min_interval_minutes=60,
                command=["does-not-run"],
                runner=lambda *args, **kwargs: called.append(args) or 0,
            )
            log_text = log.read_text(encoding="utf-8")

        self.assertEqual(0, rc)
        self.assertEqual([], called)
        self.assertIn("skip:", log_text)

    def test_x_watch_task_wrapper_uses_hourly_handoff_gate(self):
        text = (ROOT / "run_x_watch_task.sh").read_text(encoding="utf-8")

        self.assertIn("/home/pineapple/bin", text)
        self.assertIn("/home/pineapple/.local/bin", text)
        self.assertIn("FIRST_LIGHT_PYTHON", text)
        self.assertIn("scripts/x_watch_gate.py", text)
        self.assertIn("scripts/dispatch_x_watch_handoff.py", text)
        self.assertNotIn("scripts/x_watch_ingest.py", text)


if __name__ == "__main__":
    unittest.main()
