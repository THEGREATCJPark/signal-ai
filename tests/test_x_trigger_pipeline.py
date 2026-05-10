import json
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


REQUIRED_HANDLES = {
    "testingcatalog", "btibor91", "arrakis_ai", "chetaslua", "synthwavedd",
    "petergostev", "Swarek_", "KayvonJafar", "vitrupo", "immasiddtweets",
    "rowancheung", "aibreakfast", "AlphaSignalAI", "bindureddy",
    "mervenoyann", "OpenAI", "OpenAIDevs", "ChatGPTapp", "sama", "gdb",
    "polynoamial", "nickaturley", "aidan_mclau", "AnthropicAI", "claudeai",
    "ClaudeDevs", "DarioAmodei", "alexalbert__", "jackclarkSF",
    "_sholtodouglas", "GoogleDeepMind", "GoogleAI", "GeminiApp", "demishassabis",
    "JeffDean", "OfficialLoganK", "xai", "grok", "karpathy",
    "ilyasut", "MiraMurati", "johnschulman2", "_jasonwei", "ylecun", "fchollet",
    "AndrewYNg", "emollick", "simonw", "swyx", "arena", "ArtificialAnlys",
    "METR_Evals", "EpochAIResearch", "SWEbench", "LiveCodeBench", "arcprize",
    "llmstats", "steph_palazzolo", "alexeheath", "haydenfield", "shiringhaffary",
    "KylieRobison", "reckless", "caseynewton", "KevinRoose", "parmy",
    "Aaron_Tilley", "huggingface", "ClementDelangue", "Teknium",
    "NousResearch", "Alibaba_Qwen", "deepseek_ai", "MistralAI", "AIatMeta",
    "OpenRouterAI", "ollama", "vllm_project", "modal_labs", "replicate",
    "cursor_ai", "anysphere", "Replit", "amasad", "windsurf_ai", "lovable_dev",
    "v0", "vercel", "latentspacepod",
}

EXCLUDED_NOISY_HANDLES = {
    "AmandaAskell",
    "elonmusk",
    "therundownai",
    "tomwarren",
    "ZeffMax",
}


NITTER_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title><![CDATA[OpenAI: Introducing a new API]]></title>
    <link>https://nitter.net/OpenAI/status/12345#m</link>
    <guid isPermaLink="false">12345</guid>
    <pubDate>Mon, 04 May 2026 12:00:00 GMT</pubDate>
    <description><![CDATA[<p>Introducing <b>a new API</b></p>]]></description>
  </item>
</channel></rss>"""


class XTriggerScanTest(unittest.TestCase):
    def test_scheduled_scan_uses_all_watch_accounts(self):
        workflow = Path(".github/workflows/x-trigger-scan.yml").read_text(encoding="utf-8")

        self.assertIn('- cron: "0 * * * *"', workflow)
        self.assertIn("SCHEDULED_SCOPE: all", workflow)
        self.assertIn("--scope $SCOPE", workflow)

    def test_trigger_scan_workflow_passes_gemini_keys_for_korean_summary(self):
        workflow = Path(".github/workflows/x-trigger-scan.yml").read_text(encoding="utf-8")

        self.assertIn("GEMINI_API_KEYS: ${{ secrets.GEMINI_API_KEYS_CJ }}", workflow)
        self.assertIn('TRIGGER_SUMMARY_MODEL: "gemini-3.1-flash-lite-preview"', workflow)

    def test_trigger_scan_workflow_defaults_to_nitter_with_rsshub_fallbacks(self):
        workflow = Path(".github/workflows/x-trigger-scan.yml").read_text(encoding="utf-8")

        self.assertIn("X_TRIGGER_FEED_MODE: ${{ vars.X_TRIGGER_FEED_MODE || 'nitter-first' }}", workflow)
        self.assertIn("https://rsshub.pseudoyu.com,https://rsshub.app,https://rsshub.rssforever.com,https://rss.detools.dev", workflow)

    def test_load_google_keys_prefers_cj_gemini_keys(self):
        from scripts import x_trigger_scan

        env = {
            "GOOGLE_API_KEY": "bad-google",
            "GEMINI_API_KEYS": "other-one",
            "GEMINI_API_KEYS_CJ": "cj-one,cj-two",
        }

        with patch.dict("os.environ", env, clear=True):
            self.assertEqual(["cj-one", "cj-two"], x_trigger_scan._load_google_keys())

    def test_gemini_flash_lite_config_uses_structured_outputs_and_light_thinking(self):
        from scripts import x_trigger_scan

        captured = {}

        class Response:
            ok = True

            def json(self):
                return {"candidates": [{"content": {"parts": [{"text": '{"title":"요약","body":"본문입니다.","confidence":"official"}'}]}}]}

        def fake_post(url, **kwargs):
            captured["url"] = url
            captured["json"] = kwargs["json"]
            return Response()

        with patch.dict("os.environ", {"GEMINI_API_KEYS_CJ": "cj-key", "TRIGGER_SUMMARY_MODEL": "gemini-3.1-flash-lite-preview"}), \
             patch.object(x_trigger_scan.requests, "post", side_effect=fake_post):
            text = x_trigger_scan.call_google_model("원문을 요약", json_mode=True)

        self.assertIn("gemini-3.1-flash-lite-preview", captured["url"])
        config = captured["json"]["generationConfig"]
        self.assertEqual("application/json", config["responseMimeType"])
        self.assertEqual("object", config["responseSchema"]["type"])
        self.assertEqual("low", config["thinkingConfig"]["thinkingLevel"])
        self.assertIn("요약", text)

    def test_account_config_covers_requested_watchlist_without_duplicates(self):
        from scripts.x_trigger_scan import account_key, load_accounts

        accounts = load_accounts()
        configured = [account_key(account["username"]) for account in accounts]

        self.assertEqual(len(configured), len(set(configured)))
        self.assertTrue({account_key(handle) for handle in REQUIRED_HANDLES}.issubset(set(configured)))
        self.assertFalse({account_key(handle) for handle in EXCLUDED_NOISY_HANDLES} & set(configured))

    def test_load_accounts_deduplicates_handles_case_insensitively(self):
        from scripts.x_trigger_scan import load_accounts

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "accounts.json"
            path.write_text(
                json.dumps([
                    {"username": "OpenAI", "category": "official", "group": "OpenAI", "tier": "auto"},
                    {"username": "@openai", "category": "duplicate", "group": "Other", "tier": "fast"},
                ]),
                encoding="utf-8",
            )
            accounts = load_accounts(path)

        self.assertEqual(1, len(accounts))
        self.assertEqual("OpenAI", accounts[0]["username"])
        self.assertEqual("official", accounts[0]["category"])

    def test_filter_accounts_for_scopes(self):
        from scripts.x_trigger_scan import filter_accounts_for_scope, normalize_account

        accounts = [
            normalize_account({"username": "OpenAI", "tier": "auto"}),
            normalize_account({"username": "testingcatalog", "tier": "core"}),
            normalize_account({"username": "steph_palazzolo", "tier": "scoop"}),
            normalize_account({"username": "cursor_ai", "tier": "coding"}),
            normalize_account({"username": "ilyasut", "tier": "research"}),
        ]

        self.assertEqual(["OpenAI"], [a["username"] for a in filter_accounts_for_scope(accounts, "auto")])
        self.assertEqual(["OpenAI", "testingcatalog"], [a["username"] for a in filter_accounts_for_scope(accounts, "core")])
        self.assertEqual(
            ["OpenAI", "testingcatalog", "steph_palazzolo", "cursor_ai", "ilyasut"],
            [a["username"] for a in filter_accounts_for_scope(accounts, "all")],
        )

    def test_default_feed_client_uses_nitter_before_rsshub(self):
        from scripts.x_trigger_scan import FreeXFeedClient, normalize_account

        class Response:
            ok = True
            status_code = 200
            text = NITTER_RSS

        class Session:
            def __init__(self):
                self.urls = []

            def get(self, url, **kwargs):
                self.urls.append(url)
                return Response()

        session = Session()
        client = FreeXFeedClient(
            base_urls=["https://rsshub.example"],
            nitter_instances=["https://nitter.net"],
            session=session,
        )

        tweets = client.fetch_account_tweets(normalize_account({"username": "OpenAI"}), max_results=1)

        self.assertEqual("12345", tweets[0]["id"])
        self.assertEqual(["https://nitter.net/OpenAI/rss"], session.urls)
        self.assertEqual("nitter", tweets[0]["free_source"])

    def test_fetch_account_tweets_can_use_rsshub_first_when_requested(self):
        from scripts.x_trigger_scan import FreeXFeedClient, normalize_account

        class Response:
            def __init__(self, ok, text="", status_code=200):
                self.ok = ok
                self.text = text
                self.status_code = status_code

        class Session:
            def __init__(self):
                self.urls = []

            def get(self, url, **kwargs):
                self.urls.append(url)
                if "rsshub.example" in url:
                    return Response(False, status_code=503)
                return Response(True, NITTER_RSS)

        session = Session()
        client = FreeXFeedClient(
            base_urls=["https://rsshub.example"],
            nitter_instances=["https://nitter.net"],
            session=session,
            feed_mode="rsshub-first",
        )

        tweets = client.fetch_account_tweets(normalize_account({"username": "OpenAI"}), max_results=1)

        self.assertEqual("12345", tweets[0]["id"])
        self.assertTrue(session.urls[0].startswith("https://rsshub.example/twitter/user/OpenAI/"))
        self.assertEqual("nitter", tweets[0]["free_source"])
        self.assertEqual("https://nitter.net/OpenAI/rss", session.urls[-1])

    def test_fetch_account_tweets_falls_back_to_rsshub_when_nitter_fails(self):
        from scripts.x_trigger_scan import FreeXFeedClient, normalize_account

        class Response:
            def __init__(self, ok, text="", status_code=200):
                self.ok = ok
                self.text = text
                self.status_code = status_code

        class Session:
            def __init__(self):
                self.urls = []

            def get(self, url, **kwargs):
                self.urls.append(url)
                if "nitter.net" in url:
                    return Response(False, status_code=429)
                return Response(True, NITTER_RSS)

        session = Session()
        client = FreeXFeedClient(
            base_urls=["https://rsshub.example"],
            nitter_instances=["https://nitter.net"],
            session=session,
            feed_mode="nitter-first",
        )

        tweets = client.fetch_account_tweets(normalize_account({"username": "OpenAI"}), max_results=1)

        self.assertEqual("12345", tweets[0]["id"])
        self.assertEqual("rsshub", tweets[0]["free_source"])
        self.assertEqual("https://nitter.net/OpenAI/rss", session.urls[0])
        self.assertTrue(session.urls[-1].startswith("https://rsshub.example/twitter/user/OpenAI/"))

    def test_parse_nitter_rss_item_extracts_status_fields(self):
        from scripts.x_trigger_scan import parse_feed_tweets

        tweets = parse_feed_tweets(NITTER_RSS, "OpenAI", source="nitter")

        self.assertEqual(tweets[0]["id"], "12345")
        self.assertEqual(tweets[0]["guid"], "12345")
        self.assertEqual(tweets[0]["link"], "https://nitter.net/OpenAI/status/12345#m")
        self.assertEqual(tweets[0]["created_at"], "Mon, 04 May 2026 12:00:00 GMT")
        self.assertEqual(tweets[0]["text"], "Introducing a new API")
        self.assertEqual(tweets[0]["url"], "https://x.com/OpenAI/status/12345")
        self.assertEqual(tweets[0]["tweet_kind"], "post")

    def test_parse_nitter_title_classifies_retweets_and_replies(self):
        from scripts.x_trigger_scan import parse_feed_tweets

        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel>
          <item>
            <title>RT by @OpenAI: Shared launch</title>
            <link>https://nitter.net/OpenAI/status/111#m</link>
            <guid isPermaLink="false">111</guid>
            <pubDate>Mon, 04 May 2026 12:00:00 GMT</pubDate>
            <description><![CDATA[Shared launch]]></description>
          </item>
          <item>
            <title>R to @OpenAI: Reply update</title>
            <link>https://nitter.net/OpenAI/status/112#m</link>
            <guid isPermaLink="false">112</guid>
            <pubDate>Mon, 04 May 2026 12:01:00 GMT</pubDate>
            <description><![CDATA[Reply update]]></description>
          </item>
        </channel></rss>"""

        tweets = parse_feed_tweets(xml, "OpenAI", source="nitter")

        self.assertEqual(["retweet", "reply"], [tweet["tweet_kind"] for tweet in tweets])

    def test_detect_new_tweets_excludes_reply_and_retweet_by_default(self):
        from scripts.x_trigger_scan import detect_new_tweets

        tweets_by_account = {
            "OpenAI": [
                {"id": "101", "text": "post", "tweet_kind": "post"},
                {"id": "102", "text": "reply", "tweet_kind": "reply"},
                {"id": "103", "text": "retweet", "tweet_kind": "retweet"},
            ]
        }

        candidates, state = detect_new_tweets(tweets_by_account, {"last_seen_ids": {"openai": "100"}})

        self.assertEqual(["101"], [candidate["tweet"]["id"] for candidate in candidates])
        self.assertEqual("103", state["last_seen_ids"]["openai"])

    def test_detect_new_tweets_returns_only_unseen_items_in_chronological_order(self):
        from scripts.x_trigger_scan import detect_new_tweets

        tweets_by_account = {
            "sama": [
                {"id": "105", "text": "third", "tweet_kind": "post"},
                {"id": "103", "text": "second", "tweet_kind": "post"},
                {"id": "101", "text": "old", "tweet_kind": "post"},
            ]
        }

        candidates, state = detect_new_tweets(tweets_by_account, {"last_seen_ids": {"sama": "101"}})

        self.assertEqual(["103", "105"], [c["tweet"]["id"] for c in candidates])
        self.assertEqual("105", state["last_seen_ids"]["sama"])

    def test_first_run_baseline_records_latest_without_candidates(self):
        from scripts.x_trigger_scan import account_key, detect_new_tweets

        tweets_by_account = {
            "OpenAI": [{"id": "100", "text": "New model is live", "tweet_kind": "post"}]
        }

        candidates, state = detect_new_tweets(tweets_by_account, {}, bootstrap=False)

        self.assertEqual([], candidates)
        self.assertEqual("100", state["last_seen_ids"][account_key("OpenAI")])

    def test_backfill_creates_candidates_without_existing_baseline(self):
        from scripts.x_trigger_scan import account_key, detect_new_tweets

        tweets_by_account = {
            "OpenAI": [{"id": "100", "text": "New model is live", "tweet_kind": "post"}]
        }

        candidates, state = detect_new_tweets(tweets_by_account, {}, bootstrap=True)

        self.assertEqual(["100"], [candidate["tweet"]["id"] for candidate in candidates])
        self.assertEqual("100", state["last_seen_ids"][account_key("OpenAI")])

    def test_force_latest_creates_candidate_even_when_cursor_is_current(self):
        from scripts.x_trigger_scan import account_key, detect_new_tweets

        tweets_by_account = {
            "OpenAI": [{"id": "100", "text": "New model is live", "tweet_kind": "post"}]
        }

        candidates, state = detect_new_tweets(
            tweets_by_account,
            {"last_seen_ids": {account_key("OpenAI"): "100"}},
            force_latest=True,
        )

        self.assertEqual(["100"], [candidate["tweet"]["id"] for candidate in candidates])
        self.assertEqual("100", state["last_seen_ids"][account_key("OpenAI")])

    def test_workflow_can_force_latest_for_one_time_review_test(self):
        workflow = Path(".github/workflows/x-trigger-scan.yml").read_text(encoding="utf-8")

        self.assertIn("force_latest:", workflow)
        self.assertIn("--force-latest", workflow)

    def test_official_model_rollout_candidate_should_auto_publish(self):
        from scripts.x_trigger_scan import should_auto_publish_candidate

        candidate = {
            "account": {"username": "OpenAI", "category": "official"},
            "tweet": {"text": "GPT-5.5 Instant is starting to roll out in ChatGPT."},
            "summary": {"title": "GPT-5.5 Instant 출시", "body": "ChatGPT에 새 모델이 순차 배포됩니다."},
        }

        self.assertTrue(should_auto_publish_candidate(candidate))

    def test_non_official_candidate_should_not_auto_publish(self):
        from scripts.x_trigger_scan import should_auto_publish_candidate

        candidate = {
            "account": {"username": "testingcatalog", "category": "fast_signal"},
            "tweet": {"text": "GPT-5.5 Instant might be rolling out."},
            "summary": {"title": "GPT-5.5 루머", "body": "비공식 계정의 관측입니다."},
        }

        self.assertFalse(should_auto_publish_candidate(candidate))

    def test_candidate_with_explicit_high_score_should_auto_publish(self):
        from scripts.x_trigger_scan import score_trigger_candidate, should_auto_publish_candidate

        candidate = {
            "account": {"username": "testingcatalog", "category": "fast_signal"},
            "tweet": {"text": "A strong AI launch signal."},
            "summary": {"title": "High signal", "body": "High signal", "score": 85},
        }

        self.assertGreaterEqual(score_trigger_candidate(candidate), 80)
        self.assertTrue(should_auto_publish_candidate(candidate))

    def test_trigger_scan_workflow_auto_publishes_to_telegram_by_default(self):
        workflow = Path(".github/workflows/x-trigger-scan.yml").read_text(encoding="utf-8")

        self.assertIn("TELEGRAM_CHANNEL_ID: ${{ secrets.TELEGRAM_CHANNEL_ID }}", workflow)
        self.assertIn("TRIGGER_AUTO_PUBLISH_PLATFORM: ${{ vars.TRIGGER_AUTO_PUBLISH_PLATFORM || 'telegram' }}", workflow)
        self.assertIn("X_API_KEY: ${{ secrets.X_API_KEY }}", workflow)

    def test_build_candidate_issue_body_round_trips_payload_and_instructions(self):
        from scripts.x_trigger_scan import build_issue_body, extract_candidate_from_issue_body

        candidate = {
            "id": "x-123",
            "account": {"username": "OpenAI", "category": "official", "group": "OpenAI", "tier": "auto"},
            "tweet": {
                "id": "123",
                "text": "Introducing a new API.",
                "url": "https://x.com/OpenAI/status/123",
                "created_at": "2026-05-04T00:00:00Z",
            },
            "summary": {"title": "OpenAI API 공개", "body": "OpenAI가 새 API를 공개했습니다."},
            "detected_at": "2026-05-04T00:01:00Z",
        }

        body = build_issue_body(candidate)
        parsed = extract_candidate_from_issue_body(body)

        self.assertEqual("x-123", parsed["id"])
        self.assertIn("## X 트리거 검수", body)
        self.assertIn("계정", body)
        self.assertIn("분류", body)
        self.assertIn("원문 링크", body)
        self.assertIn("게시 시각", body)
        self.assertIn("감지 시각", body)
        self.assertIn("한국어 요약", body)
        self.assertIn("추천 발행문", body)
        self.assertIn("승인 방법", body)
        self.assertIn("거절 방법", body)
        self.assertIn("/approve-trigger", body)
        self.assertIn("/reject-trigger", body)

    def test_issue_recommendation_matches_validated_x_post_text(self):
        from bot import x_poster
        from scripts.x_trigger_scan import build_issue_body

        candidate = {
            "id": "x-123",
            "account": {"username": "OpenAI", "category": "official", "group": "OpenAI", "tier": "auto"},
            "tweet": {
                "id": "123",
                "text": "Introducing a new realtime API for developers.",
                "url": "https://x.com/OpenAI/status/123",
                "created_at": "2026-05-04T00:00:00Z",
            },
            "summary": {
                "title": "OpenAI ships a realtime API",
                "body": (
                    "This is the short comment that should be published. "
                    "This second sentence is deliberately long and should be dropped "
                    "instead of being clipped with an ellipsis before the issue is opened."
                ),
            },
            "detected_at": "2026-05-04T00:01:00Z",
        }

        body = build_issue_body(candidate)
        post_text = candidate["x_post_text"]

        self.assertIn(post_text, body)
        self.assertIn("X 길이 검사", body)
        self.assertEqual(candidate["x_post_weight"], x_poster._tweet_weight(post_text))
        self.assertLessEqual(candidate["x_post_weight"], x_poster.MAX_TWEET_WEIGHT)
        self.assertNotIn("...", post_text)

    def test_issue_recommendation_refits_existing_overlong_x_post_text(self):
        from bot import x_poster
        from scripts.x_trigger_scan import build_issue_body

        candidate = {
            "id": "x-123",
            "account": {"username": "OpenAI", "category": "official", "group": "OpenAI", "tier": "auto"},
            "tweet": {
                "id": "123",
                "text": "Introducing a new realtime API for developers.",
                "url": "https://x.com/OpenAI/status/" + ("1234567890" * 6),
                "created_at": "2026-05-04T00:00:00Z",
            },
            "summary": {
                "title": "OpenAI ships a realtime API",
                "body": "Short comment.",
            },
            "x_post_text": "OpenAI ships a realtime API\n" + ("Long stale reviewer note. " * 30),
            "detected_at": "2026-05-04T00:01:00Z",
        }

        build_issue_body(candidate)
        post_text = candidate["x_post_text"]

        self.assertLessEqual(candidate["x_post_weight"], x_poster.MAX_TWEET_WEIGHT)
        self.assertLessEqual(candidate["x_post_chars"], x_poster.MAX_TWEET_CHARS)
        self.assertIn(candidate["tweet"]["url"], post_text)
        self.assertNotIn("...", post_text)

    def test_summarize_tweet_uses_ai_json_when_available(self):
        from scripts.x_trigger_scan import summarize_tweet

        def fake_ai(prompt, json_mode=False):
            self.assertTrue(json_mode)
            self.assertIn("@OpenAI", prompt)
            return json.dumps({"title": "OpenAI 발표", "body": "OpenAI가 중요한 발표를 했습니다."})

        summary = summarize_tweet(
            {"text": "We shipped something important."},
            {"username": "OpenAI", "category": "official"},
            ai_call=fake_ai,
        )

        self.assertEqual("OpenAI 발표", summary["title"])
        self.assertEqual("OpenAI가 중요한 발표를 했습니다.", summary["body"])

    def test_fallback_summary_is_korean_review_safe(self):
        from scripts.x_trigger_scan import fallback_summary

        summary = fallback_summary(
            {"text": "We shipped a new realtime API for developers."},
            {"username": "OpenAIDevs"},
        )

        self.assertIn("@OpenAIDevs", summary["title"])
        self.assertIn("AI 요약을 생성하지 못했습니다", summary["body"])
        self.assertIn("원문", summary["body"])

    def test_summary_prompt_is_korean_and_readable(self):
        from scripts.x_trigger_scan import build_summary_prompt

        prompt = build_summary_prompt(
            {"text": "We shipped something important.", "created_at": "2026-05-04T00:00:00Z"},
            {"username": "OpenAI", "category": "official", "group": "OpenAI"},
        )

        self.assertIn("한국어", prompt)
        self.assertIn("원문", prompt)
        self.assertIn("@OpenAI", prompt)
        self.assertNotIn("??", prompt)

    def test_github_issue_title_and_review_notification_are_korean(self):
        from scripts import x_trigger_scan

        candidate = {
            "id": "x-123",
            "account": {"username": "OpenAI", "category": "official", "group": "OpenAI", "tier": "auto"},
            "tweet": {"id": "123", "text": "Introducing a new API.", "url": "https://x.com/OpenAI/status/123"},
            "summary": {"title": "OpenAI 새 API 공개", "body": "OpenAI가 새 API를 공개했습니다."},
        }

        posted = {}

        class Response:
            ok = True

            def json(self):
                return {"html_url": "https://github.com/example/issues/1"}

        def fake_post(url, **kwargs):
            if url.endswith("/issues"):
                posted.update(kwargs["json"])
            return Response()

        with patch.object(x_trigger_scan, "ensure_github_labels"), \
             patch.object(x_trigger_scan.requests, "post", side_effect=fake_post):
            url = x_trigger_scan.create_github_issue(candidate, token="token", repo="owner/repo")

        self.assertEqual("https://github.com/example/issues/1", url)
        self.assertTrue(posted["title"].startswith("[X 트리거 검수] @OpenAI:"))
        self.assertIn("## X 트리거 검수", posted["body"])

    def test_dry_run_does_not_save_state_when_no_candidates(self):
        from scripts import x_trigger_scan

        class FakeClient:
            def __init__(self, **kwargs):
                pass

            def fetch_recent_by_accounts(self, accounts, *, max_results, state):
                return {"OpenAI": []}, state

        args = types.SimpleNamespace(
            accounts=None,
            scope="auto",
            state=None,
            max_results=1,
            backfill=False,
            dry_run=True,
            feed_mode="nitter",
        )

        with patch.object(x_trigger_scan, "FreeXFeedClient", FakeClient), \
             patch.object(x_trigger_scan, "load_trigger_state", return_value={}), \
             patch.object(x_trigger_scan, "save_trigger_state") as save:
            rc = x_trigger_scan.run_scan(args)

        self.assertEqual(0, rc)
        save.assert_not_called()

    def test_run_scan_auto_publishes_official_high_signal_candidate_after_issue(self):
        from scripts import x_trigger_scan

        calls = []

        class FakeClient:
            def __init__(self, **kwargs):
                pass

            def fetch_recent_by_accounts(self, accounts, *, max_results, state):
                return {
                    "OpenAI": [{
                        "id": "101",
                        "text": "GPT-5.5 Instant is starting to roll out in ChatGPT.",
                        "tweet_kind": "post",
                        "url": "https://x.com/OpenAI/status/101",
                    }]
                }, state

        args = types.SimpleNamespace(
            accounts=None,
            scope="auto",
            state=None,
            max_results=1,
            backfill=False,
            force_latest=False,
            dry_run=False,
            feed_mode="nitter",
        )

        def fake_issue(candidate, **kwargs):
            calls.append(("issue", candidate["id"]))
            return "https://github.com/owner/repo/issues/9"

        def fake_publish(candidate, **kwargs):
            calls.append(("publish", candidate["id"], kwargs.get("platform")))
            return ["telegram", "x"]

        def fake_update(issue_url, candidate, published):
            calls.append(("update", issue_url, tuple(published)))

        with patch.object(x_trigger_scan, "FreeXFeedClient", FakeClient), \
             patch.object(x_trigger_scan, "load_trigger_state", return_value={"last_seen_ids": {"openai": "100"}}), \
             patch.object(x_trigger_scan, "save_trigger_state") as save, \
             patch.object(x_trigger_scan, "summarize_tweet", return_value={
                 "title": "GPT-5.5 Instant 배포",
                 "body": "OpenAI가 ChatGPT에 GPT-5.5 Instant를 순차 배포합니다.",
                 "confidence": "official",
             }), \
             patch.object(x_trigger_scan, "create_github_issue", side_effect=fake_issue), \
             patch.object(x_trigger_scan, "maybe_notify_telegram"), \
             patch.object(x_trigger_scan, "publish_trigger_candidate", side_effect=fake_publish), \
             patch.object(x_trigger_scan, "record_auto_publish_on_issue", side_effect=fake_update):
            rc = x_trigger_scan.run_scan(args)

        self.assertEqual(0, rc)
        self.assertEqual([
            ("issue", "x-101"),
            ("publish", "x-101", "telegram"),
            ("update", "https://github.com/owner/repo/issues/9", ("telegram", "x")),
        ], calls)
        save.assert_called_once()


class XTriggerReviewTest(unittest.TestCase):
    def test_diagnostic_workflow_can_post_plain_x_text(self):
        workflow = Path(".github/workflows/x_post_diagnostic.yml").read_text(encoding="utf-8")

        self.assertIn("X_API_KEY: ${{ secrets.X_API_KEY }}", workflow)
        self.assertIn("X_API_SECRET: ${{ secrets.X_API_SECRET }}", workflow)
        self.assertIn("X_ACCESS_TOKEN: ${{ secrets.X_ACCESS_TOKEN }}", workflow)
        self.assertIn("X_ACCESS_TOKEN_SECRET: ${{ secrets.X_ACCESS_TOKEN_SECRET }}", workflow)
        self.assertIn("X_CLIENT_ID: ${{ secrets.X_CLIENT_ID }}", workflow)
        self.assertIn("X_CLIENT_SECRET: ${{ secrets.X_CLIENT_SECRET }}", workflow)
        self.assertIn("X_REFRESH_TOKEN: ${{ secrets.X_REFRESH_TOKEN }}", workflow)
        self.assertIn("python scripts/x_post_text.py", workflow)

    def test_review_workflow_passes_oauth1_credentials_for_x_publish(self):
        workflow = Path(".github/workflows/x-trigger-review.yml").read_text(encoding="utf-8")

        self.assertIn("X_API_KEY: ${{ secrets.X_API_KEY }}", workflow)
        self.assertIn("X_API_SECRET: ${{ secrets.X_API_SECRET }}", workflow)
        self.assertIn("X_ACCESS_TOKEN: ${{ secrets.X_ACCESS_TOKEN }}", workflow)
        self.assertIn("X_ACCESS_TOKEN_SECRET: ${{ secrets.X_ACCESS_TOKEN_SECRET }}", workflow)
        self.assertIn("X_TWEET_URL: https://api.x.com/2/tweets", workflow)
        self.assertIn("X_CLIENT_ID: ${{ secrets.X_CLIENT_ID }}", workflow)
        self.assertIn("X_CLIENT_SECRET: ${{ secrets.X_CLIENT_SECRET }}", workflow)
        self.assertIn("X_REFRESH_TOKEN: ${{ secrets.X_REFRESH_TOKEN }}", workflow)

    def test_review_command_parses_english_and_korean_commands(self):
        from scripts.x_trigger_review import parse_review_command, parse_review_platform, reviewer_is_allowed

        for command in ("yes", "y", "예", "ㅇ", "approve", "승인", "/approve", "/approve-trigger"):
            self.assertEqual("approve", parse_review_command(command))
        for command in ("no", "n", "아니오", "아니요", "ㄴ", "reject", "거절", "/reject", "/reject-trigger"):
            self.assertEqual("reject", parse_review_command(command))
        self.assertEqual("x", parse_review_platform("/approve-trigger x"))
        self.assertEqual("telegram", parse_review_platform("승인 telegram"))
        self.assertIsNone(parse_review_platform("yes"))
        self.assertTrue(reviewer_is_allowed("hb", "COLLABORATOR", []))
        self.assertFalse(reviewer_is_allowed("drive-by", "NONE", []))

    def test_build_trigger_article_uses_summary_and_source_url(self):
        from scripts.x_trigger_review import build_trigger_article

        candidate = {
            "id": "x-123",
            "account": {"username": "sama", "category": "person"},
            "tweet": {"url": "https://x.com/sama/status/123", "created_at": "2026-05-04T00:00:00Z"},
            "summary": {"title": "핵심 발언", "body": "중요한 발언을 요약했습니다."},
        }

        article = build_trigger_article(candidate, now=datetime(2026, 5, 4, tzinfo=timezone.utc))

        self.assertEqual("trigger-x-123", article["id"])
        self.assertEqual("핵심 발언", article["title"])
        self.assertEqual("중요한 발언을 요약했습니다.", article["summary"])
        self.assertEqual("https://x.com/sama/status/123", article["url"])
        self.assertEqual("x_trigger", article["source"])

    def test_publish_trigger_candidate_calls_telegram_and_x_once(self):
        from scripts.x_trigger_review import publish_trigger_candidate

        calls = []

        class FakeState:
            def is_published(self, article_id, platform):
                return False

            def mark_published(self, article_id, platform):
                calls.append(("mark", article_id, platform))

            def save(self):
                calls.append(("save",))

        candidate = {
            "id": "x-123",
            "account": {"username": "OpenAI", "category": "official"},
            "tweet": {"id": "123", "url": "https://x.com/OpenAI/status/123"},
            "summary": {"title": "Launch", "body": "Launch summary"},
        }

        fake_telegram = types.ModuleType("bot.telegram_bot")
        fake_telegram.send_article = lambda article: calls.append(("telegram", article["id"]))
        fake_x = types.ModuleType("bot.x_poster")
        fake_x.post_article = lambda article: calls.append(("x", article["id"]))
        fake_state = types.ModuleType("publisher.state")
        fake_state.article_key = lambda article: article["id"]
        fake_state.get_state = lambda: FakeState()

        with patch.dict(sys.modules, {
            "bot.telegram_bot": fake_telegram,
            "bot.x_poster": fake_x,
            "publisher.state": fake_state,
        }), patch("db.articles.upsert_generated_articles", side_effect=lambda articles: calls.append(("upsert_articles", articles[0]["id"])) or len(articles)):
            published = publish_trigger_candidate(candidate, platform="both")

        self.assertEqual(["telegram", "x"], published)
        self.assertLess(calls.index(("upsert_articles", "trigger-x-123")), calls.index(("telegram", "trigger-x-123")))
        self.assertIn(("telegram", "trigger-x-123"), calls)
        self.assertIn(("x", "trigger-x-123"), calls)
        self.assertIn(("mark", "trigger-x-123", "telegram"), calls)
        self.assertIn(("mark", "trigger-x-123", "x"), calls)

    def test_publish_trigger_candidate_keeps_telegram_when_x_fails(self):
        from scripts.x_trigger_review import publish_trigger_candidate

        calls = []

        class FakeState:
            def is_published(self, article_id, platform):
                return False

            def mark_published(self, article_id, platform):
                calls.append(("mark", article_id, platform))

            def save(self):
                calls.append(("save",))

        candidate = {
            "id": "x-789",
            "account": {"username": "OpenAI", "category": "official"},
            "tweet": {"id": "789", "url": "https://x.com/OpenAI/status/789"},
            "summary": {"title": "Launch", "body": "Launch summary"},
        }

        fake_telegram = types.ModuleType("bot.telegram_bot")
        fake_telegram.send_article = lambda article: calls.append(("telegram", article["id"]))
        fake_x = types.ModuleType("bot.x_poster")
        fake_x.post_article = lambda article: (_ for _ in ()).throw(RuntimeError("x failed"))
        fake_state = types.ModuleType("publisher.state")
        fake_state.article_key = lambda article: article["id"]
        fake_state.get_state = lambda: FakeState()

        with patch.dict(sys.modules, {
            "bot.telegram_bot": fake_telegram,
            "bot.x_poster": fake_x,
            "publisher.state": fake_state,
        }), patch("db.articles.upsert_generated_articles", return_value=1):
            published = publish_trigger_candidate(candidate, platform="both")

        self.assertEqual(["telegram"], published)
        self.assertIn(("telegram", "trigger-x-789"), calls)
        self.assertIn(("mark", "trigger-x-789", "telegram"), calls)
        self.assertNotIn(("mark", "trigger-x-789", "x"), calls)
        self.assertIn(("save",), calls)

    def test_publish_trigger_candidate_does_not_pre_escape_telegram_article(self):
        from scripts.x_trigger_review import publish_trigger_candidate

        captured = {}

        class FakeState:
            def is_published(self, article_id, platform):
                return False

            def mark_published(self, article_id, platform):
                return None

            def save(self):
                return None

        candidate = {
            "id": "x-456",
            "account": {"username": "OpenAI", "category": "official"},
            "tweet": {"id": "456", "url": "https://x.com/OpenAI/status/456"},
            "summary": {
                "title": "A&B <launch>",
                "body": "Use A&B with <tags> safely.",
            },
        }

        fake_telegram = types.ModuleType("bot.telegram_bot")
        fake_telegram.send_article = lambda article: captured.update(article)
        fake_x = types.ModuleType("bot.x_poster")
        fake_x.post_article = lambda article: None
        fake_state = types.ModuleType("publisher.state")
        fake_state.article_key = lambda article: article["id"]
        fake_state.get_state = lambda: FakeState()

        with patch.dict(sys.modules, {
            "bot.telegram_bot": fake_telegram,
            "bot.x_poster": fake_x,
            "publisher.state": fake_state,
        }), patch("db.articles.upsert_generated_articles", return_value=1):
            publish_trigger_candidate(candidate, platform="telegram")

        self.assertEqual("A&B <launch>", captured["title"])
        self.assertEqual("Use A&B with <tags> safely.", captured["summary"])

    def test_handle_event_reject_does_not_publish_and_closes_issue(self):
        from scripts import x_trigger_review

        calls = []

        class FakeClient:
            def add_comment(self, issue_number, body):
                calls.append(("comment", issue_number, body))

            def add_labels(self, issue_number, labels):
                calls.append(("labels", issue_number, labels))

            def close_issue(self, issue_number, reason="completed"):
                calls.append(("close", issue_number, reason))

        event = {
            "action": "created",
            "issue": {"number": 7, "labels": [{"name": "x-trigger"}], "body": ""},
            "comment": {
                "body": "거절",
                "user": {"login": "reviewer"},
                "author_association": "MEMBER",
            },
        }

        with patch.object(x_trigger_review, "GitHubIssueClient", return_value=FakeClient()), \
             patch.object(x_trigger_review, "publish_trigger_candidate") as publish:
            rc = x_trigger_review.handle_event(event)

        self.assertEqual(0, rc)
        publish.assert_not_called()
        self.assertIn(("labels", 7, ["trigger-rejected"]), calls)
        self.assertIn(("close", 7, "not_planned"), calls)

    def test_handle_event_ignores_already_processed_issue(self):
        from scripts import x_trigger_review

        event = {
            "action": "created",
            "issue": {"number": 7, "labels": [{"name": "x-trigger"}, {"name": "trigger-approved"}], "body": ""},
            "comment": {
                "body": "approve",
                "user": {"login": "reviewer"},
                "author_association": "MEMBER",
            },
        }

        with patch.object(x_trigger_review, "GitHubIssueClient") as client, \
             patch.object(x_trigger_review, "publish_trigger_candidate") as publish:
            rc = x_trigger_review.handle_event(event)

        self.assertEqual(0, rc)
        client.assert_not_called()
        publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
