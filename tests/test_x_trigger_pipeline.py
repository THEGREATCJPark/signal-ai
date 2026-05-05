import json
import unittest
from datetime import datetime, timezone


class XTriggerScanTest(unittest.TestCase):
    def test_account_config_covers_requested_watchlist_without_duplicates(self):
        from scripts.x_trigger_scan import account_key, load_accounts

        requested = {
            "testingcatalog", "btibor91", "arrakis_ai", "chetaslua", "synthwavedd",
            "petergostev", "Swarek_", "KayvonJafar", "vitrupo", "immasiddtweets",
            "rowancheung", "therundownai", "aibreakfast", "AlphaSignalAI", "bindureddy",
            "mervenoyann", "OpenAI", "OpenAIDevs", "ChatGPTapp", "sama", "gdb",
            "polynoamial", "nickaturley", "aidan_mclau", "AnthropicAI", "claudeai",
            "ClaudeDevs", "DarioAmodei", "alexalbert__", "AmandaAskell", "jackclarkSF",
            "_sholtodouglas", "GoogleDeepMind", "GoogleAI", "GeminiApp", "demishassabis",
            "JeffDean", "OfficialLoganK", "xai", "grok", "elonmusk", "karpathy",
            "ilyasut", "MiraMurati", "johnschulman2", "_jasonwei", "ylecun", "fchollet",
            "AndrewYNg", "emollick", "simonw", "swyx", "arena", "ArtificialAnlys",
            "METR_Evals", "EpochAIResearch", "SWEbench", "LiveCodeBench", "arcprize",
            "llmstats", "steph_palazzolo", "alexeheath", "haydenfield", "shiringhaffary",
            "ZeffMax", "KylieRobison", "reckless", "caseynewton", "KevinRoose", "parmy",
            "Aaron_Tilley", "tomwarren", "huggingface", "ClementDelangue", "Teknium",
            "NousResearch", "Alibaba_Qwen", "deepseek_ai", "MistralAI", "AIatMeta",
            "OpenRouterAI", "ollama", "vllm_project", "modal_labs", "replicate",
            "cursor_ai", "anysphere", "Replit", "amasad", "windsurf_ai", "lovable_dev",
            "v0", "vercel", "latentspacepod",
        }

        accounts = load_accounts()
        configured = [account_key(account["username"]) for account in accounts]

        self.assertEqual(len(configured), len(set(configured)))
        self.assertTrue({account_key(handle) for handle in requested}.issubset(set(configured)))

    def test_filter_accounts_for_auto_and_manual_scopes(self):
        from scripts.x_trigger_scan import filter_accounts_for_scope, normalize_account

        accounts = [
            normalize_account({"username": "OpenAI", "tier": "auto"}),
            normalize_account({"username": "testingcatalog", "tier": "core"}),
            normalize_account({"username": "steph_palazzolo", "tier": "scoop"}),
            normalize_account({"username": "cursor_ai", "tier": "coding"}),
            normalize_account({"username": "ilyasut", "tier": "research"}),
        ]

        self.assertEqual(
            [account["username"] for account in filter_accounts_for_scope(accounts, "auto")],
            ["OpenAI"],
        )
        self.assertEqual(
            [account["username"] for account in filter_accounts_for_scope(accounts, "core")],
            ["OpenAI", "testingcatalog"],
        )
        self.assertEqual(
            [account["username"] for account in filter_accounts_for_scope(accounts, "all")],
            ["OpenAI", "testingcatalog", "steph_palazzolo", "cursor_ai", "ilyasut"],
        )
        self.assertEqual(
            [account["username"] for account in filter_accounts_for_scope(accounts, "coding")],
            ["OpenAI", "testingcatalog", "cursor_ai"],
        )
        self.assertEqual(
            [account["username"] for account in filter_accounts_for_scope(accounts, "research")],
            ["OpenAI", "testingcatalog", "ilyasut"],
        )

    def test_build_free_feed_url_uses_rsshub_route_without_x_api(self):
        from scripts.x_trigger_scan import build_free_feed_url

        url = build_free_feed_url("https://rsshub.app", "OpenAI", max_results=1)

        self.assertEqual(
            url,
            "https://rsshub.app/twitter/user/OpenAI/excludeReplies=1&includeRts=0?limit=1",
        )

    def test_parse_rsshub_tweets_extracts_status_id_and_clean_text(self):
        from scripts.x_trigger_scan import parse_feed_tweets

        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel>
          <item>
            <title><![CDATA[OpenAI: Introducing a new API]]></title>
            <link>https://x.com/OpenAI/status/12345</link>
            <guid>https://x.com/OpenAI/status/12345</guid>
            <pubDate>Mon, 04 May 2026 12:00:00 GMT</pubDate>
            <description><![CDATA[<p>Introducing <b>a new API</b></p>]]></description>
          </item>
        </channel></rss>"""

        tweets = parse_feed_tweets(xml, "OpenAI")

        self.assertEqual(tweets[0]["id"], "12345")
        self.assertEqual(tweets[0]["url"], "https://x.com/OpenAI/status/12345")
        self.assertEqual(tweets[0]["text"], "Introducing a new API")

    def test_fetch_account_tweets_falls_back_to_nitter_when_rsshub_fails(self):
        from scripts.x_trigger_scan import FreeXFeedClient, normalize_account

        rss = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel>
          <item>
            <title>OpenAI: Introducing a fallback feed</title>
            <link>https://nitter.net/OpenAI/status/67890#m</link>
            <guid isPermaLink="false">67890</guid>
            <pubDate>Mon, 04 May 2026 12:00:00 GMT</pubDate>
            <description><![CDATA[<p>Introducing a fallback feed</p>]]></description>
          </item>
        </channel></rss>"""

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
                return Response(True, rss)

        session = Session()
        client = FreeXFeedClient(
            base_urls=["https://rsshub.example"],
            nitter_instances=["https://nitter.net"],
            session=session,
        )

        tweets = client.fetch_account_tweets(normalize_account({"username": "OpenAI"}), max_results=1)

        self.assertEqual(tweets[0]["id"], "67890")
        self.assertEqual(tweets[0]["url"], "https://x.com/OpenAI/status/67890")
        self.assertEqual(tweets[0]["free_source"], "nitter")
        self.assertEqual(session.urls[-1], "https://nitter.net/OpenAI/rss")

    def test_bootstrap_accounts_records_latest_without_candidates(self):
        from scripts.x_trigger_scan import account_key, detect_new_tweets

        tweets_by_account = {
            "OpenAI": [
                {"id": "100", "text": "New model is live", "created_at": "2026-05-04T00:00:00Z"}
            ]
        }

        candidates, state = detect_new_tweets(tweets_by_account, {}, bootstrap=False)

        self.assertEqual(candidates, [])
        self.assertEqual(state["last_seen_ids"][account_key("OpenAI")], "100")

    def test_detect_new_tweets_returns_only_unseen_items_in_chronological_order(self):
        from scripts.x_trigger_scan import detect_new_tweets

        tweets_by_account = {
            "sama": [
                {"id": "105", "text": "third", "created_at": "2026-05-04T00:03:00Z"},
                {"id": "103", "text": "second", "created_at": "2026-05-04T00:02:00Z"},
                {"id": "101", "text": "old", "created_at": "2026-05-04T00:01:00Z"},
            ]
        }

        candidates, state = detect_new_tweets(tweets_by_account, {"last_seen_ids": {"sama": "101"}})

        self.assertEqual([c["tweet"]["id"] for c in candidates], ["103", "105"])
        self.assertEqual(state["last_seen_ids"]["sama"], "105")

    def test_build_candidate_issue_body_round_trips_payload(self):
        from scripts.x_trigger_scan import build_issue_body, extract_candidate_from_issue_body

        candidate = {
            "id": "x-123",
            "account": {"username": "OpenAI", "category": "official"},
            "tweet": {
                "id": "123",
                "text": "Introducing a new API.",
                "url": "https://x.com/OpenAI/status/123",
                "created_at": "2026-05-04T00:00:00Z",
            },
            "summary": {"title": "OpenAI, 새 API 공개", "body": "OpenAI가 새 API를 공개했다."},
        }

        body = build_issue_body(candidate)
        parsed = extract_candidate_from_issue_body(body)

        self.assertEqual(parsed["id"], "x-123")
        self.assertEqual(parsed["tweet"]["url"], "https://x.com/OpenAI/status/123")
        self.assertIn("/approve-trigger", body)
        self.assertIn("/reject-trigger", body)

    def test_summarize_tweet_uses_ai_json_when_available(self):
        from scripts.x_trigger_scan import summarize_tweet

        def fake_ai(prompt, json_mode=False):
            self.assertTrue(json_mode)
            self.assertIn("@OpenAI", prompt)
            return json.dumps({"title": "OpenAI 발표", "body": "OpenAI가 중요한 발표를 했다."})

        summary = summarize_tweet(
            {"text": "We shipped something important."},
            {"username": "OpenAI", "category": "official"},
            ai_call=fake_ai,
        )

        self.assertEqual(summary["title"], "OpenAI 발표")
        self.assertEqual(summary["body"], "OpenAI가 중요한 발표를 했다.")


class XTriggerReviewTest(unittest.TestCase):
    def test_review_command_authorizes_collaborators_by_default(self):
        from scripts.x_trigger_review import parse_review_command, reviewer_is_allowed

        self.assertEqual(parse_review_command("/approve-trigger"), "approve")
        self.assertEqual(parse_review_command("LGTM\n/REJECT-TRIGGER"), "reject")
        self.assertTrue(reviewer_is_allowed("hb", "COLLABORATOR", []))
        self.assertFalse(reviewer_is_allowed("drive-by", "NONE", []))

    def test_build_trigger_article_uses_summary_and_source_url(self):
        from scripts.x_trigger_review import build_trigger_article

        candidate = {
            "id": "x-123",
            "account": {"username": "sama", "category": "person"},
            "tweet": {"url": "https://x.com/sama/status/123", "created_at": "2026-05-04T00:00:00Z"},
            "summary": {"title": "핵심 인물 발언", "body": "샘 알트먼이 중요한 힌트를 남겼다."},
        }

        article = build_trigger_article(candidate, now=datetime(2026, 5, 4, tzinfo=timezone.utc))

        self.assertEqual(article["id"], "trigger-x-123")
        self.assertEqual(article["title"], "핵심 인물 발언")
        self.assertEqual(article["summary"], "샘 알트먼이 중요한 힌트를 남겼다.")
        self.assertEqual(article["url"], "https://x.com/sama/status/123")
        self.assertEqual(article["source"], "x_trigger")


if __name__ == "__main__":
    unittest.main()
