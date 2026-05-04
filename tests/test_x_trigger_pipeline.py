import json
import unittest
from datetime import datetime, timezone


class XTriggerScanTest(unittest.TestCase):
    def test_filter_accounts_for_auto_and_manual_scopes(self):
        from scripts.x_trigger_scan import filter_accounts_for_scope, normalize_account

        accounts = [
            normalize_account({"username": "OpenAI", "tier": "auto"}),
            normalize_account({"username": "testingcatalog", "tier": "core"}),
            normalize_account({"username": "steph_palazzolo", "tier": "scoop"}),
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
            ["OpenAI", "testingcatalog", "steph_palazzolo"],
        )

    def test_resolve_user_ids_reuses_cached_ids_and_marks_new_users(self):
        from scripts.x_trigger_scan import normalize_account, resolve_account_users

        accounts = [
            normalize_account({"username": "OpenAI", "tier": "auto"}),
            normalize_account({"username": "sama", "tier": "core"}),
        ]
        state = {"user_ids": {"openai": "42"}}
        lookups = []

        def fake_lookup(usernames):
            lookups.append(usernames)
            return {"sama": {"id": "99", "username": "sama"}}

        users, next_state = resolve_account_users(accounts, state, fake_lookup)

        self.assertEqual(users["openai"]["id"], "42")
        self.assertEqual(users["sama"]["id"], "99")
        self.assertEqual(lookups, [["sama"]])
        self.assertEqual(next_state["user_ids"]["sama"], "99")

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
