import unittest
from datetime import datetime, timezone


NOW = datetime(2026, 5, 16, 12, 0, tzinfo=timezone.utc)


def candidate(
    *,
    username: str,
    category: str,
    text: str,
    title: str,
    body: str,
    confidence: str = "verified",
    group: str = "AI",
    tier: str = "core",
    tweet_id: str = "100",
    created_at: str = "2026-05-16T11:00:00Z",
    url: str | None = None,
    **tweet_extra,
) -> dict:
    return {
        "id": f"x-{tweet_id}",
        "account": {"username": username, "category": category, "group": group, "tier": tier},
        "tweet": {
            "id": tweet_id,
            "text": text,
            "url": url or f"https://x.com/{username}/status/{tweet_id}",
            "created_at": created_at,
            **tweet_extra,
        },
        "summary": {"title": title, "body": body, "confidence": confidence},
        "detected_at": "2026-05-16T11:01:00Z",
    }


class XTriggerScoringTest(unittest.TestCase):
    def test_official_openai_chatgpt_product_launch_auto_both(self):
        from scripts.x_trigger_scoring import score_trigger_candidate_detail

        detail = score_trigger_candidate_detail(candidate(
            username="OpenAI",
            category="official",
            group="OpenAI",
            tier="auto",
            text="Introducing ChatGPT personal finance, rolling out today. https://openai.com/blog/chatgpt-personal-finance",
            title="OpenAI launches ChatGPT personal finance",
            body="OpenAI is rolling out a new ChatGPT personal finance feature with an official blog post.",
            confidence="verified",
            tweet_id="101",
        ), now=NOW)

        self.assertGreaterEqual(detail["score"], 90)
        self.assertEqual("auto_both", detail["decision"])
        self.assertEqual([], detail["hard_blocks"])

    def test_official_v0_browser_use_launch_auto_both(self):
        from scripts.x_trigger_scoring import score_trigger_candidate_detail

        detail = score_trigger_candidate_detail(candidate(
            username="v0",
            category="official",
            group="Vercel/v0",
            tier="auto",
            text="v0 Browser Use is now available for agents and AI builders. Read the release notes.",
            title="v0 launches Browser Use",
            body="v0 released Browser Use, a new feature for agent workflows and AI builders.",
            confidence="verified",
            tweet_id="102",
        ), now=NOW)

        self.assertGreaterEqual(detail["score"], 90)
        self.assertEqual("auto_both", detail["decision"])

    def test_verified_sixty_point_candidate_auto_publishes_to_both(self):
        from scripts.x_trigger_scoring import score_trigger_candidate_detail, should_auto_publish_candidate

        item = candidate(
            username="OpenAI",
            category="official",
            group="OpenAI",
            tier="auto",
            text="OpenAI ships a small but useful ChatGPT API feature for developers.",
            title="OpenAI ships ChatGPT API feature",
            body="OpenAI ships a useful ChatGPT API feature for developers.",
            confidence="verified",
            tweet_id="111",
        )
        item["summary"]["score"] = 60
        detail = score_trigger_candidate_detail(item, now=NOW)

        self.assertEqual(60, detail["score"])
        self.assertEqual("auto_both", detail["decision"])
        self.assertEqual([], detail["hard_blocks"])
        self.assertTrue(should_auto_publish_candidate(item, now=NOW))

    def test_detection_account_quote_of_zed_official_source_requires_review(self):
        from scripts.x_trigger_scoring import score_trigger_candidate_detail

        detail = score_trigger_candidate_detail(candidate(
            username="testingcatalog",
            category="detection",
            group="early rumor/detection",
            tier="core",
            text="Zed officially says ChatGPT subscription integration is live. https://x.com/zeddotdev/status/202",
            title="Zed adds ChatGPT subscription integration",
            body="TestingCatalog relays Zed's official post about ChatGPT subscription integration.",
            confidence="reported",
            tweet_id="103",
            quoted_status_id="202",
        ), now=NOW)

        self.assertGreaterEqual(detail["score"], 65)
        self.assertIn(detail["decision"], {"review", "auto_telegram_review_x"})
        self.assertNotEqual("auto_both", detail["decision"])

    def test_reporter_scoop_apple_openai_relationship_review_only(self):
        from scripts.x_trigger_scoring import score_trigger_candidate_detail

        detail = score_trigger_candidate_detail(candidate(
            username="alexeheath",
            category="scoop",
            group="reporters/inside sources",
            tier="scoop",
            text="Scoop: Apple and OpenAI's relationship has deteriorated over product integration talks.",
            title="Apple-OpenAI relationship reportedly worsens",
            body="A trusted reporter says the Apple-OpenAI relationship is under strain.",
            confidence="reported",
            tweet_id="104",
        ), now=NOW)

        self.assertGreaterEqual(detail["score"], 65)
        self.assertEqual("review", detail["decision"])
        self.assertNotEqual("auto_both", detail["decision"])

    def test_gemini_price_performance_rumor_is_hard_blocked(self):
        from scripts.x_trigger_scoring import score_trigger_candidate_detail

        detail = score_trigger_candidate_detail(candidate(
            username="testingcatalog",
            category="detection",
            text="Rumor: Gemini Pro pricing and performance tier may change next week.",
            title="Gemini Pro price and performance rumor",
            body="A watcher account reports an unverified Gemini Pro pricing and performance rumor.",
            confidence="rumor",
            tweet_id="105",
        ), now=NOW)

        self.assertEqual("rumor", detail["confidence"])
        self.assertTrue(detail["hard_blocks"])
        self.assertNotEqual("auto_both", detail["decision"])

    def test_tsmc_intel_market_commentary_drops(self):
        from scripts.x_trigger_scoring import score_trigger_candidate_detail

        detail = score_trigger_candidate_detail(candidate(
            username="marketwatcher",
            category="watch",
            group="markets",
            text="TSMC and Intel stock sentiment looks weak after AI capex fears hit the market.",
            title="TSMC and Intel market reaction",
            body="The post is mainly about stock sentiment and market reaction.",
            confidence="unknown",
            tweet_id="106",
        ), now=NOW)

        self.assertLess(detail["score"], 45)
        self.assertEqual("drop", detail["decision"])

    def test_monet_style_aesthetic_reaction_drops(self):
        from scripts.x_trigger_scoring import score_trigger_candidate_detail

        detail = score_trigger_candidate_detail(candidate(
            username="designer",
            category="watch",
            text="This AI image has such Monet vibes. Beautiful light and texture.",
            title="Monet style reaction",
            body="A personal aesthetic reaction to an AI image.",
            confidence="unknown",
            tweet_id="107",
        ), now=NOW)

        self.assertLess(detail["score"], 45)
        self.assertEqual("drop", detail["decision"])

    def test_event_ticket_announcement_never_auto_publishes(self):
        from scripts.x_trigger_scoring import score_trigger_candidate_detail

        detail = score_trigger_candidate_detail(candidate(
            username="latentspacepod",
            category="coding_agent",
            group="AI coding/agents",
            text="Tickets are open for our AI agents conference and live podcast taping.",
            title="AI agents conference tickets open",
            body="The post promotes event tickets and a podcast appearance.",
            confidence="verified",
            tweet_id="108",
        ), now=NOW)

        self.assertIn(detail["decision"], {"drop", "watch"})
        self.assertNotIn(detail["decision"], {"auto_both", "auto_telegram_review_x"})

    def test_recent_duplicate_story_key_penalty_blocks_auto_both(self):
        from scripts.x_trigger_scoring import build_story_key, score_trigger_candidate_detail

        original = candidate(
            username="OpenAI",
            category="official",
            group="OpenAI",
            tier="auto",
            text="Introducing ChatGPT personal finance, rolling out today.",
            title="OpenAI launches ChatGPT personal finance",
            body="OpenAI is rolling out ChatGPT personal finance.",
            confidence="verified",
            tweet_id="109",
        )
        key = build_story_key(original)
        detail = score_trigger_candidate_detail(
            original,
            now=NOW,
            state={"published_story_keys": {key: "2026-05-16T10:00:00Z"}},
        )

        self.assertTrue(any(p["name"] == "duplicate_story_key" for p in detail["penalties"]))
        self.assertIn(detail["decision"], {"drop", "watch", "review"})
        self.assertNotEqual("auto_both", detail["decision"])

    def test_duplicate_story_key_blocks_even_high_explicit_score(self):
        from scripts.x_trigger_scoring import build_story_key, score_trigger_candidate_detail, should_auto_publish_candidate

        item = candidate(
            username="OpenAI",
            category="official",
            group="OpenAI",
            tier="auto",
            text="Introducing ChatGPT personal finance, rolling out today.",
            title="OpenAI launches ChatGPT personal finance",
            body="OpenAI is rolling out ChatGPT personal finance.",
            confidence="verified",
            tweet_id="112",
        )
        item["summary"]["score"] = 100
        key = build_story_key(item)
        state = {"published_story_keys": {key: "2026-05-16T10:00:00Z"}}
        detail = score_trigger_candidate_detail(item, now=NOW, state=state)

        self.assertIn("duplicate_story_key", detail["hard_blocks"])
        self.assertEqual(55, detail["score"])
        self.assertNotEqual("auto_both", detail["decision"])
        self.assertFalse(should_auto_publish_candidate(item, now=NOW, state=state))

    def test_explicit_score_does_not_bypass_hard_blocks(self):
        from scripts.x_trigger_scoring import score_trigger_candidate_detail, should_auto_publish_candidate

        item = candidate(
            username="testingcatalog",
            category="detection",
            text="Rumor: Gemini Pro pricing may change soon.",
            title="Gemini Pro pricing rumor",
            body="An unverified watcher rumor about Gemini Pro pricing.",
            confidence="rumor",
            tweet_id="110",
        )
        item["summary"]["score"] = 99
        detail = score_trigger_candidate_detail(item, now=NOW)

        self.assertTrue(detail["hard_blocks"])
        self.assertNotEqual("auto_both", detail["decision"])
        self.assertFalse(should_auto_publish_candidate(item, now=NOW))


if __name__ == "__main__":
    unittest.main()
