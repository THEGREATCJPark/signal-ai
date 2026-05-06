import importlib
import os
import unittest
from datetime import datetime
from unittest.mock import patch


class PublishFormatTest(unittest.TestCase):
    def test_x_daily_summary_posts_date_and_headlines_only(self):
        import bot.x_poster as x_poster

        articles = [
            {"title": "GPT-5.5가 보안 벤치마크에서 강한 성능을 보였습니다.", "url": "https://example.com/gpt-55"},
            {"title": "새 에이전트 기능 공개가 이어졌습니다.", "url": "https://example.com/agent"},
        ]

        with patch.object(x_poster, "post_tweet", return_value={"id": "1"}) as post_tweet:
            x_poster.post_tweet(x_poster.build_daily_summary_text(articles, now=datetime(2026, 5, 7)))

        text = post_tweet.call_args.args[0]
        self.assertIn("5\uc6d4 7\uc77c AI \ucd5c\uc804\ubc29 \uc18c\uc2dd", text)
        self.assertIn("1. GPT-5.5가 보안 벤치마크에서 강한 성능을 보였습니다.", text)
        self.assertIn("2. 새 에이전트 기능 공개가 이어졌습니다.", text)
        self.assertNotIn("https://example.com", text)
        self.assertNotIn("\uc6d0\ubb38", text)
        self.assertNotIn("First Light", text)
        self.assertNotIn("FirstLight", text)
        self.assertNotIn("AI 최전방 뉴스", text)
        self.assertNotIn("브리핑", text)
        self.assertNotIn("#AI", text)

    def test_x_article_posts_article_content_without_metadata_or_hashtags(self):
        import bot.x_poster as x_poster

        article = {
            "title": "새 모델 공개",
            "summary": "개발자에게 유의미한 변경점이 포함됐다.",
            "url": "https://example.com/model",
            "source": "TechCrunch",
            "score": 99,
        }

        with patch.object(x_poster, "post_tweet", return_value={"id": "1"}) as post_tweet:
            x_poster.post_article(article)

        text = post_tweet.call_args.args[0]
        self.assertIn("새 모델 공개", text)
        self.assertIn("개발자에게 유의미한 변경점이 포함됐다.", text)
        self.assertIn("https://example.com/model", text)
        self.assertNotIn("TechCrunch", text)
        self.assertNotIn("99", text)
        self.assertNotIn("FirstLight", text)
        self.assertNotIn("#AI", text)

    def test_x_article_text_is_trimmed_by_x_weighted_length(self):
        import bot.x_poster as x_poster

        article = {
            "title": "GPT-5.5, 사이버 보안 및 게임 벤치마크에서 강력한 성능 입증",
            "summary": "OpenAI의 최신 모델 GPT-5.5가 다양한 테스트에서 놀라운 성능을 보여주고 있습니다. " * 8,
            "url": "https://example.com/gpt-55",
        }

        text = x_poster.build_compact_article_post_text(article)

        self.assertLessEqual(x_poster._tweet_weight(text), 260)
        self.assertIn("https://example.com/gpt-55", text)

    def test_x_trigger_article_posts_headline_keywords_and_source(self):
        import bot.x_poster as x_poster

        captured = {}

        def fake_post_tweet(text):
            captured["text"] = text
            return {"id": "1"}

        article = {
            "source": "x_trigger",
            "title": "OpenAI가 ChatGPT에 GPT-5.5 Instant 배포를 시작했습니다.",
            "summary": "OpenAI가 더 자연스럽고 간결한 답변을 제공하는 새 모델을 배포합니다.",
            "url": "https://x.com/OpenAIDevs/status/2051453905343828350",
            "raw_json": {
                "account": {"username": "OpenAIDevs", "group": "OpenAI"},
                "tweet": {"id": "2051453905343828350", "text": "GPT-5.5 Instant is starting to roll out in ChatGPT."},
            },
        }

        with patch.object(x_poster, "post_tweet", side_effect=fake_post_tweet):
            x_poster.post_article(article)

        self.assertIn("OpenAI가 ChatGPT에 GPT-5.5 Instant 배포를 시작했습니다.", captured["text"])
        self.assertIn("\ud0a4\uc6cc\ub4dc:", captured["text"])
        self.assertIn("\ubaa8\ub378\ucd9c\uc2dc", captured["text"])
        self.assertIn("\uc2e0\ubaa8\ub378", captured["text"])
        self.assertIn("\uc81c\ud488\uc5c5\ub370\uc774\ud2b8", captured["text"])
        self.assertIn("ChatGPT", captured["text"])
        self.assertNotIn("@OpenAIDevs", captured["text"])
        self.assertIn("\uc6d0\ubb38: https://x.com/OpenAIDevs/status/2051453905343828350", captured["text"])
        self.assertNotIn("더 자연스럽고 간결한 답변", captured["text"])
        self.assertLessEqual(x_poster._tweet_weight(captured["text"]), 260)

    def test_telegram_article_formats_content_only(self):
        from bot.formatter import format_article

        text = format_article(
            {
                "title": "AI 에이전트 업데이트",
                "summary": "실사용 흐름이 단순해졌다.",
                "url": "https://example.com/agent",
                "source": "hackernews",
                "score": 321,
                "comments": 8,
            }
        )

        self.assertIn("<b>AI 에이전트 업데이트</b>", text)
        self.assertIn("실사용 흐름이 단순해졌다.", text)
        self.assertIn('href="https://example.com/agent"', text)
        self.assertNotIn("First Light", text)
        self.assertNotIn("AI 최전방 뉴스", text)
        self.assertNotIn("Hacker News", text)
        self.assertNotIn("321", text)
        self.assertNotIn("8", text)

    def test_scheduler_does_not_send_telegram_digest_header(self):
        with patch.dict(os.environ, {"TELEGRAM_PER_MESSAGE_DELAY": "0"}, clear=False):
            import bot.scheduler as scheduler

            scheduler = importlib.reload(scheduler)

        class State:
            def get_unpublished(self, articles, platform):
                return articles

            def mark_published(self, key, platform):
                return None

            def save(self):
                return None

        article = {
            "title": "짧은 뉴스",
            "summary": "요약",
            "url": "https://example.com/news",
            "score": 10,
        }

        with patch.object(scheduler, "get_state", return_value=State()), \
             patch.object(scheduler, "send_digest_header", create=True) as send_digest_header, \
             patch.object(scheduler, "send_article", return_value={"ok": True}), \
             patch.object(scheduler.time, "sleep"):
            scheduler.publish([article], platform="telegram", force=True)

        send_digest_header.assert_not_called()

    def test_scheduler_posts_x_daily_summary_once(self):
        with patch.dict(os.environ, {"TELEGRAM_PER_MESSAGE_DELAY": "0"}, clear=False):
            import bot.scheduler as scheduler

            scheduler = importlib.reload(scheduler)

        class State:
            def __init__(self):
                self.marked = []

            def get_unpublished(self, articles, platform):
                return articles

            def mark_published(self, key, platform):
                self.marked.append((key, platform))

            def save(self):
                return None

        state = State()
        articles = [
            {"id": f"a{i}", "title": f"Title {i}", "summary": "body", "score": 100 - i}
            for i in range(1, 7)
        ]
        posted = []

        with patch.object(scheduler, "get_state", return_value=state), \
             patch.object(scheduler, "post_daily_summary", side_effect=lambda articles: posted.append([a["id"] for a in articles]) or {"id": "tweet-1"}):
            scheduler.publish(articles, platform="x", force=True)

        self.assertEqual([[f"a{i}" for i in range(1, 7)]], posted)
        self.assertEqual(
            [(f"a{i}", "x") for i in range(1, 7)],
            state.marked,
        )

    def test_scheduler_can_tolerate_one_failed_platform_after_success(self):
        with patch.dict(os.environ, {"TELEGRAM_PER_MESSAGE_DELAY": "0"}, clear=False):
            import bot.scheduler as scheduler

            scheduler = importlib.reload(scheduler)

        class State:
            def get_unpublished(self, articles, platform):
                return articles

            def mark_published(self, key, platform):
                return None

            def save(self):
                return None

        article = {
            "id": "a1",
            "title": "짧은 뉴스",
            "summary": "요약",
            "url": "https://example.com/news",
            "score": 10,
        }

        with patch.object(scheduler, "get_state", return_value=State()), \
             patch.object(scheduler, "send_article", return_value={"ok": True}), \
             patch.object(scheduler, "post_daily_summary", side_effect=RuntimeError("x forbidden")), \
             patch.object(scheduler.time, "sleep"):
            scheduler.publish([article], platform="both", force=True, strict=False)

    def test_scheduler_still_fails_when_no_platform_succeeds(self):
        with patch.dict(os.environ, {"TELEGRAM_PER_MESSAGE_DELAY": "0"}, clear=False):
            import bot.scheduler as scheduler

            scheduler = importlib.reload(scheduler)

        class State:
            def get_unpublished(self, articles, platform):
                return articles

            def mark_published(self, key, platform):
                return None

            def save(self):
                return None

        article = {"id": "a1", "title": "짧은 뉴스", "score": 10}

        with patch.object(scheduler, "get_state", return_value=State()), \
             patch.object(scheduler, "post_daily_summary", side_effect=RuntimeError("x forbidden")):
            with self.assertRaises(RuntimeError):
                scheduler.publish([article], platform="x", force=True, strict=False)


if __name__ == "__main__":
    unittest.main()
