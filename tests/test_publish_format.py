import importlib
import os
import unittest
from unittest.mock import patch


class PublishFormatTest(unittest.TestCase):
    def test_x_daily_summary_posts_content_only(self):
        import bot.x_poster as x_poster

        article = {
            "title": "GPT-5.5, 사이버 보안 벤치마크에서 강력한 성능 입증",
            "url": "https://example.com/gpt-55",
        }

        with patch.object(x_poster, "post_tweet", return_value={"id": "1"}) as post_tweet:
            x_poster.post_daily_summary([article])

        text = post_tweet.call_args.args[0]
        self.assertEqual(
            text,
            "1. GPT-5.5, 사이버 보안 벤치마크에서 강력한 성능 입증",
        )
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

    def test_x_trigger_article_posts_daily_style_title_only(self):
        import bot.x_poster as x_poster

        captured = {}

        def fake_post_tweet(text):
            captured["text"] = text
            return {"id": "1"}

        article = {
            "source": "x_trigger",
            "title": "OpenAI 발표",
            "summary": "OpenAI가 실시간 음성 API 개선 내용을 공개했습니다.",
            "url": "https://x.com/OpenAIDevs/status/2051453905343828350",
            "raw_json": {
                "account": {"username": "OpenAIDevs"},
                "tweet": {"id": "2051453905343828350"},
            },
        }

        with patch.object(x_poster, "post_tweet", side_effect=fake_post_tweet):
            x_poster.post_article(article)

        self.assertNotIn("https://x.com/", captured["text"])
        self.assertEqual("1. OpenAI 발표", captured["text"])
        self.assertNotIn("OpenAIDevs", captured["text"])
        self.assertNotIn("실시간 음성 API", captured["text"])

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


if __name__ == "__main__":
    unittest.main()
