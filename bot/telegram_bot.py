import os

import requests
from dotenv import load_dotenv

from bot.formatter import format_article, format_daily_digest

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"


def _send_message(text: str, disable_preview: bool = False) -> dict:
    """Send a message through the Telegram Bot API."""
    resp = requests.post(
        f"{API_BASE}/sendMessage",
        json={
            "chat_id": CHANNEL_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_preview,
        },
    )
    resp.raise_for_status()
    return resp.json()


def _send_photo(image_path: str, caption: str = "") -> dict:
    """Send a local image through the Telegram Bot API."""
    with open(image_path, "rb") as photo:
        resp = requests.post(
            f"{API_BASE}/sendPhoto",
            data={
                "chat_id": CHANNEL_ID,
                "caption": caption[:1024],
                "parse_mode": "HTML",
            },
            files={"photo": photo},
        )
    resp.raise_for_status()
    return resp.json()


def send_article(article: dict) -> dict:
    """Publish a single content-only article message to Telegram."""
    message = format_article(article)
    media = article.get("media", [])

    if media:
        image_path = media[0].get("path", "")
        if image_path and os.path.exists(image_path):
            return _send_photo(image_path, caption=message)

    return _send_message(message, disable_preview=False)


def send_daily_digest(articles: list[dict]) -> dict:
    """Publish a content-only digest in one Telegram message."""
    message = format_daily_digest(articles)
    return _send_message(message, disable_preview=True)


def send_digest_header(count: int) -> dict:
    """Legacy helper retained for manual use; scheduler no longer calls this."""
    return _send_message(f"<b>AI 최전방 뉴스</b>\n오늘 {count}건의 소식을 공유합니다.", disable_preview=True)


def send_test_message() -> dict:
    """Send a Telegram bot connectivity test message."""
    return _send_message("AI 최전방 뉴스 봇 연결 테스트 완료!")


if __name__ == "__main__":
    result = send_test_message()
    if result.get("ok"):
        print("테스트 메시지 발송 성공!")
    else:
        print(f"발송 실패: {result}")
