import os

import requests
from dotenv import load_dotenv

from bot.formatter import format_article, format_daily_digest

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"


def _send_message(text: str, disable_preview: bool = False) -> dict:
    """텔레그램 Bot API로 메시지 발송"""
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


def send_article(article: dict) -> dict:
    """단일 기사를 텔레그램 채널에 발행"""
    message = format_article(article)
    return _send_message(message, disable_preview=False)


def send_daily_digest(articles: list[dict]) -> dict:
    """일일 다이제스트를 텔레그램 채널에 발행"""
    message = format_daily_digest(articles)
    return _send_message(message, disable_preview=True)


def send_test_message() -> dict:
    """봇 연결 테스트용 메시지 발송"""
    return _send_message("Signal AI 봇 연결 테스트 완료!")


if __name__ == "__main__":
    result = send_test_message()
    if result.get("ok"):
        print("테스트 메시지 발송 성공!")
    else:
        print(f"발송 실패: {result}")
