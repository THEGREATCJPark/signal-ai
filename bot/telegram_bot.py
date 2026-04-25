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


def _send_photo(image_path: str, caption: str = "") -> dict:
    """텔레그램 Bot API로 이미지 발송 (로컬 파일)"""
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
    """단일 기사를 텔레그램 채널에 발행 (이미지 포함)"""
    message = format_article(article)
    media = article.get("media", [])

    # 이미지가 있으면 첫 번째 이미지와 함께 발송
    if media:
        image_path = media[0].get("path", "")
        if image_path and os.path.exists(image_path):
            return _send_photo(image_path, caption=message)

    return _send_message(message, disable_preview=False)


def send_daily_digest(articles: list[dict]) -> dict:
    """일일 다이제스트(헤더 + 인덱스)를 한 메시지로 발행. 본문 잘림 가능성 있어서
    기본 발행 경로(scheduler.publish)는 send_digest_header + send_article 루프를 사용."""
    message = format_daily_digest(articles)
    return _send_message(message, disable_preview=True)


def send_digest_header(count: int) -> dict:
    """기사별 메시지를 보내기 전, 짧은 헤더를 먼저 발행."""
    from datetime import datetime

    today = datetime.now().strftime("%Y년 %m월 %d일")
    text = (
        f"📡 <b>First Light AI — {today} 브리핑</b>\n"
        f"오늘 {count}건의 소식을 공유합니다.\n"
        + ("━" * 20)
    )
    return _send_message(text, disable_preview=True)


def send_test_message() -> dict:
    """봇 연결 테스트용 메시지 발송"""
    return _send_message("First Light AI 봇 연결 테스트 완료!")


if __name__ == "__main__":
    result = send_test_message()
    if result.get("ok"):
        print("테스트 메시지 발송 성공!")
    else:
        print(f"발송 실패: {result}")
