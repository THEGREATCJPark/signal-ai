import asyncio
import os

from dotenv import load_dotenv
from telegram import Bot

from bot.formatter import format_article, format_daily_digest

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")

bot = Bot(token=BOT_TOKEN)


async def send_article(article: dict) -> None:
    """단일 기사를 텔레그램 채널에 발행"""
    message = format_article(article)
    await bot.send_message(
        chat_id=CHANNEL_ID,
        text=message,
        parse_mode="HTML",
        disable_web_page_preview=False,
    )


async def send_daily_digest(articles: list[dict]) -> None:
    """일일 다이제스트를 텔레그램 채널에 발행"""
    message = format_daily_digest(articles)
    await bot.send_message(
        chat_id=CHANNEL_ID,
        text=message,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


async def send_test_message() -> None:
    """봇 연결 테스트용 메시지 발송"""
    await bot.send_message(
        chat_id=CHANNEL_ID,
        text="Signal AI 봇 연결 테스트 완료!",
    )


if __name__ == "__main__":
    asyncio.run(send_test_message())
