import os

import tweepy
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("X_API_KEY")
API_SECRET = os.getenv("X_API_SECRET")
ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")


def _get_client() -> tweepy.Client:
    """X API v2 클라이언트 생성"""
    return tweepy.Client(
        consumer_key=API_KEY,
        consumer_secret=API_SECRET,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET,
    )


def post_tweet(text: str) -> dict:
    """트윗 게시 (280자 제한)"""
    client = _get_client()
    response = client.create_tweet(text=text[:280])
    return response.data


def post_article(article: dict) -> dict:
    """단일 기사를 X에 포스팅"""
    title = article.get("title", "")
    url = article.get("url", "")
    source = article.get("source", "")
    score = article.get("score", 0)

    # 280자 제한에 맞춰 포맷
    text = f"📡 {title}\n\n📌 {source} | 📊 {score}점\n🔗 {url}\n\n#AI #SignalAI"
    return post_tweet(text)


def post_daily_summary(articles: list[dict]) -> dict:
    """일일 요약을 X에 포스팅"""
    from datetime import datetime

    today = datetime.now().strftime("%m/%d")
    lines = [f"📡 Signal AI {today} 브리핑\n"]

    for i, article in enumerate(articles[:5], 1):
        title = article.get("title", "")
        # 트윗 길이 제한을 위해 제목 축약
        if len(title) > 40:
            title = title[:37] + "..."
        lines.append(f"{i}. {title}")

    lines.append("\n#AI #SignalAI #구구브리핑")
    return post_tweet("\n".join(lines))
