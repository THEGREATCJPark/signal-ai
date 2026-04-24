import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

# OAuth 2.0 credentials
X_CLIENT_ID = os.getenv("X_CLIENT_ID")
X_CLIENT_SECRET = os.getenv("X_CLIENT_SECRET")
X_REFRESH_TOKEN = os.getenv("X_REFRESH_TOKEN")

TWEET_URL = "https://api.x.com/2/tweets"
TOKEN_URL = "https://api.x.com/2/oauth2/token"


def _get_access_token() -> str:
    """Refresh token으로 새 access token 발급"""
    resp = requests.post(
        TOKEN_URL,
        auth=(X_CLIENT_ID, X_CLIENT_SECRET),
        data={
            "grant_type": "refresh_token",
            "refresh_token": X_REFRESH_TOKEN,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"]


def post_tweet(text: str) -> dict:
    """트윗 게시 (280자 제한, OAuth 2.0 Bearer)"""
    access_token = _get_access_token()

    resp = requests.post(
        TWEET_URL,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json={"text": text[:280]},
    )
    print(f"[x] Status: {resp.status_code}, Response: {resp.text[:300]}")
    resp.raise_for_status()
    return resp.json().get("data", {})


def post_article(article: dict) -> dict:
    """단일 기사를 X에 포스팅"""
    title = article.get("title", "")
    url = article.get("url", "")
    source = article.get("source", "")
    score = article.get("score", 0)

    text = f"📡 {title}\n\n📌 {source} | 📊 {score}점\n🔗 {url}\n\n#AI #FirstLightAI"
    return post_tweet(text)


def post_daily_summary(articles: list[dict]) -> dict:
    """일일 요약을 X에 포스팅"""
    from datetime import datetime

    today = datetime.now().strftime("%m/%d")
    lines = [f"📡 First Light AI {today} 브리핑\n"]

    for i, article in enumerate(articles[:5], 1):
        title = article.get("title", "")
        if len(title) > 40:
            title = title[:37] + "..."
        lines.append(f"{i}. {title}")

    lines.append("\n#AI #FirstLightAI")
    return post_tweet("\n".join(lines))
