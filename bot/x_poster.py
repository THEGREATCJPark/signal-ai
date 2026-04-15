import json
import os

import requests
from dotenv import load_dotenv
from requests_oauthlib import OAuth1

load_dotenv()

API_KEY = os.getenv("X_API_KEY")
API_SECRET = os.getenv("X_API_SECRET")
ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")

TWEET_URL = "https://api.twitter.com/2/tweets"


def _get_auth() -> OAuth1:
    """OAuth 1.0a 인증 객체 생성"""
    return OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)


def post_tweet(text: str) -> dict:
    """트윗 게시 (280자 제한)"""
    print(f"[x-debug] API_KEY: {API_KEY[:4] if API_KEY else 'EMPTY'}..., "
          f"API_SECRET: {'SET' if API_SECRET else 'EMPTY'}, "
          f"ACCESS_TOKEN: {ACCESS_TOKEN[:4] if ACCESS_TOKEN else 'EMPTY'}..., "
          f"ACCESS_TOKEN_SECRET: {'SET' if ACCESS_TOKEN_SECRET else 'EMPTY'}")

    auth = _get_auth()
    payload = {"text": text[:280]}

    resp = requests.post(TWEET_URL, auth=auth, json=payload)
    print(f"[x-debug] Status: {resp.status_code}, Response: {resp.text[:500]}")

    resp.raise_for_status()
    return resp.json().get("data", {})


def post_article(article: dict) -> dict:
    """단일 기사를 X에 포스팅"""
    title = article.get("title", "")
    url = article.get("url", "")
    source = article.get("source", "")
    score = article.get("score", 0)

    text = f"📡 {title}\n\n📌 {source} | 📊 {score}점\n🔗 {url}\n\n#AI #SignalAI"
    return post_tweet(text)


def post_daily_summary(articles: list[dict]) -> dict:
    """일일 요약을 X에 포스팅"""
    from datetime import datetime

    today = datetime.now().strftime("%m/%d")
    lines = [f"📡 Signal AI {today} 브리핑\n"]

    for i, article in enumerate(articles[:5], 1):
        title = article.get("title", "")
        if len(title) > 40:
            title = title[:37] + "..."
        lines.append(f"{i}. {title}")

    lines.append("\n#AI #SignalAI #구구브리핑")
    return post_tweet("\n".join(lines))
