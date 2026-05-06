import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

load_dotenv()

X_API_KEY = os.getenv("X_API_KEY")
X_API_SECRET = os.getenv("X_API_SECRET")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")

TWEET_URL = os.getenv("X_TWEET_URL", "https://api.twitter.com/2/tweets")
MAX_DAILY_SUMMARY_ITEMS = 5


def _has_oauth1_credentials() -> bool:
    return all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET])


def _oauth1_auth():
    if not _has_oauth1_credentials():
        raise RuntimeError("X OAuth 1.0a credentials are incomplete")
    try:
        from requests_oauthlib import OAuth1
    except ImportError as exc:
        raise RuntimeError("requests-oauthlib is required for X OAuth 1.0a posting") from exc

    return OAuth1(
        client_key=X_API_KEY,
        client_secret=X_API_SECRET,
        resource_owner_key=X_ACCESS_TOKEN,
        resource_owner_secret=X_ACCESS_TOKEN_SECRET,
    )


def post_tweet(text: str) -> dict:
    """Post a tweet using OAuth 1.0a User Context only."""
    payload = {"text": text[:280]}
    print(f"[x] Payload chars: {len(payload['text'])}")

    print("[x] Auth mode: OAuth 1.0a User Context")
    resp = requests.post(
        TWEET_URL,
        auth=_oauth1_auth(),
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    print(f"[x] Status: {resp.status_code}, Response: {resp.text[:300]}")
    resp.raise_for_status()
    return resp.json().get("data", {})


def _fit_tweet(text: str, limit: int = 280) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def build_article_post_text(article: dict) -> str:
    title = str(article.get("title") or "").strip()
    summary = str(article.get("summary") or "").strip()
    url = str(article.get("url") or "").strip()

    parts = [part for part in [title, summary, url] if part]
    return _fit_tweet("\n\n".join(parts))


def build_trigger_post_text(article: dict) -> str:
    """Build trigger X text in the same one-line shape as daily publishing."""
    return build_daily_summary_text([article])


def daily_summary_articles(articles: list[dict]) -> list[dict]:
    """Return the articles that can actually fit in the one-tweet daily summary."""
    return [
        article for article in articles
        if str(article.get("title") or "").strip()
    ][:MAX_DAILY_SUMMARY_ITEMS]


def build_daily_summary_text(articles: list[dict]) -> str:
    lines = []
    for i, article in enumerate(daily_summary_articles(articles), 1):
        title = str(article.get("title") or "").strip()
        if title:
            lines.append(f"{i}. {title}")

    return _fit_tweet("\n".join(lines))


def post_article(article: dict) -> dict:
    """Post a single article to X without digest branding."""
    if article.get("source") == "x_trigger":
        return post_tweet(build_trigger_post_text(article))
    return post_tweet(build_article_post_text(article))


def post_daily_summary(articles: list[dict]) -> dict:
    """Post article titles to X without headers, footers, or hashtags."""
    return post_tweet(build_daily_summary_text(articles))
