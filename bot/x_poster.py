import os
import re
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests_oauthlib import OAuth1

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

load_dotenv()

X_API_KEY = os.getenv("X_API_KEY")
X_API_SECRET = os.getenv("X_API_SECRET")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")

TWEET_URL = os.getenv("X_TWEET_URL", "https://api.x.com/2/tweets")
V1_TWEET_URL = os.getenv("X_V1_TWEET_URL", "https://api.x.com/1.1/statuses/update.json")
MAX_DAILY_SUMMARY_ITEMS = 5


def _has_oauth1_credentials() -> bool:
    return all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET])


def _oauth1_auth() -> OAuth1:
    if not _has_oauth1_credentials():
        raise RuntimeError("X OAuth 1.0a credentials are incomplete")
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
    if resp.ok:
        return _tweet_response_data(resp)
    if resp.status_code in {401, 403}:
        print("[x] v2 create tweet rejected; retrying with OAuth 1.0a v1.1 status update")
        resp = requests.post(
            V1_TWEET_URL,
            auth=_oauth1_auth(),
            data={"status": payload["text"]},
            timeout=30,
        )
        print(f"[x] Status: {resp.status_code}, Response: {resp.text[:300]}")
    resp.raise_for_status()
    return _tweet_response_data(resp)


def _tweet_response_data(resp: requests.Response) -> dict:
    data = resp.json()
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        return data["data"]
    if isinstance(data, dict) and (data.get("id_str") or data.get("id")):
        return {
            "id": str(data.get("id_str") or data.get("id")),
            "text": str(data.get("text") or ""),
        }
    return data if isinstance(data, dict) else {}


def _fit_tweet(text: str, limit: int = 280) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _summary_lines(summary: str, max_lines: int) -> list[str]:
    summary = re.sub(r"\s+", " ", (summary or "").strip())
    if not summary or max_lines <= 0:
        return []
    parts = [part.strip() for part in re.split(r"(?<=[.!?。！？다요죠니다습니다])\s+", summary) if part.strip()]
    if not parts:
        parts = [summary]
    return parts[:max_lines]


def build_compact_article_post_text(article: dict, *, max_lines: int = 5, limit: int = 280) -> str:
    """Build compact title/summary/source text for X posts."""
    title = str(article.get("title") or article.get("headline") or "").strip()
    summary = str(article.get("summary") or article.get("body") or "").strip()
    url = str(article.get("url") or "").strip()

    summary_budget = max(0, max_lines - 1 - (1 if url else 0))
    lines = [line for line in [title] if line]
    lines.extend(_summary_lines(summary, summary_budget))
    if url:
        lines.append(url)

    text = "\n".join(lines[:max_lines])
    if len(text) <= limit:
        return text

    fixed_lines = [line for line in [title] if line]
    if url:
        reserved = len("\n".join(fixed_lines + [url]))
        summary_limit = max(0, limit - reserved - 1)
        fitted_summary = _fit_tweet(" ".join(_summary_lines(summary, summary_budget)), summary_limit)
        lines = fixed_lines + ([fitted_summary] if fitted_summary else []) + [url]
    else:
        reserved = len("\n".join(fixed_lines))
        summary_limit = max(0, limit - reserved - (1 if fixed_lines else 0))
        fitted_summary = _fit_tweet(" ".join(_summary_lines(summary, summary_budget)), summary_limit)
        lines = fixed_lines + ([fitted_summary] if fitted_summary else [])
    return _fit_tweet("\n".join(lines[:max_lines]), limit)


def build_article_post_text(article: dict) -> str:
    title = str(article.get("title") or "").strip()
    summary = str(article.get("summary") or "").strip()
    url = str(article.get("url") or "").strip()

    parts = [part for part in [title, summary, url] if part]
    return _fit_tweet("\n\n".join(parts))


def build_trigger_post_text(article: dict) -> str:
    """Build trigger X text from the same title/summary/url shape as Telegram."""
    return build_compact_article_post_text(article)


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
