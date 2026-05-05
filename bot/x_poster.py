import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests_oauthlib import OAuth1

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

load_dotenv()

X_CLIENT_ID = os.getenv("X_CLIENT_ID")
X_CLIENT_SECRET = os.getenv("X_CLIENT_SECRET")
X_REFRESH_TOKEN = os.getenv("X_REFRESH_TOKEN")
X_REFRESH_TOKEN_STATE_KEY = os.getenv("X_REFRESH_TOKEN_STATE_KEY", "x_oauth2_refresh_token")
X_API_KEY = os.getenv("X_API_KEY")
X_API_SECRET = os.getenv("X_API_SECRET")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")

TWEET_URL = os.getenv("X_TWEET_URL", "https://api.twitter.com/2/tweets")
TOKEN_URL = "https://api.x.com/2/oauth2/token"


def _load_stored_refresh_token() -> str | None:
    """Load the latest rotated X refresh token from mutable pipeline state."""
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        return None
    try:
        from db.articles import load_pipeline_state

        state = load_pipeline_state(X_REFRESH_TOKEN_STATE_KEY) or {}
    except Exception as exc:
        print(f"[x] refresh token state load skipped: {exc}")
        return None
    token = state.get("refresh_token")
    return str(token).strip() if token else None


def _save_stored_refresh_token(refresh_token: str) -> None:
    """Persist a rotated X refresh token for the next scheduled run."""
    if not refresh_token:
        return
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        print("[x] WARNING: Supabase is not configured; cannot persist rotated refresh_token")
        return

    from datetime import datetime, timezone

    from db.articles import save_pipeline_state

    save_pipeline_state(
        X_REFRESH_TOKEN_STATE_KEY,
        {
            "refresh_token": refresh_token,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    print("[x] rotated refresh_token persisted to Supabase pipeline_state")


def _get_access_token() -> str:
    """Refresh OAuth 2.0 access token, persisting rotated refresh tokens."""
    refresh_token = _load_stored_refresh_token() or X_REFRESH_TOKEN
    if not X_CLIENT_ID or not refresh_token:
        raise RuntimeError("X_CLIENT_ID or X_REFRESH_TOKEN environment variable is missing")

    auth = (X_CLIENT_ID, X_CLIENT_SECRET) if X_CLIENT_SECRET else None
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    if not X_CLIENT_SECRET:
        payload["client_id"] = X_CLIENT_ID

    resp = requests.post(TOKEN_URL, auth=auth, data=payload, timeout=30)
    if not resp.ok:
        print(f"[x] Token endpoint {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()

    data = resp.json()
    new_refresh = data.get("refresh_token")
    if new_refresh and new_refresh != refresh_token:
        _save_stored_refresh_token(new_refresh)
    return data["access_token"]


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
    """Post a tweet. OAuth 1.0a is preferred; OAuth 2.0 is a fallback."""
    if _has_oauth1_credentials():
        print("[x] Auth mode: OAuth 1.0a User Context")
        resp = requests.post(
            TWEET_URL,
            auth=_oauth1_auth(),
            headers={"Content-Type": "application/json"},
            json={"text": text[:280]},
            timeout=30,
        )
    else:
        print("[x] Auth mode: OAuth 2.0 refresh-token fallback")
        access_token = _get_access_token()
        resp = requests.post(
            TWEET_URL,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={"text": text[:280]},
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


def build_daily_summary_text(articles: list[dict]) -> str:
    lines = []
    for i, article in enumerate(articles[:5], 1):
        title = str(article.get("title") or "").strip()
        if title:
            lines.append(f"{i}. {title}")

    return _fit_tweet("\n".join(lines))


def post_article(article: dict) -> dict:
    """Post a single article to X without digest branding."""
    return post_tweet(build_article_post_text(article))


def post_daily_summary(articles: list[dict]) -> dict:
    """Post article titles to X without headers, footers, or hashtags."""
    return post_tweet(build_daily_summary_text(articles))
