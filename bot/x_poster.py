import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests_oauthlib import OAuth1

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

load_dotenv()

# OAuth 2.0 credentials
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
    """Load latest rotated X refresh token from mutable pipeline state."""
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
    """Persist rotated X refresh token for the next scheduled run."""
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
    """Refresh token으로 새 access token 발급.

    X OAuth 2.0의 refresh_token은 사용할 때마다 **회전(rotate)**한다. 응답에 새
    refresh_token이 포함되며 이전 값은 즉시 무효화된다. GitHub Secrets에 한 번
    박아두면 다음 호출부터 invalid_grant 400.

    여기서는 응답 body를 로그로 노출해서 실제 에러(invalid_grant / invalid_client /
    invalid_request)를 식별 가능하게 하고, 새 refresh_token을 stdout에 출력해 수동
    회전이 가능하게 한다. (영속화는 다음 단계: Supabase pipeline_state 권장.)
    """
    refresh_token = _load_stored_refresh_token() or X_REFRESH_TOKEN
    if not X_CLIENT_ID or not refresh_token:
        raise RuntimeError("X_CLIENT_ID 또는 X_REFRESH_TOKEN 환경변수 없음")

    auth = (X_CLIENT_ID, X_CLIENT_SECRET) if X_CLIENT_SECRET else None
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    # Public client(secret 없음): client_id를 body로 함께 보내야 함.
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
    """Post a tweet. OAuth 1.0a preferred; OAuth 2.0 fallback."""
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
