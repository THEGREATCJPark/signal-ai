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
    """Refresh token으로 새 access token 발급.

    X OAuth 2.0의 refresh_token은 사용할 때마다 **회전(rotate)**한다. 응답에 새
    refresh_token이 포함되며 이전 값은 즉시 무효화된다. GitHub Secrets에 한 번
    박아두면 다음 호출부터 invalid_grant 400.

    여기서는 응답 body를 로그로 노출해서 실제 에러(invalid_grant / invalid_client /
    invalid_request)를 식별 가능하게 하고, 새 refresh_token을 stdout에 출력해 수동
    회전이 가능하게 한다. (영속화는 다음 단계: Supabase pipeline_state 권장.)
    """
    if not X_CLIENT_ID or not X_REFRESH_TOKEN:
        raise RuntimeError("X_CLIENT_ID 또는 X_REFRESH_TOKEN 환경변수 없음")

    auth = (X_CLIENT_ID, X_CLIENT_SECRET) if X_CLIENT_SECRET else None
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": X_REFRESH_TOKEN,
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
    if new_refresh and new_refresh != X_REFRESH_TOKEN:
        print("[x] WARNING: refresh_token rotated. Update GH Secret X_REFRESH_TOKEN to:")
        print(f"[x] NEW_REFRESH_TOKEN={new_refresh}")
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
