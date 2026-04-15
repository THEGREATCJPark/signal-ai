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


def _get_api_v1() -> tweepy.API:
    """X API v1.1 (이미지 업로드용)"""
    auth = tweepy.OAuth1UserHandler(
        API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
    )
    return tweepy.API(auth)


def _upload_media(image_path: str) -> int | None:
    """이미지를 X에 업로드하고 media_id 반환"""
    if not image_path or not os.path.exists(image_path):
        return None
    api = _get_api_v1()
    media = api.media_upload(filename=image_path)
    return media.media_id


def post_tweet(text: str, media_path: str = None) -> dict:
    """트윗 게시 (280자 제한, 이미지 선택)"""
    client = _get_client()
    media_ids = None

    if media_path:
        media_id = _upload_media(media_path)
        if media_id:
            media_ids = [media_id]

    response = client.create_tweet(text=text[:280], media_ids=media_ids)
    return response.data


def post_article(article: dict) -> dict:
    """단일 기사를 X에 포스팅 (이미지 포함)"""
    title = article.get("title", "")
    url = article.get("url", "")
    source = article.get("source", "")
    score = article.get("score", 0)

    text = f"📡 {title}\n\n📌 {source} | 📊 {score}점\n🔗 {url}\n\n#AI #SignalAI"

    # 첫 번째 이미지 첨부
    media = article.get("media", [])
    media_path = media[0].get("path") if media else None

    return post_tweet(text, media_path=media_path)


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
