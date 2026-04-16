"""
articles 테이블 CRUD.
"""

import hashlib
import json
from datetime import datetime, timezone

from db.client import get_client


def article_key(article: dict) -> str:
    """기사의 stable key 생성. id가 있으면 사용, 없으면 title 해시."""
    aid = article.get("id", "")
    if aid:
        return aid
    title = article.get("title", "")
    return hashlib.sha256(title.encode()).hexdigest()[:12]


def upsert_article(article: dict) -> dict:
    """기사 저장 (이미 존재하면 업데이트)."""
    client = get_client()
    row = {
        "id": article_key(article),
        "source": article.get("source", "unknown"),
        "title": article.get("title", ""),
        "url": article.get("url"),
        "score": article.get("score", 0),
        "comments": article.get("comments", 0),
        "summary": article.get("summary"),
        "body": article.get("body"),
        "tags": article.get("tags", []),
        "raw_json": json.dumps(article, ensure_ascii=False),
        "crawled_at": article.get("timestamp", datetime.now(timezone.utc).isoformat()),
    }
    result = client.table("articles").upsert(row).execute()
    return result.data[0] if result.data else row


def upsert_articles(articles: list[dict]) -> list[dict]:
    """여러 기사를 일괄 저장."""
    return [upsert_article(a) for a in articles]


def get_article(article_id: str) -> dict | None:
    """ID로 기사 조회."""
    client = get_client()
    result = client.table("articles").select("*").eq("id", article_id).execute()
    return result.data[0] if result.data else None


def list_articles(limit: int = 50, offset: int = 0, source: str | None = None) -> list[dict]:
    """기사 목록 조회 (최신순)."""
    client = get_client()
    query = client.table("articles").select("*").order("crawled_at", desc=True)
    if source:
        query = query.eq("source", source)
    result = query.range(offset, offset + limit - 1).execute()
    return result.data or []


def list_articles_by_date(date_str: str) -> list[dict]:
    """특정 날짜의 기사 조회 (YYYY-MM-DD)."""
    client = get_client()
    result = (
        client.table("articles")
        .select("*")
        .gte("crawled_at", f"{date_str}T00:00:00Z")
        .lt("crawled_at", f"{date_str}T23:59:59Z")
        .order("score", desc=True)
        .execute()
    )
    return result.data or []


def search_articles_by_keyword(keyword: str, limit: int = 20) -> list[dict]:
    """제목/요약에서 키워드 검색 (ILIKE)."""
    client = get_client()
    result = (
        client.table("articles")
        .select("*")
        .or_(f"title.ilike.%{keyword}%,summary.ilike.%{keyword}%")
        .order("crawled_at", desc=True)
        .limit(limit)
        .execute()
    )
    return result.data or []
