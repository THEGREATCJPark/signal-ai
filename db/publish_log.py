"""Supabase-backed publish log helpers."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from typing import Any

from db.client import get_client


def mark_published(article_id: str, platform: str, message_id: str | None = None) -> None:
    row = {
        "article_id": article_id,
        "platform": platform,
        "message_id": message_id,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    get_client(service=True).table("publish_log").upsert(
        row,
        on_conflict="article_id,platform",
    ).execute()


def list_published(platform: str | None = None) -> list[dict[str, Any]]:
    query = get_client(service=True).table("publish_log").select("*")
    if platform:
        query = query.eq("platform", platform)
    res = query.execute()
    return list(getattr(res, "data", None) or [])


def _article_key(article: dict[str, Any]) -> str:
    article_id = str(article.get("id") or "").strip()
    if article_id:
        return article_id
    title = str(article.get("title") or article.get("headline") or "")
    return hashlib.sha256(title.encode()).hexdigest()[:12]


class DBPublishedState:
    """Published-state facade backed only by Supabase."""

    def __init__(self, platform: str = "github_pages"):
        self.platform = platform

    def published_ids(self, platform: str | None = None) -> set[str]:
        target_platform = platform or self.platform
        return {str(row["article_id"]) for row in list_published(target_platform)}

    def is_published(self, article_id: str, platform: str | None = None) -> bool:
        return str(article_id) in self.published_ids(platform)

    def mark(self, article_id: str, message_id: str | None = None) -> None:
        mark_published(str(article_id), self.platform, message_id=message_id)

    def mark_published(
        self,
        article_id: str,
        platform: str | None = None,
        message_id: str | None = None,
    ) -> None:
        mark_published(str(article_id), platform or self.platform, message_id=message_id)

    def get_unpublished(
        self,
        articles: list[dict[str, Any]],
        platform: str | None = None,
    ) -> list[dict[str, Any]]:
        published = self.published_ids(platform)
        return [article for article in articles if _article_key(article) not in published]

    def save(self) -> None:
        return None
