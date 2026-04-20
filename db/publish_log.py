"""Supabase-backed publish log helpers."""
from __future__ import annotations

from datetime import datetime, timezone
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


class DBPublishedState:
    """Published-state facade backed only by Supabase."""

    def __init__(self, platform: str = "github_pages"):
        self.platform = platform

    def published_ids(self) -> set[str]:
        return {str(row["article_id"]) for row in list_published(self.platform)}

    def is_published(self, article_id: str) -> bool:
        return str(article_id) in self.published_ids()

    def mark(self, article_id: str, message_id: str | None = None) -> None:
        mark_published(str(article_id), self.platform, message_id=message_id)
