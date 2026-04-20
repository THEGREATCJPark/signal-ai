"""Supabase helpers for raw source posts."""
from __future__ import annotations

from typing import Any

from db.client import get_client

POST_COLUMNS = (
    "id, source, source_id, source_url, author, content, timestamp, "
    "parent_id, metadata, fetched_at"
)


def upsert_posts(rows: list[dict[str, Any]]) -> int:
    """Batch upsert raw posts by (source, source_id)."""
    if not rows:
        return 0
    get_client(service=True).table("posts").upsert(
        rows,
        on_conflict="source,source_id",
    ).execute()
    return len(rows)


def list_recent_posts_by_source(days: int = 3, per_source: int = 15) -> list[dict[str, Any]]:
    """Return top scored recent posts per source via the Supabase RPC."""
    res = get_client(service=True).rpc(
        "get_recent_posts_by_source",
        {"days": int(days), "per_source": int(per_source)},
    ).execute()
    return list(getattr(res, "data", None) or [])


def count_posts() -> int:
    """Return the current Supabase posts count."""
    res = get_client(service=True).table("posts").select("id", count="exact").limit(1).execute()
    count = getattr(res, "count", None)
    return int(count or 0)
