"""Supabase-backed publish log helpers.

Exposes two surfaces:

- Functional: ``mark_published`` / ``list_published`` for ad-hoc scripts
  (e.g. the GitHub Pages writer).
- ``DBPublishedState`` object matching ``publisher.state.PublishedState`` API
  so callers can switch between JSON-file and Supabase backends transparently
  via ``publisher.state.get_state()`` when ``USE_DB=true``.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from db.client import get_client


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def mark_published(article_id: str, platform: str, message_id: str | None = None) -> None:
    row = {
        "article_id": article_id,
        "platform": platform,
        "message_id": message_id,
        "published_at": _now_iso(),
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
    """Supabase ``publish_log`` facade with the same API as ``PublishedState``.

    Lookups are cached for the lifetime of the instance. ``mark_published``
    writes through to the DB immediately; ``save()`` is a no-op kept for
    interface parity with the JSON-file variant.
    """

    def __init__(self) -> None:
        self._cache: dict[str, set[str]] = {}

    def _ensure_platform(self, platform: str) -> set[str]:
        if platform not in self._cache:
            self._cache[platform] = {
                str(row["article_id"]) for row in list_published(platform)
            }
        return self._cache[platform]

    def is_published(self, article_id: str, platform: str) -> bool:
        return str(article_id) in self._ensure_platform(platform)

    def mark_published(self, article_id: str, platform: str, message_id: str | None = None) -> None:
        mark_published(str(article_id), platform, message_id=message_id)
        self._ensure_platform(platform).add(str(article_id))

    def get_unpublished(self, articles: list[dict], platform: str) -> list[dict]:
        from publisher.state import article_key  # local import: avoid cycle

        published = self._ensure_platform(platform)
        return [a for a in articles if article_key(a) not in published]

    def save(self) -> None:  # noqa: D401 - interface parity
        """No-op. Writes happen inline in ``mark_published``."""
        return
