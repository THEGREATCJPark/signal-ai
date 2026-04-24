"""Supabase helpers for generated public articles and pipeline state."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from db.client import get_client

PUBLIC_STATE_KEY = "public_state"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _tags(article: dict[str, Any]) -> list[str]:
    tags = article.get("tags")
    if isinstance(tags, list):
        return [str(t) for t in tags]
    out = []
    for key in ("category", "trust", "placement"):
        value = article.get(key)
        if value:
            out.append(str(value))
    return out


def article_to_row(article: dict[str, Any], generated_at: str | None = None) -> dict[str, Any]:
    article_id = str(article.get("id") or "").strip()
    if not article_id:
        raise ValueError("generated article is missing id")
    created_at = article.get("created_at") or article.get("placed_at") or generated_at or _now_iso()
    raw_json = dict(article)
    return {
        "id": article_id,
        "source": str(article.get("source") or "first_light_ai"),
        "title": str(article.get("headline") or article.get("title") or "").strip(),
        "url": article.get("url") or article.get("source_url"),
        "score": _safe_int(article.get("score")),
        "comments": _safe_int(article.get("comments")),
        "summary": article.get("summary"),
        "body": article.get("body"),
        "tags": _tags(article),
        "raw_json": raw_json,
        "crawled_at": created_at,
        "created_at": created_at,
        "placement": article.get("placement"),
        "placed_at": article.get("placed_at"),
        "category": article.get("category"),
        "trust": article.get("trust"),
        "generated_at": generated_at,
        "updated_at": _now_iso(),
    }


def upsert_generated_articles(articles: list[dict[str, Any]], generated_at: str | None = None) -> int:
    rows = [article_to_row(article, generated_at=generated_at) for article in articles]
    if not rows:
        return 0
    get_client(service=True).table("articles").upsert(rows, on_conflict="id").execute()
    return len(rows)


def save_pipeline_state(key: str, value: dict[str, Any]) -> None:
    row = {"key": key, "value": value, "updated_at": _now_iso()}
    get_client(service=True).table("pipeline_state").upsert(row, on_conflict="key").execute()


def load_pipeline_state(key: str) -> dict[str, Any] | None:
    res = (
        get_client(service=True)
        .table("pipeline_state")
        .select("value")
        .eq("key", key)
        .limit(1)
        .execute()
    )
    data = list(getattr(res, "data", None) or [])
    if not data:
        return None
    value = data[0].get("value")
    if isinstance(value, str):
        return json.loads(value)
    return value


def save_public_state(state: dict[str, Any]) -> None:
    generated_at = state.get("generated_at")
    save_pipeline_state(PUBLIC_STATE_KEY, state)
    upsert_generated_articles(list(state.get("articles") or []), generated_at=generated_at)


def load_public_state() -> dict[str, Any] | None:
    return load_pipeline_state(PUBLIC_STATE_KEY)
