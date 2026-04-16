"""
발행 상태 관리 — data/published.json 기반 중복 발행 방지.

구조:
{
  "signal-001": {
    "telegram": "2026-04-15T10:00:00+00:00",
    "x": "2026-04-15T10:00:05+00:00"
  }
}
"""

import hashlib
import json
import os
from datetime import datetime, timezone

DEFAULT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "published.json")


class PublishedState:
    def __init__(self, path: str = DEFAULT_PATH):
        self.path = os.path.abspath(path)
        self._state: dict = {}
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self._state = json.load(f)

    def save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._state, f, ensure_ascii=False, indent=2)

    def is_published(self, article_id: str, platform: str) -> bool:
        return platform in self._state.get(article_id, {})

    def mark_published(self, article_id: str, platform: str):
        if article_id not in self._state:
            self._state[article_id] = {}
        self._state[article_id][platform] = datetime.now(timezone.utc).isoformat()

    def get_unpublished(self, articles: list[dict], platform: str) -> list[dict]:
        return [
            a for a in articles
            if not self.is_published(article_key(a), platform)
        ]


def article_key(article: dict) -> str:
    """기사의 stable key 생성. id가 있으면 사용, 없으면 title 해시."""
    aid = article.get("id", "")
    if aid:
        return aid
    title = article.get("title", "")
    return hashlib.sha256(title.encode()).hexdigest()[:12]
