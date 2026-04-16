"""
publish_log 테이블 — 발행 이력 관리.

PublishedState와 동일한 인터페이스를 제공하여 기존 코드 호환성 유지.
"""

from datetime import datetime, timezone

from db.client import get_client


class DBPublishedState:
    """DB 기반 발행 상태 관리 (publisher/state.py의 PublishedState 대체)."""

    def is_published(self, article_id: str, platform: str) -> bool:
        """해당 기사가 특정 플랫폼에 발행되었는지 확인."""
        client = get_client()
        result = (
            client.table("publish_log")
            .select("id")
            .eq("article_id", article_id)
            .eq("platform", platform)
            .execute()
        )
        return len(result.data) > 0

    def mark_published(self, article_id: str, platform: str, message_id: str | None = None):
        """발행 완료 기록."""
        client = get_client()
        row = {
            "article_id": article_id,
            "platform": platform,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        if message_id:
            row["message_id"] = message_id
        client.table("publish_log").upsert(row, on_conflict="article_id,platform").execute()

    def get_unpublished(self, articles: list[dict], platform: str) -> list[dict]:
        """아직 발행되지 않은 기사 필터링."""
        from db.articles import article_key

        return [
            a for a in articles
            if not self.is_published(article_key(a), platform)
        ]

    def save(self):
        """DB는 자동 저장이므로 no-op. 기존 인터페이스 호환용."""
        pass


def get_publish_history(platform: str | None = None, limit: int = 50) -> list[dict]:
    """발행 이력 조회."""
    client = get_client()
    query = client.table("publish_log").select("*").order("published_at", desc=True)
    if platform:
        query = query.eq("platform", platform)
    result = query.limit(limit).execute()
    return result.data or []
