"""
article_clusters 테이블 — 유사 기사 그룹핑.
"""

from db.client import get_client


def create_cluster(representative_id: str, cluster_name: str | None = None) -> int:
    """새 클러스터 생성, 클러스터 ID 반환."""
    client = get_client()
    row = {
        "representative_id": representative_id,
        "cluster_name": cluster_name,
    }
    result = client.table("article_clusters").insert(row).execute()
    return result.data[0]["id"]


def add_to_cluster(cluster_id: int, article_id: str, similarity: float):
    """기사를 기존 클러스터에 추가."""
    client = get_client()
    row = {
        "cluster_id": cluster_id,
        "article_id": article_id,
        "similarity": similarity,
    }
    client.table("article_cluster_members").upsert(row).execute()


def get_cluster_members(cluster_id: int) -> list[dict]:
    """클러스터의 모든 기사 조회."""
    client = get_client()
    result = (
        client.table("article_cluster_members")
        .select("*, articles(*)")
        .eq("cluster_id", cluster_id)
        .order("similarity", desc=True)
        .execute()
    )
    return result.data or []


def find_cluster_for_article(article_id: str) -> int | None:
    """기사가 속한 클러스터 ID 반환."""
    client = get_client()
    result = (
        client.table("article_cluster_members")
        .select("cluster_id")
        .eq("article_id", article_id)
        .limit(1)
        .execute()
    )
    if result.data:
        return result.data[0]["cluster_id"]
    return None
