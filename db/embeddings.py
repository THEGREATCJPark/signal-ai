"""
article_embeddings 테이블 — 벡터 저장/검색 (pgvector).
"""

from db.client import get_client


def upsert_embedding(article_id: str, embedding: list[float], model_name: str = "multilingual-e5-small"):
    """기사 임베딩 저장."""
    client = get_client()
    row = {
        "article_id": article_id,
        "embedding": embedding,
        "model_name": model_name,
    }
    client.table("article_embeddings").upsert(row).execute()


def get_embedding(article_id: str) -> list[float] | None:
    """기사 임베딩 조회."""
    client = get_client()
    result = (
        client.table("article_embeddings")
        .select("embedding")
        .eq("article_id", article_id)
        .execute()
    )
    if result.data:
        return result.data[0]["embedding"]
    return None


def search_similar(query_embedding: list[float], top_k: int = 10, threshold: float = 0.0) -> list[dict]:
    """코사인 유사도로 유사 기사 검색.

    Supabase에서 pgvector RPC 함수를 호출.
    사전에 match_articles SQL function이 생성되어 있어야 함.
    """
    client = get_client()
    result = client.rpc(
        "match_articles",
        {
            "query_embedding": query_embedding,
            "match_count": top_k,
            "match_threshold": threshold,
        },
    ).execute()
    return result.data or []


def list_unembedded_articles(limit: int = 100) -> list[dict]:
    """임베딩이 아직 생성되지 않은 기사 조회."""
    client = get_client()
    # articles LEFT JOIN article_embeddings WHERE embedding IS NULL
    result = client.rpc("get_unembedded_articles", {"lim": limit}).execute()
    return result.data or []
