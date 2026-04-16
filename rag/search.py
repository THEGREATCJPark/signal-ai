"""
벡터 유사도 검색 — 자연어 질문으로 관련 기사 찾기.

사용법:
    from rag.search import search

    results = search("현재 SOTA 모델이 뭐야?")
    for r in results:
        print(r["title"], r["similarity"])
"""

from db.embeddings import search_similar
from rag.embedder import embed


def search(query: str, top_k: int = 10, threshold: float = 0.3) -> list[dict]:
    """자연어 쿼리로 유사 기사 검색.

    Args:
        query: 검색 질문 (자연어)
        top_k: 반환할 최대 기사 수
        threshold: 최소 유사도 (0.0~1.0)

    Returns:
        유사도 내림차순 정렬된 기사 목록.
        각 dict에 similarity 필드 포함.
    """
    query_embedding = embed(query, prefix="query: ")
    results = search_similar(query_embedding, top_k=top_k, threshold=threshold)
    return results


def find_related(article_id: str, top_k: int = 5) -> list[dict]:
    """특정 기사와 관련된 기사 검색."""
    from db.embeddings import get_embedding

    embedding = get_embedding(article_id)
    if embedding is None:
        return []

    results = search_similar(embedding, top_k=top_k + 1, threshold=0.3)
    # 자기 자신 제외
    return [r for r in results if r["article_id"] != article_id][:top_k]
