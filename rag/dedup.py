"""
시맨틱 중복 제거 — 코사인 유사도 기반.

같은 뉴스가 HackerNews, Reddit, GeekNews 등 여러 소스에서 수집될 때
title hash가 아닌 의미적 유사도로 중복을 감지한다.
"""

from db.articles import article_key
from db.clusters import add_to_cluster, create_cluster, find_cluster_for_article
from db.embeddings import search_similar, upsert_embedding
from rag.embedder import embed_article

SIMILARITY_THRESHOLD = 0.85  # 이 이상이면 같은 뉴스로 판단


def process_article(article: dict) -> dict:
    """기사를 임베딩하고, 유사 기사가 있으면 클러스터에 묶는다.

    Returns:
        {
            "article_id": str,
            "is_duplicate": bool,
            "cluster_id": int | None,
            "similar_to": str | None,  # 유사 기사 ID
            "similarity": float | None,
        }
    """
    aid = article_key(article)

    # 1. 임베딩 생성
    embedding = embed_article(article)

    # 2. DB에 임베딩 저장
    upsert_embedding(aid, embedding)

    # 3. 유사 기사 검색 (자기 자신 제외)
    similar = search_similar(embedding, top_k=5, threshold=SIMILARITY_THRESHOLD)
    similar = [s for s in similar if s["article_id"] != aid]

    if not similar:
        return {
            "article_id": aid,
            "is_duplicate": False,
            "cluster_id": None,
            "similar_to": None,
            "similarity": None,
        }

    # 4. 가장 유사한 기사
    best = similar[0]

    # 5. 클러스터 처리
    existing_cluster = find_cluster_for_article(best["article_id"])
    if existing_cluster:
        cluster_id = existing_cluster
    else:
        # 새 클러스터 생성 (대표: 점수가 높은 기사)
        cluster_id = create_cluster(
            representative_id=best["article_id"],
            cluster_name=best.get("title", "")[:100],
        )
        add_to_cluster(cluster_id, best["article_id"], similarity=1.0)

    add_to_cluster(cluster_id, aid, similarity=best["similarity"])

    return {
        "article_id": aid,
        "is_duplicate": True,
        "cluster_id": cluster_id,
        "similar_to": best["article_id"],
        "similarity": best["similarity"],
    }


def deduplicate_articles(articles: list[dict]) -> list[dict]:
    """기사 목록에서 시맨틱 중복을 제거한 결과 반환.

    각 클러스터에서 score가 가장 높은 기사만 남긴다.
    """
    results = []
    seen_clusters: set[int] = set()

    # score 내림차순 정렬 — 높은 점수 기사가 대표가 됨
    sorted_articles = sorted(articles, key=lambda x: x.get("score", 0), reverse=True)

    for article in sorted_articles:
        info = process_article(article)

        if not info["is_duplicate"]:
            results.append(article)
        elif info["cluster_id"] not in seen_clusters:
            # 클러스터의 첫 번째(=최고 점수) 기사만 포함
            results.append(article)
            seen_clusters.add(info["cluster_id"])

    return results
