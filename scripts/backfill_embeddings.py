"""
기존 기사에 대한 임베딩 일괄 생성 스크립트.

DB에 저장된 기사 중 임베딩이 없는 것들을 찾아서 생성.

사용법:
    python scripts/backfill_embeddings.py
    python scripts/backfill_embeddings.py --batch-size 50
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db.articles import article_key
from db.embeddings import list_unembedded_articles, upsert_embedding
from rag.embedder import embed_article


def main():
    parser = argparse.ArgumentParser(description="기존 기사 임베딩 일괄 생성")
    parser.add_argument("--batch-size", type=int, default=100, help="한 번에 처리할 기사 수")
    args = parser.parse_args()

    print("임베딩이 없는 기사 조회 중...")
    articles = list_unembedded_articles(limit=args.batch_size)

    if not articles:
        print("모든 기사에 임베딩이 생성되어 있습니다.")
        return

    print(f"{len(articles)}개 기사에 임베딩 생성 시작...")

    success = 0
    for i, article in enumerate(articles, 1):
        try:
            embedding = embed_article(article)
            upsert_embedding(article["id"], embedding)
            success += 1
            if i % 10 == 0:
                print(f"  진행: {i}/{len(articles)}")
        except Exception as e:
            print(f"  실패 [{article['id']}]: {e}")

    print(f"\n완료: {success}/{len(articles)}개 임베딩 생성")


if __name__ == "__main__":
    main()
