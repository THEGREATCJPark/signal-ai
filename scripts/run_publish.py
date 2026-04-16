#!/usr/bin/env python3
"""
First Light AI 발행 스크립트.
기존 JSON 결과물(docs/articles.json)을 읽어서 Telegram/X에 발행합니다.

Usage:
    python scripts/run_publish.py --dry-run
    python scripts/run_publish.py --platform telegram
    python scripts/run_publish.py --force --platform both
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

DEFAULT_INPUT = os.path.join(ROOT, "docs", "articles.json")


def normalize_articles(raw) -> list[dict]:
    """다양한 JSON 형식을 bot/이 기대하는 스키마로 정규화.

    지원 형식:
    1. Discord 다이제스트 (dict with "articles" key, headline/body 필드)
    2. 플랫 리스트 (list of dicts with title/summary 필드)
    """
    if isinstance(raw, dict) and "articles" in raw:
        source = raw.get("source", "unknown")
        timestamp = raw.get("generated_at", datetime.now(timezone.utc).isoformat())
        articles = raw["articles"]
    elif isinstance(raw, list):
        return raw  # 이미 플랫 형식이면 그대로 반환
    else:
        print("오류: 지원하지 않는 JSON 구조입니다.")
        return []

    normalized = []
    for a in articles:
        title = a.get("headline", a.get("title", "제목 없음"))
        aid = a.get("id", "")
        if not aid:
            aid = hashlib.sha256(title.encode()).hexdigest()[:12]

        body = a.get("body", a.get("summary", ""))
        summary = body[:200] + "..." if len(body) > 200 else body
        if not summary:
            summary = "요약 없음"

        normalized.append({
            "id": aid,
            "source": a.get("source", source),
            "title": title,
            "url": a.get("url", ""),
            "score": a.get("score", 0),
            "comments": a.get("comments", 0),
            "timestamp": a.get("timestamp", timestamp),
            "summary": summary,
            "media": a.get("media", []),
        })

    return normalized


def main():
    parser = argparse.ArgumentParser(description="First Light AI 발행")
    parser.add_argument("--input", default=DEFAULT_INPUT,
                        help="입력 JSON 파일 경로 (기본: docs/articles.json)")
    parser.add_argument("--dry-run", action="store_true",
                        help="실제 발행 없이 미리보기")
    parser.add_argument("--platform", choices=["telegram", "x", "both"],
                        default="both", help="발행 플랫폼 (기본: both)")
    parser.add_argument("--force", action="store_true",
                        help="이미 발행된 기사도 재발행")
    parser.add_argument("--limit", type=int, default=0,
                        help="발행할 최대 기사 수 (0=무제한)")
    args = parser.parse_args()

    # 입력 파일 확인
    if not os.path.exists(args.input):
        print(f"오류: 입력 파일을 찾을 수 없습니다: {args.input}")
        sys.exit(1)

    # JSON 로드
    with open(args.input, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # 정규화
    articles = normalize_articles(raw)
    if not articles:
        print("발행할 기사가 없습니다.")
        return

    print(f"로드된 기사: {len(articles)}개 (소스: {args.input})")

    # 발행
    from bot.scheduler import publish
    publish(articles, dry_run=args.dry_run, platform=args.platform,
           force=args.force, limit=args.limit)


if __name__ == "__main__":
    main()
