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
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

DEFAULT_INPUT = os.path.join(ROOT, "docs", "articles.json")
DEFAULT_SOURCE = os.getenv("FIRST_LIGHT_PUBLISH_SOURCE", "file").strip().lower() or "file"


def load_raw_articles(source: str, input_path: str | os.PathLike) -> dict | list:
    """Load generated articles from a local JSON file or Supabase public state."""
    source = source.strip().lower()
    if source == "file":
        path = Path(input_path)
        if not path.exists():
            raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {path}")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    if source == "supabase":
        from db.articles import load_public_state

        state = load_public_state()
        if not state:
            raise RuntimeError("Supabase public_state가 비어 있습니다. 먼저 scripts/sync_articles_to_supabase.py를 실행하세요.")
        return state

    raise ValueError(f"지원하지 않는 소스입니다: {source}")


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
    parser.add_argument("--source", choices=["file", "supabase"], default=DEFAULT_SOURCE,
                        help="기사 입력 소스 (기본: FIRST_LIGHT_PUBLISH_SOURCE 또는 file)")
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

    # JSON 로드
    try:
        raw = load_raw_articles(args.source, args.input)
    except Exception as exc:
        print(f"오류: {exc}")
        sys.exit(1)

    # 정규화
    articles = normalize_articles(raw)
    if not articles:
        print("발행할 기사가 없습니다.")
        return

    source_label = args.input if args.source == "file" else "supabase:public_state"
    print(f"로드된 기사: {len(articles)}개 (소스: {source_label})")

    # 발행
    from bot.scheduler import publish
    publish(articles, dry_run=args.dry_run, platform=args.platform,
           force=args.force, limit=args.limit)


if __name__ == "__main__":
    main()
