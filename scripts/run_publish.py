#!/usr/bin/env python3
"""First Light AI 발행 스크립트.

Supabase ``articles`` 테이블(또는 JSON 파일)을 읽어서 Telegram/X에 발행.

Usage:
    # Supabase에서 최근 24h 내 top/main 기사 발행 (기본, dry-run)
    python scripts/run_publish.py --source supabase --dry-run

    # 로컬 JSON 파일
    python scripts/run_publish.py --source file --input docs/articles.json --dry-run

    # 실발행
    python scripts/run_publish.py --source supabase --platform telegram
"""

from __future__ import annotations

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
    """JSON payload → bot/scheduler publish() 스키마로 정규화."""
    if isinstance(raw, dict) and "articles" in raw:
        source = raw.get("source", "unknown")
        timestamp = raw.get("generated_at", datetime.now(timezone.utc).isoformat())
        articles = raw["articles"]
    elif isinstance(raw, list):
        return raw
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


def _load_from_file(path: str) -> list[dict]:
    if not os.path.exists(path):
        print(f"오류: 입력 파일을 찾을 수 없습니다: {path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return normalize_articles(json.load(f))


def _load_from_supabase(since_hours: int, placements: list[str] | None, limit: int) -> list[dict]:
    from db.articles import fetch_recent_articles

    rows = fetch_recent_articles(
        since_hours=since_hours,
        placements=placements,
        limit=max(limit or 200, 1),
    )
    normalized = []
    for r in rows:
        title = r.get("title") or r.get("headline") or "제목 없음"
        aid = str(r.get("id") or hashlib.sha256(title.encode()).hexdigest()[:12])
        body = r.get("body") or r.get("summary") or ""
        summary = r.get("summary") or (body[:200] + ("..." if len(body) > 200 else ""))
        normalized.append({
            "id": aid,
            "source": r.get("source") or "first_light_ai",
            "title": title,
            "url": r.get("url") or "",
            "score": r.get("score") or 0,
            "comments": r.get("comments") or 0,
            "timestamp": r.get("generated_at") or r.get("created_at"),
            "summary": summary or "요약 없음",
            "media": r.get("media") or [],
            "placement": r.get("placement"),
            "category": r.get("category"),
            "trust": r.get("trust"),
        })
    return normalized


def main():
    parser = argparse.ArgumentParser(description="First Light AI 발행")
    parser.add_argument("--source", choices=["supabase", "file"], default="supabase",
                        help="발행 대상 소스 (기본: supabase)")
    parser.add_argument("--input", default=DEFAULT_INPUT,
                        help="--source=file 일 때 읽을 JSON 경로")
    parser.add_argument("--since-hours", type=int, default=24,
                        help="--source=supabase 일 때 최근 N시간 내 기사만 (0=무제한, 기본 24)")
    parser.add_argument("--placements", default="top,main",
                        help="--source=supabase 일 때 placement 화이트리스트 (콤마 구분, 빈값=필터 없음)")
    parser.add_argument("--dry-run", action="store_true",
                        help="실제 발행 없이 미리보기")
    parser.add_argument("--platform", choices=["telegram", "x", "both"],
                        default="both", help="발행 플랫폼 (기본: both)")
    parser.add_argument("--force", action="store_true",
                        help="이미 발행된 기사도 재발행")
    parser.add_argument("--limit", type=int, default=0,
                        help="발행할 최대 기사 수 (0=무제한)")
    args = parser.parse_args()

    if args.source == "supabase":
        placements = [p.strip() for p in args.placements.split(",") if p.strip()] or None
        articles = _load_from_supabase(args.since_hours, placements, args.limit or 200)
        label = f"supabase articles (since={args.since_hours}h, placements={placements})"
    else:
        articles = _load_from_file(args.input)
        label = args.input

    if not articles:
        print("발행할 기사가 없습니다.")
        return

    print(f"로드된 기사: {len(articles)}개 (소스: {label})")

    from bot.scheduler import publish
    publish(articles, dry_run=args.dry_run, platform=args.platform,
            force=args.force, limit=args.limit)


if __name__ == "__main__":
    main()
