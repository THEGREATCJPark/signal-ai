#!/usr/bin/env python3
"""Sync generated First Light AI articles into Supabase.

This only writes generated article state to Supabase. It does not publish to
Telegram or X.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db.articles import save_public_state  # noqa: E402

DEFAULT_INPUT = ROOT / "docs" / "articles.json"


def load_public_articles_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {path}")

    with path.open("r", encoding="utf-8") as f:
        state = json.load(f)

    if not isinstance(state, dict):
        raise ValueError("articles JSON은 dict 형식이어야 합니다.")
    articles = state.get("articles")
    if not isinstance(articles, list):
        raise ValueError("articles JSON에 list 형식의 'articles'가 필요합니다.")
    return state


def sync_articles(input_path: str | Path = DEFAULT_INPUT, dry_run: bool = False) -> int:
    path = Path(input_path)
    state = load_public_articles_json(path)
    count = len(state.get("articles") or [])

    if dry_run:
        print(f"[DRY-RUN] Supabase articles/public_state 동기화 예정: {count}개 ({path})")
        return count

    save_public_state(state)
    print(f"Supabase articles/public_state 동기화 완료: {count}개 ({path})")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync generated First Light AI articles to Supabase")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="입력 articles.json 경로")
    parser.add_argument("--dry-run", action="store_true", help="Supabase 쓰기 없이 검증만 수행")
    args = parser.parse_args()

    try:
        sync_articles(args.input, dry_run=args.dry_run)
    except Exception as exc:
        print(f"오류: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
