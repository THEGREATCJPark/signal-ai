#!/usr/bin/env python3
"""Run the 08:30 KST daily publish path from the local automation machine."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bot.scheduler import publish  # noqa: E402
from scripts.run_publish import load_raw_articles, normalize_articles  # noqa: E402
from scripts.sync_articles_to_supabase import sync_articles  # noqa: E402
from scripts.validate_articles import validate  # noqa: E402

DEFAULT_INPUT = ROOT / "docs" / "articles.json"


def run_daily_publish_local(
    *,
    input_path: Path = DEFAULT_INPUT,
    platform: str = "both",
    dry_run: bool = False,
    force: bool = False,
    limit: int = 0,
    allow_partial: bool = True,
) -> int:
    load_dotenv(ROOT / ".env", override=False)
    os.environ.setdefault("USE_DB", "true")

    ok, errors = validate(str(input_path), require_fresh_kst=True)
    if not ok:
        for error in errors:
            print(f"FAIL: {error}", file=sys.stderr)
        return 1

    if dry_run:
        source = "file"
    else:
        sync_articles(input_path)
        source = "supabase"

    raw = load_raw_articles(source, input_path)
    articles = normalize_articles(raw)
    publish(
        articles,
        dry_run=dry_run,
        platform=platform,
        force=force,
        limit=limit,
        strict=not allow_partial,
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--platform", choices=["telegram", "x", "both"], default="both")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--allow-partial", action="store_true", default=True)
    parser.add_argument("--strict", dest="allow_partial", action="store_false")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return run_daily_publish_local(
        input_path=args.input,
        platform=args.platform,
        dry_run=args.dry_run,
        force=args.force,
        limit=args.limit,
        allow_partial=args.allow_partial,
    )


if __name__ == "__main__":
    raise SystemExit(main())
