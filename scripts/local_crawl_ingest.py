#!/usr/bin/env python3
"""Run all crawlers locally and upload normalized JSONL rows to Supabase.

No crawling runs on GitHub Actions. This wrapper is for the local WSL scheduler:
public web sources plus Discord are crawled locally, then the resulting JSONL
files are upserted to Supabase `posts`.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CRAWLED_DIR = ROOT / "data" / "crawled"
CRAWLER_COMMANDS = (
    "crawlers/run_public.py",
    "crawlers/discord.py",
)


def ensure_local_only(allow_ci: bool = False) -> None:
    if os.getenv("GITHUB_ACTIONS") == "true" and not allow_ci:
        raise SystemExit("Crawling is local-only. Do not run crawlers on GitHub Actions.")


def run_crawlers(commands: tuple[str, ...] = CRAWLER_COMMANDS) -> None:
    for command in commands:
        subprocess.run([sys.executable, str(ROOT / command)], cwd=ROOT, check=True)


def today_jsonl_paths() -> list[Path]:
    today = datetime.now().strftime("%Y-%m-%d")
    return sorted(CRAWLED_DIR.glob(f"*-{today}.jsonl"))


def resolve_paths(paths: list[Path]) -> list[Path]:
    resolved = paths or today_jsonl_paths()
    if not resolved:
        raise SystemExit(f"No JSONL files found in {CRAWLED_DIR}")
    missing = [path for path in resolved if not path.exists()]
    if missing:
        joined = ", ".join(str(path) for path in missing)
        raise SystemExit(f"Missing JSONL file(s): {joined}")
    return resolved


def main() -> None:
    parser = argparse.ArgumentParser(description="Local all-source crawl -> Supabase posts")
    parser.add_argument("paths", nargs="*", type=Path, help="Optional JSONL paths to ingest")
    parser.add_argument("--skip-crawl", action="store_true", help="Ingest existing JSONL only")
    parser.add_argument("--allow-ci", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    ensure_local_only(allow_ci=args.allow_ci)
    if not args.skip_crawl:
        run_crawlers()

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from db.ingest import ingest_paths

    paths = resolve_paths(args.paths)
    result = ingest_paths(paths, batch_size=args.batch_size)
    print(f"Total upserted: {result['inserted']}, skipped: {result['skipped']}")
    print("By source:")
    for source, count in sorted(result["by_source"].items(), key=lambda item: (-item[1], item[0])):
        print(f"  {source:15s} {count}")


if __name__ == "__main__":
    main()
