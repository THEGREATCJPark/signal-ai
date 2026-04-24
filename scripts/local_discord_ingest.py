#!/usr/bin/env python3
"""Run the local-only Discord crawler and upload its JSONL result to Supabase.

This script is intentionally blocked on GitHub Actions. Discord export stays on
the local WSL machine; only normalized JSONL rows are upserted to Supabase.
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


def ensure_local_only(allow_ci: bool = False) -> None:
    if os.getenv("GITHUB_ACTIONS") == "true" and not allow_ci:
        raise SystemExit(
            "Discord ingest is local-only. Do not run Discord export on GitHub Actions."
        )


def run_discord_crawler() -> None:
    subprocess.run(
        [sys.executable, str(ROOT / "crawlers" / "discord.py")],
        cwd=ROOT,
        check=True,
    )


def today_discord_jsonl() -> Path:
    today = datetime.now().strftime("%Y-%m-%d")
    return CRAWLED_DIR / f"discord-{today}.jsonl"


def resolve_paths(paths: list[Path]) -> list[Path]:
    if paths:
        resolved = paths
    else:
        resolved = [today_discord_jsonl()]
    missing = [path for path in resolved if not path.exists()]
    if missing:
        joined = ", ".join(str(path) for path in missing)
        raise SystemExit(f"Missing Discord JSONL file(s): {joined}")
    return resolved


def main() -> None:
    parser = argparse.ArgumentParser(description="Local Discord crawl -> Supabase posts")
    parser.add_argument("paths", nargs="*", type=Path, help="Optional Discord JSONL paths")
    parser.add_argument("--skip-crawl", action="store_true", help="Ingest existing JSONL only")
    parser.add_argument("--allow-ci", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    ensure_local_only(allow_ci=args.allow_ci)
    if not args.skip_crawl:
        run_discord_crawler()

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from db.ingest import ingest_paths

    paths = resolve_paths(args.paths)
    result = ingest_paths(paths, batch_size=args.batch_size)
    print(f"Discord upserted: {result['inserted']}, skipped: {result['skipped']}")
    print("By source:")
    for source, count in sorted(result["by_source"].items(), key=lambda item: (-item[1], item[0])):
        print(f"  {source:15s} {count}")


if __name__ == "__main__":
    main()
