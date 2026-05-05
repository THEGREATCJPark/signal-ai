#!/usr/bin/env python3
"""Run the hourly X watch crawler locally and ingest its JSONL output."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CRAWLED_DIR = ROOT / "data" / "crawled"
CRAWLER_COMMAND = ROOT / "crawlers" / "x_watch.py"


def ensure_local_only(allow_ci: bool = False) -> None:
    if os.getenv("GITHUB_ACTIONS") == "true" and not allow_ci:
        raise SystemExit("X watch crawling is local-only. Do not run it on GitHub Actions.")


def today_jsonl_path() -> Path:
    today = datetime.now().strftime("%Y-%m-%d")
    return CRAWLED_DIR / f"x_watch-{today}.jsonl"


def run_crawler(extra_args: list[str] | None = None) -> None:
    command = [sys.executable, str(CRAWLER_COMMAND)]
    if extra_args:
        command.extend(extra_args)
    subprocess.run(command, cwd=ROOT, check=True)


def resolve_paths(paths: list[Path]) -> list[Path]:
    resolved = paths or [today_jsonl_path()]
    missing = [path for path in resolved if not path.exists()]
    if missing:
        joined = ", ".join(str(path) for path in missing)
        raise SystemExit(f"Missing X watch JSONL file(s): {joined}")
    return resolved


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path, help="Optional x_watch JSONL paths to ingest")
    parser.add_argument("--skip-crawl", action="store_true", help="Ingest existing JSONL only")
    parser.add_argument("--allow-ci", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--instances", help="Comma-separated Nitter instance base URLs")
    parser.add_argument("--delay", type=float, help="Delay between account feed requests in seconds")
    args = parser.parse_args(argv)

    ensure_local_only(allow_ci=args.allow_ci)
    if not args.skip_crawl:
        crawler_args: list[str] = []
        if args.instances:
            crawler_args.extend(["--instances", args.instances])
        if args.delay is not None:
            crawler_args.extend(["--delay", str(args.delay)])
        run_crawler(crawler_args)

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from db.ingest import ingest_paths

    result = ingest_paths(resolve_paths(args.paths), batch_size=args.batch_size)
    print(f"Total upserted: {result['inserted']}, skipped: {result['skipped']}")
    print("By source:")
    for source, count in sorted(result["by_source"].items(), key=lambda item: (-item[1], item[0])):
        print(f"  {source:15s} {count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
