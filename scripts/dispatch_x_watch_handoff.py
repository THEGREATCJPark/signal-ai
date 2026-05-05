#!/usr/bin/env python3
"""Run only the X watch crawler, then hand its JSONL to Actions secrets ingest."""
from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
CRAWLED_DIR = ROOT / "data" / "crawled"
CRAWLER_COMMAND = "crawlers/x_watch.py"
HANDOFF_COMMAND = "scripts/dispatch_local_crawl_handoff.py"


def ensure_local_only() -> None:
    if os.getenv("GITHUB_ACTIONS") == "true":
        raise SystemExit("X watch handoff must run locally, not on GitHub Actions.")


def today_jsonl_path() -> Path:
    today = datetime.now().strftime("%Y-%m-%d")
    return CRAWLED_DIR / f"x_watch-{today}.jsonl"


def run_x_watch(extra_args: list[str] | None = None) -> Path:
    command = [sys.executable, str(ROOT / CRAWLER_COMMAND)]
    if extra_args:
        command.extend(extra_args)
    subprocess.run(command, cwd=ROOT, check=True)
    path = today_jsonl_path()
    if not path.exists():
        raise SystemExit(f"X watch crawler did not produce {path}")
    return path


def handoff_path(path: Path, *, no_wait: bool = False, batch_size: int = 500) -> None:
    command = [
        sys.executable,
        str(ROOT / HANDOFF_COMMAND),
        "--skip-crawl",
        "--batch-size",
        str(batch_size),
    ]
    if no_wait:
        command.append("--no-wait")
    command.append(str(path))
    subprocess.run(command, cwd=ROOT, check=True)


def run_handoff(
    *,
    no_wait: bool = False,
    batch_size: int = 500,
    crawler_args: list[str] | None = None,
) -> None:
    ensure_local_only()
    path = run_x_watch(crawler_args)
    handoff_path(path, no_wait=no_wait, batch_size=batch_size)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-wait", action="store_true", help="Trigger Actions ingest and exit immediately")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--instances", help="Comma-separated Nitter instance base URLs")
    parser.add_argument("--delay", type=float, help="Delay between account feed requests in seconds")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    crawler_args: list[str] = []
    if args.instances:
        crawler_args.extend(["--instances", args.instances])
    if args.delay is not None:
        crawler_args.extend(["--delay", str(args.delay)])
    run_handoff(no_wait=args.no_wait, batch_size=args.batch_size, crawler_args=crawler_args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
