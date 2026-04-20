#!/usr/bin/env python3
"""One-time backfill of the current public articles.json state to Supabase."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db.articles import save_public_state


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, default=ROOT / "docs" / "articles.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    state = json.loads(args.path.read_text(encoding="utf-8"))
    if not args.dry_run:
        save_public_state(state)
    prefix = "would backfill public state: " if args.dry_run else "backfilled public state: "
    message = (
        prefix +
        f"{len(state.get('articles') or [])} articles, "
        f"last_run_at={state.get('last_run_at')}"
    )
    print(message)


if __name__ == "__main__":
    main()
