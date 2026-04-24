#!/usr/bin/env python3
"""One-time backfill from local data/signal.db posts to Supabase posts.

After a successful non-dry-run backfill, the SQLite file is moved to a .bak
file so the pipeline cannot accidentally keep using it.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "signal.db"
sys.path.insert(0, str(ROOT))

from db.posts import upsert_posts


def _metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {"value": str(value)}
    return parsed if isinstance(parsed, dict) else {"value": parsed}


def _row_to_post(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "source": row["source"],
        "source_id": str(row["source_id"]),
        "source_url": row["source_url"],
        "author": row["author"],
        "content": row["content"],
        "timestamp": row["timestamp"],
        "parent_id": row["parent_id"],
        "metadata": _metadata(row["metadata"]),
        "fetched_at": row["fetched_at"] or row["timestamp"],
    }


def iter_posts(db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        for row in conn.execute(
            """
            select source, source_id, source_url, author, content,
                   timestamp, parent_id, metadata, fetched_at
            from posts
            order by id
            """
        ):
            yield _row_to_post(row)
    finally:
        conn.close()


def backfill(db_path: Path, batch_size: int, dry_run: bool) -> int:
    batch = []
    total = 0
    for post in iter_posts(db_path):
        batch.append(post)
        if len(batch) >= batch_size:
            if not dry_run:
                upsert_posts(batch)
            total += len(batch)
            print(f"backfilled {total} posts")
            batch = []
    if batch:
        if not dry_run:
            upsert_posts(batch)
        total += len(batch)
        print(f"backfilled {total} posts")
    return total


def archive_sqlite(db_path: Path) -> Path:
    suffix = datetime.now().strftime("%Y%m%d%H%M%S")
    archived = db_path.with_name(f"{db_path.name}.{suffix}.bak")
    shutil.move(str(db_path), str(archived))
    return archived


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-archive", action="store_true")
    args = parser.parse_args()

    if not args.db.exists():
        raise SystemExit(f"SQLite DB not found: {args.db}")
    total = backfill(args.db, args.batch_size, args.dry_run)
    print(f"total posts processed: {total}")
    if not args.dry_run and not args.no_archive:
        archived = archive_sqlite(args.db)
        print(f"archived SQLite DB: {archived}")


if __name__ == "__main__":
    main()
