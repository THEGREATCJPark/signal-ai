#!/usr/bin/env python3
"""Ingest JSONL crawler output into Supabase posts.

Parallel to db/ingest.py (SQLite). Same JSONL input contract; writes to
Supabase public.posts via db.posts.upsert_posts (on_conflict source,source_id).

Usage:
  python3 db/supabase_ingest.py                 # ingest all data/crawled/*.jsonl
  python3 db/supabase_ingest.py file1.jsonl ... # ingest specific files

Requires env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from db.posts import upsert_posts

ROOT = Path(__file__).resolve().parents[1]
CRAWLED_DIR = ROOT / "data" / "crawled"
REQUIRED_FIELDS = ("source", "source_id", "content", "timestamp")


def _metadata(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {"value": value}
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    return {"value": value}


def normalize_post(payload: dict[str, Any]) -> dict[str, Any]:
    missing = [field for field in REQUIRED_FIELDS if payload.get(field) in (None, "")]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")
    timestamp = str(payload["timestamp"])
    return {
        "source": str(payload["source"]),
        "source_id": str(payload["source_id"]),
        "source_url": payload.get("source_url"),
        "author": payload.get("author"),
        "content": str(payload["content"]),
        "timestamp": timestamp,
        "parent_id": payload.get("parent_id"),
        "metadata": _metadata(payload.get("metadata")),
        "fetched_at": payload.get("fetched_at") or timestamp,
    }


def _flush(batch: list[dict[str, Any]], counts: Counter[str]) -> int:
    if not batch:
        return 0
    rows = list(batch)
    inserted = upsert_posts(rows)
    for row in batch:
        counts[row["source"]] += 1
    batch.clear()
    return inserted


def ingest_paths(paths: list[Path], batch_size: int = 500) -> dict[str, Any]:
    inserted = 0
    skipped = 0
    by_source: Counter[str] = Counter()
    batch: list[dict[str, Any]] = []

    for path in paths:
        with path.open(encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                    batch.append(normalize_post(payload))
                except (json.JSONDecodeError, ValueError, TypeError) as exc:
                    print(f"  [{path.name}:{line_no}] skipped: {exc}", file=sys.stderr)
                    skipped += 1
                    continue
                if len(batch) >= batch_size:
                    inserted += _flush(batch, by_source)
    inserted += _flush(batch, by_source)
    return {"inserted": inserted, "skipped": skipped, "by_source": dict(by_source)}


def default_paths() -> list[Path]:
    return sorted(CRAWLED_DIR.glob("*.jsonl"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="*", type=Path)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    paths = args.paths or default_paths()
    if not paths:
        raise SystemExit(f"No JSONL files found in {CRAWLED_DIR}")

    existing = []
    for path in paths:
        if path.exists():
            existing.append(path)
        else:
            print(f"  skip: {path} not found", file=sys.stderr)
    if not existing:
        raise SystemExit("No ingestable JSONL files found")

    result = ingest_paths(existing, batch_size=args.batch_size)
    print(f"Total upserted: {result['inserted']}, skipped: {result['skipped']}")
    print("By source:")
    for source, count in sorted(result["by_source"].items(), key=lambda item: (-item[1], item[0])):
        print(f"  {source:15s} {count}")


if __name__ == "__main__":
    main()
