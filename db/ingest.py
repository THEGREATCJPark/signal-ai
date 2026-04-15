#!/usr/bin/env python3
"""Ingest JSONL crawler output → SQLite DB.

Usage:
  python3 db/ingest.py                 # ingest all data/crawled/*.jsonl
  python3 db/ingest.py file1.jsonl ... # ingest specific files
"""
import json, sys
from pathlib import Path
import sqlite3

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "signal.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"
CRAWLED_DIR = ROOT / "data" / "crawled"

def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    # Apply schema
    conn.executescript(SCHEMA_PATH.read_text())
    return conn

def ingest_file(conn, path: Path):
    """Insert posts from a JSONL file. Returns (inserted, skipped)."""
    inserted = skipped = 0
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                p = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"  [{path.name}:{line_no}] bad JSON: {e}", file=sys.stderr)
                skipped += 1
                continue

            meta = p.get("metadata") or {}
            if isinstance(meta, dict):
                meta_str = json.dumps(meta, ensure_ascii=False)
            else:
                meta_str = str(meta)

            try:
                conn.execute("""
                    INSERT OR IGNORE INTO posts
                    (source, source_id, source_url, author, content,
                     timestamp, parent_id, metadata, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    p["source"],
                    str(p["source_id"]),
                    p.get("source_url"),
                    p.get("author"),
                    p["content"],
                    p["timestamp"],
                    p.get("parent_id"),
                    meta_str,
                    p.get("fetched_at") or p["timestamp"],
                ))
                if conn.total_changes > 0 and conn.execute(
                    "SELECT changes()"
                ).fetchone()[0] > 0:
                    inserted += 1
                else:
                    skipped += 1
            except (KeyError, sqlite3.Error) as e:
                print(f"  [{path.name}:{line_no}] error: {e}", file=sys.stderr)
                skipped += 1
    return inserted, skipped

def main():
    paths = [Path(p) for p in sys.argv[1:]] if len(sys.argv) > 1 else sorted(CRAWLED_DIR.glob("*.jsonl"))
    if not paths:
        print(f"No JSONL files found in {CRAWLED_DIR}", file=sys.stderr)
        sys.exit(1)

    conn = connect()
    total_ins = total_skip = 0
    for path in paths:
        if not path.exists():
            print(f"  skip: {path} not found", file=sys.stderr)
            continue
        before = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        ins, skip = ingest_file(conn, path)
        conn.commit()
        after = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        real_ins = after - before
        print(f"  {path.name}: +{real_ins} new (tried {ins}, skipped {skip})")
        total_ins += real_ins
        total_skip += skip

    total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    print(f"\nTotal inserted: {total_ins}, skipped/dedup: {total_skip}")
    print(f"DB now has {total} posts at {DB_PATH}")

    # Source breakdown
    print("\nBy source:")
    for src, cnt in conn.execute("SELECT source, COUNT(*) FROM posts GROUP BY source ORDER BY 2 DESC"):
        print(f"  {src:15s} {cnt}")

    conn.close()

if __name__ == "__main__":
    main()
