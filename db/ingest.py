#!/usr/bin/env python3
"""Ingest JSONL crawler output → SQLite DB + Supabase.

Usage:
  python3 db/ingest.py                 # ingest all data/crawled/*.jsonl
  python3 db/ingest.py file1.jsonl ... # ingest specific files
  python3 db/ingest.py --supabase-only # skip SQLite, Supabase only
"""
import json, os, sys
from pathlib import Path
import sqlite3

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "signal.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"
CRAWLED_DIR = ROOT / "data" / "crawled"


# ── SQLite ──────────────────────────────────────────────────────

def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
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


# ── Supabase ────────────────────────────────────────────────────

def _post_to_article(p: dict) -> dict:
    """크롤러 post dict → Supabase articles 테이블 row로 변환."""
    meta = p.get("metadata") or {}
    source = p.get("source", "unknown")
    source_id = str(p.get("source_id", ""))

    title = meta.get("title", "")
    if not title:
        # content 첫 줄을 title로 사용
        content = p.get("content", "")
        title = content.split("\n")[0][:200] if content else source_id

    return {
        "id": f"{source}-{source_id}",
        "source": source,
        "title": title,
        "url": meta.get("external_url") or p.get("source_url", ""),
        "score": meta.get("points", 0) or meta.get("score", 0) or meta.get("baseScore", 0),
        "comments": meta.get("num_comments", 0) or meta.get("numComments", 0),
        "summary": "",  # LLM 파이프라인에서 나중에 채움
        "body": p.get("content", ""),
        "tags": meta.get("tags", []) if isinstance(meta.get("tags"), list) else [],
        "raw_json": json.dumps(p, ensure_ascii=False),
        "crawled_at": p.get("fetched_at") or p.get("timestamp"),
    }


def ingest_to_supabase(paths: list[Path]) -> int:
    """JSONL 파일들을 Supabase articles 테이블에 적재. 적재 건수 반환."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("[supabase] SUPABASE_URL/KEY 미설정 — 건너뜀", file=sys.stderr)
        return 0

    try:
        from supabase import create_client
        client = create_client(url, key)
    except Exception as e:
        print(f"[supabase] 연결 실패: {e}", file=sys.stderr)
        return 0

    total = 0
    for path in paths:
        if not path.exists():
            continue
        rows = []
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    p = json.loads(line)
                    rows.append(_post_to_article(p))
                except (json.JSONDecodeError, KeyError):
                    continue

        if not rows:
            continue

        try:
            result = client.table("articles").upsert(rows, on_conflict="id").execute()
            count = len(result.data) if result.data else 0
            total += count
            print(f"  [supabase] {path.name}: +{count} rows")
        except Exception as e:
            print(f"  [supabase] {path.name} 실패: {e}", file=sys.stderr)

    print(f"[supabase] 총 {total}건 적재 완료")
    return total


# ── Main ────────────────────────────────────────────────────────

def main():
    supabase_only = "--supabase-only" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    paths = [Path(p) for p in args] if args else sorted(CRAWLED_DIR.glob("*.jsonl"))

    if not paths:
        print(f"No JSONL files found in {CRAWLED_DIR}", file=sys.stderr)
        sys.exit(1)

    # SQLite 적재
    if not supabase_only:
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

        print("\nBy source:")
        for src, cnt in conn.execute("SELECT source, COUNT(*) FROM posts GROUP BY source ORDER BY 2 DESC"):
            print(f"  {src:15s} {cnt}")
        conn.close()

    # Supabase 적재
    ingest_to_supabase(paths)

if __name__ == "__main__":
    main()
