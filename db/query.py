#!/usr/bin/env python3
"""Simple query helpers for the signal DB.

Examples:
  python3 db/query.py stats                       # counts per source
  python3 db/query.py recent 20                   # 20 most recent posts
  python3 db/query.py search "claude mythos" 10   # FTS search top 10
  python3 db/query.py since 7d                    # posts from last 7 days
  python3 db/query.py top reddit 5                # top 5 scored reddit posts
"""
import json, sys
from pathlib import Path
import sqlite3
from datetime import datetime, timedelta, timezone

DB_PATH = Path(__file__).parent.parent / "data" / "signal.db"

def conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def cmd_stats(c):
    total = c.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    print(f"Total: {total}")
    print("\nBy source:")
    for r in c.execute("SELECT source, COUNT(*) n, MIN(timestamp) oldest, MAX(timestamp) newest FROM posts GROUP BY source ORDER BY n DESC"):
        print(f"  {r['source']:15s} {r['n']:4d}  [{r['oldest'][:10]} ~ {r['newest'][:10]}]")

def cmd_recent(c, n=20):
    rows = c.execute("""
      SELECT source, author, timestamp, substr(content, 1, 120) preview, source_url
      FROM posts ORDER BY timestamp DESC LIMIT ?
    """, (int(n),)).fetchall()
    for r in rows:
        print(f"[{r['timestamp'][:16]}] [{r['source']}] {r['author'] or '?'}")
        print(f"  {r['preview']}...")
        if r['source_url']: print(f"  {r['source_url']}")
        print()

def cmd_search(c, query, n=10):
    rows = c.execute("""
      SELECT p.source, p.author, p.timestamp, substr(p.content, 1, 150) preview, p.source_url,
             rank
      FROM posts_fts f JOIN posts p ON p.id = f.rowid
      WHERE posts_fts MATCH ?
      ORDER BY rank
      LIMIT ?
    """, (query, int(n))).fetchall()
    print(f"FTS results for '{query}': {len(rows)}\n")
    for r in rows:
        print(f"[{r['timestamp'][:16]}] [{r['source']}] {r['author'] or '?'}")
        print(f"  {r['preview']}...")
        print()

def cmd_since(c, spec):
    # spec: 1d, 7d, 24h
    unit = spec[-1]; n = int(spec[:-1])
    delta = timedelta(days=n) if unit == 'd' else timedelta(hours=n)
    cutoff = (datetime.now(timezone.utc) - delta).isoformat()
    rows = c.execute("""
      SELECT source, COUNT(*) n FROM posts
      WHERE timestamp > ? GROUP BY source ORDER BY n DESC
    """, (cutoff,)).fetchall()
    print(f"Posts since {cutoff[:16]}:")
    for r in rows: print(f"  {r['source']:15s} {r['n']}")

def cmd_top(c, source, n=5):
    """Show posts sorted by score field (varies per source)."""
    rows = c.execute("""
      SELECT source, author, timestamp, substr(content, 1, 150) preview,
             source_url, metadata
      FROM posts WHERE source = ?
      ORDER BY timestamp DESC LIMIT ?
    """, (source, int(n) * 5)).fetchall()
    # Extract score from metadata per source
    scored = []
    for r in rows:
        meta = json.loads(r['metadata'])
        score = meta.get('score') or meta.get('points') or meta.get('likes') or meta.get('upvotes') or 0
        scored.append((score, r, meta))
    scored.sort(reverse=True, key=lambda x: x[0])
    for score, r, meta in scored[:int(n)]:
        print(f"[score={score}] [{r['timestamp'][:16]}] [{r['source']}] {r['author'] or '?'}")
        print(f"  {r['preview']}...")
        if r['source_url']: print(f"  {r['source_url']}")
        print()

CMDS = {
    "stats": lambda c, *a: cmd_stats(c),
    "recent": lambda c, *a: cmd_recent(c, a[0] if a else 20),
    "search": lambda c, *a: cmd_search(c, a[0], a[1] if len(a) > 1 else 10),
    "since": lambda c, *a: cmd_since(c, a[0] if a else "1d"),
    "top": lambda c, *a: cmd_top(c, a[0], a[1] if len(a) > 1 else 5),
}

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in CMDS:
        print(__doc__); sys.exit(1)
    c = conn()
    CMDS[sys.argv[1]](c, *sys.argv[2:])

if __name__ == "__main__":
    main()
