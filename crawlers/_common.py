"""Common schema + file IO for all crawlers."""
import json, os, sys
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data" / "crawled"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def post(source, source_id, content, *, source_url=None, author=None,
         timestamp=None, parent_id=None, metadata=None):
    """Build a normalized post dict."""
    return {
        "source": source,
        "source_id": str(source_id),
        "source_url": source_url,
        "author": author,
        "content": content,
        "timestamp": timestamp.isoformat() if hasattr(timestamp, "isoformat") else timestamp,
        "parent_id": parent_id,
        "metadata": metadata or {},
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

def save(source, posts):
    """Save to data/crawled/<source>-<date>.jsonl"""
    today = datetime.now().strftime("%Y-%m-%d")
    out = DATA_DIR / f"{source}-{today}.jsonl"
    with out.open("w", encoding="utf-8") as f:
        for p in posts:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")
    print(f"[{source}] saved {len(posts)} posts → {out}", file=sys.stderr)
    return out
