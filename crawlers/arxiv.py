#!/usr/bin/env python3
"""arXiv — RSS feeds for cs.AI, cs.CL, cs.LG."""
import sys, xml.etree.ElementTree as ET, requests, re
from datetime import datetime, timezone
from _common import post, save

CATEGORIES = ["cs.AI", "cs.CL", "cs.LG"]

def fetch():
    posts = []
    for cat in CATEGORIES:
        url = f"https://rss.arxiv.org/rss/{cat}"
        try:
            r = requests.get(url, timeout=30)
            if not r.ok:
                print(f"[arxiv/{cat}] {r.status_code}", file=sys.stderr); continue
            root = ET.fromstring(r.content)
            for item in root.iter("item"):
                title = (item.findtext("title") or "").strip()
                desc = (item.findtext("description") or "").strip()
                link = (item.findtext("link") or "").strip()
                pub = item.findtext("pubDate") or ""
                # Extract arxiv id from link (https://arxiv.org/abs/2510.12345)
                m = re.search(r"abs/(\S+)$", link)
                arxiv_id = m.group(1) if m else link
                try:
                    ts = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %z")
                except ValueError:
                    ts = datetime.now(timezone.utc)
                # Strip HTML from description
                desc = re.sub(r"<[^>]+>", "", desc).strip()
                posts.append(post(
                    source="arxiv",
                    source_id=arxiv_id,
                    source_url=link,
                    author=None,  # arxiv authors not in RSS
                    content=f"{title}\n\n{desc}",
                    timestamp=ts,
                    metadata={"category": cat, "title": title},
                ))
        except Exception as e:
            print(f"[arxiv/{cat}] error: {e}", file=sys.stderr)
    return posts

if __name__ == "__main__":
    ps = fetch()
    save("arxiv", ps)
