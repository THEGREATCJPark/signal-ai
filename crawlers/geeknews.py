#!/usr/bin/env python3
"""GeekNews (news.hada.io) — Korean tech news Atom feed."""
import sys, xml.etree.ElementTree as ET, requests, re
from datetime import datetime, timezone
from _common import post, save

ATOM = "{http://www.w3.org/2005/Atom}"

def fetch():
    url = "https://news.hada.io/rss/news"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    posts = []
    for entry in root.iter(f"{ATOM}entry"):
        title = (entry.findtext(f"{ATOM}title") or "").strip()
        summary = (entry.findtext(f"{ATOM}summary") or entry.findtext(f"{ATOM}content") or "").strip()
        link_el = entry.find(f"{ATOM}link")
        link = link_el.get("href") if link_el is not None else ""
        gid = (entry.findtext(f"{ATOM}id") or link).strip()
        updated = entry.findtext(f"{ATOM}updated") or entry.findtext(f"{ATOM}published") or ""
        try: ts = datetime.fromisoformat(updated.replace("Z", "+00:00"))
        except: ts = datetime.now(timezone.utc)
        summary_clean = re.sub(r"<[^>]+>", "", summary).strip()
        # Filter AI-relevant items
        full = (title + " " + summary_clean).lower()
        if not any(k in full for k in ["ai", "llm", "gpt", "claude", "gemini", "모델", "인공지능", "ml", "agent"]):
            continue
        posts.append(post(
            source="geeknews",
            source_id=gid,
            source_url=link,
            author=None,
            content=f"{title}\n\n{summary_clean}",
            timestamp=ts,
            metadata={"title": title},
        ))
    return posts

if __name__ == "__main__":
    ps = fetch()
    save("geeknews", ps)
