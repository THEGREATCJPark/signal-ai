#!/usr/bin/env python3
"""LessWrong — AI alignment posts via GraphQL."""
import sys, requests
from datetime import datetime, timezone
from _common import post, save

GQL_URL = "https://www.lesswrong.com/graphql"
QUERY = """
query RecentPosts($limit: Int) {
  posts(input: {terms: {view: "new", limit: $limit}}) {
    results {
      _id
      title
      slug
      htmlBody
      contents { plaintextMainText }
      postedAt
      user { displayName }
      baseScore
      commentCount
      tags { name }
    }
  }
}
"""

def fetch(limit=30):
    try:
        # Force identity encoding — brotli causes issues in some Python envs
        r = requests.post(GQL_URL, json={"query": QUERY, "variables": {"limit": limit}},
                          headers={"Content-Type": "application/json",
                                   "Accept-Encoding": "gzip, deflate"}, timeout=30)
        r.raise_for_status()
        data = r.json()
        results = data.get("data", {}).get("posts", {}).get("results", [])
    except Exception as e:
        print(f"[lesswrong] GraphQL failed: {e}, falling back to RSS", file=sys.stderr)
        return fetch_rss()

    posts = []
    for p in results:
        body = (p.get("contents") or {}).get("plaintextMainText") or ""
        tags = [t["name"] for t in (p.get("tags") or [])]
        # AI-relevant filter
        text = (p.get("title","") + " " + " ".join(tags)).lower()
        if not any(k in text for k in ["ai","llm","gpt","claude","alignment","agi","rlhf","model"]):
            continue
        try: ts = datetime.fromisoformat(p["postedAt"].replace("Z", "+00:00"))
        except: ts = datetime.now(timezone.utc)
        posts.append(post(
            source="lesswrong",
            source_id=p["_id"],
            source_url=f"https://www.lesswrong.com/posts/{p['_id']}/{p.get('slug','')}",
            author=(p.get("user") or {}).get("displayName"),
            content=f"{p.get('title','')}\n\n{body[:5000]}",
            timestamp=ts,
            metadata={
                "title": p.get("title"),
                "score": p.get("baseScore", 0),
                "num_comments": p.get("commentCount", 0),
                "tags": tags,
            },
        ))
    return posts

def fetch_rss():
    import xml.etree.ElementTree as ET, re
    url = "https://www.lesswrong.com/feed.xml?view=magic"
    r = requests.get(url, headers={"Accept-Encoding": "gzip, deflate"}, timeout=30)
    if not r.ok: return []
    root = ET.fromstring(r.content)
    posts = []
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        desc = re.sub(r"<[^>]+>", "", item.findtext("description") or "").strip()
        link = (item.findtext("link") or "").strip()
        guid = (item.findtext("guid") or link).strip()
        pub = item.findtext("pubDate") or ""
        try: ts = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %z")
        except: ts = datetime.now(timezone.utc)
        posts.append(post(
            source="lesswrong",
            source_id=guid,
            source_url=link,
            content=f"{title}\n\n{desc}",
            timestamp=ts,
            metadata={"title": title},
        ))
    return posts

if __name__ == "__main__":
    ps = fetch()
    save("lesswrong", ps)
