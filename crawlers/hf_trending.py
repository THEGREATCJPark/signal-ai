#!/usr/bin/env python3
"""HuggingFace — trending models + papers via public API."""
import sys, requests
from datetime import datetime, timezone
from _common import post, save

def fetch_models(limit=50):
    # sort=trending returns 400 — use downloads which effectively shows recent popular
    url = f"https://huggingface.co/api/models?sort=likes7d&direction=-1&limit={limit}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    posts = []
    for m in r.json():
        mid = m.get("modelId") or m.get("id")
        if not mid: continue
        try: ts = datetime.fromisoformat(m["lastModified"].replace("Z", "+00:00"))
        except: ts = datetime.now(timezone.utc)
        posts.append(post(
            source="huggingface",
            source_id=f"model:{mid}",
            source_url=f"https://huggingface.co/{mid}",
            author=mid.split("/")[0] if "/" in mid else None,
            content=f"Model: {mid}\nPipeline: {m.get('pipeline_tag','')}\nTags: {', '.join(m.get('tags',[]))}",
            timestamp=ts,
            metadata={
                "model_id": mid,
                "downloads": m.get("downloads", 0),
                "likes": m.get("likes", 0),
                "pipeline_tag": m.get("pipeline_tag"),
                "library_name": m.get("library_name"),
                "tags": m.get("tags", []),
            },
        ))
    return posts

def fetch_papers(limit=30):
    """Daily HF papers."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"https://huggingface.co/api/daily_papers?date={today}"
    try:
        r = requests.get(url, timeout=30)
        if not r.ok: return []
        posts = []
        for p in r.json()[:limit]:
            paper = p.get("paper", {})
            pid = paper.get("id")
            if not pid: continue
            ts_str = p.get("publishedAt") or paper.get("publishedAt") or today
            try: ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except: ts = datetime.now(timezone.utc)
            posts.append(post(
                source="huggingface",
                source_id=f"paper:{pid}",
                source_url=f"https://huggingface.co/papers/{pid}",
                author=None,
                content=f"{paper.get('title','')}\n\n{paper.get('summary','')}",
                timestamp=ts,
                metadata={
                    "paper_id": pid,
                    "title": paper.get("title"),
                    "upvotes": paper.get("upvotes", 0),
                    "num_comments": p.get("numComments", 0),
                },
            ))
        return posts
    except Exception as e:
        print(f"[hf/papers] error: {e}", file=sys.stderr)
        return []

if __name__ == "__main__":
    ps = fetch_models() + fetch_papers()
    save("huggingface", ps)
