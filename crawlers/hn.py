#!/usr/bin/env python3
"""Hacker News — Algolia API. No auth, unlimited."""
import sys, requests
from datetime import datetime, timezone
from _common import post, save

AI_KEYWORDS = ["GPT", "Claude", "Gemini", "LLM", "AI", "openai", "anthropic",
               "deepmind", "llama", "mistral", "transformer", "diffusion",
               "huggingface", "ollama", "stable diffusion", "sora"]

def fetch(hours=24, hits=100):
    # AI-related stories from last N hours
    ts = int(datetime.now(timezone.utc).timestamp()) - hours * 3600
    # Broad query — get AI + discuss tag
    url = "https://hn.algolia.com/api/v1/search_by_date"
    params = {
        "tags": "story",
        "numericFilters": f"created_at_i>{ts}",
        "hitsPerPage": hits,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    posts = []
    for h in data.get("hits", []):
        title = h.get("title") or ""
        text = h.get("story_text") or ""
        url_field = h.get("url")
        # Filter AI-relevant
        full = f"{title} {text}".lower()
        if not any(k.lower() in full for k in AI_KEYWORDS):
            continue
        posts.append(post(
            source="hackernews",
            source_id=h["objectID"],
            source_url=f"https://news.ycombinator.com/item?id={h['objectID']}",
            author=h.get("author"),
            content=f"{title}\n\n{text}".strip(),
            timestamp=datetime.fromtimestamp(h["created_at_i"], tz=timezone.utc),
            metadata={
                "title": title,
                "external_url": url_field,
                "points": h.get("points") or 0,
                "num_comments": h.get("num_comments") or 0,
            },
        ))
    return posts

if __name__ == "__main__":
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 24
    ps = fetch(hours=hours)
    save("hackernews", ps)
