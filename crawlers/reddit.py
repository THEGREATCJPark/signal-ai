#!/usr/bin/env python3
"""Reddit — .json endpoints (no auth required, rate-limited but generous)."""
import sys, time, requests
from datetime import datetime, timezone
from _common import post, save

SUBREDDITS = ["LocalLLaMA", "MachineLearning", "singularity", "OpenAI",
              "ArtificialIntelligence", "ClaudeAI"]

def fetch(limit=50):
    posts = []
    for sub in SUBREDDITS:
        url = f"https://www.reddit.com/r/{sub}/new.json?limit={limit}"
        try:
            r = requests.get(url, headers={"User-Agent": "signal-ai-bot/0.1"}, timeout=30)
            if r.status_code != 200:
                print(f"[reddit/{sub}] {r.status_code}", file=sys.stderr)
                continue
            data = r.json()
            for child in data.get("data", {}).get("children", []):
                d = child.get("data", {})
                title = d.get("title", "")
                body = d.get("selftext", "")
                posts.append(post(
                    source="reddit",
                    source_id=d["id"],
                    source_url=f"https://reddit.com{d.get('permalink','')}",
                    author=d.get("author"),
                    content=f"{title}\n\n{body}".strip(),
                    timestamp=datetime.fromtimestamp(d["created_utc"], tz=timezone.utc),
                    metadata={
                        "subreddit": sub,
                        "title": title,
                        "score": d.get("score", 0),
                        "upvote_ratio": d.get("upvote_ratio"),
                        "num_comments": d.get("num_comments", 0),
                        "flair": d.get("link_flair_text"),
                        "external_url": d.get("url") if not d.get("is_self") else None,
                    },
                ))
            time.sleep(2)  # Be polite
        except Exception as e:
            print(f"[reddit/{sub}] error: {e}", file=sys.stderr)
    return posts

if __name__ == "__main__":
    ps = fetch()
    save("reddit", ps)
