#!/usr/bin/env python3
"""docs/{index,archive}.html의 __ARTICLES_JSON__ 자리에 docs/articles.json을 인라인으로
주입하고 gist에 푸시한다."""
import json, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).parent
GIST_ID = "a9a6b3f417be5221efd2969fe8da85ed"

data = json.loads((ROOT / "docs" / "articles.json").read_text(encoding="utf-8"))
inline = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")

tmp = Path("/tmp")
urls = []
for fn in ("index.html", "archive.html"):
    tmpl = (ROOT / "docs" / fn).read_text(encoding="utf-8")
    out = tmpl.replace("__ARTICLES_JSON__", inline)
    (tmp / fn).write_text(out, encoding="utf-8")
    r = subprocess.run(
        ["gh", "gist", "edit", GIST_ID, "-a", str(tmp / fn)],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        # -a adds a new file; if it already exists, try without -a (update)
        r2 = subprocess.run(
            ["gh", "gist", "edit", GIST_ID, str(tmp / fn)],
            capture_output=True, text=True,
        )
        if r2.returncode != 0:
            print(f"gist edit failed for {fn}:\n{r.stderr}\n{r2.stderr}", file=sys.stderr)
            sys.exit(1)
    urls.append(f"https://cdn.statically.io/gist/pineapplesour/{GIST_ID}/raw/{fn}")
    print(f"pushed {fn} ({len(out):,} bytes)")

# Fetch latest commit SHA for cache-busted URLs (statically.io caches immutably per-SHA)
r = subprocess.run(
    ["gh", "api", f"gists/{GIST_ID}", "--jq", ".history[0].version"],
    capture_output=True, text=True,
)
sha = r.stdout.strip() if r.returncode == 0 else None

print("\nURLs (htmlpreview — gist 최신 raw 자동 렌더):")
for fn in ("index.html", "archive.html"):
    print(f"  https://htmlpreview.github.io/?https://gist.githubusercontent.com/pineapplesour/{GIST_ID}/raw/{fn}")
if sha:
    print("\n(백업) statically cache-busted:")
    for fn in ("index.html", "archive.html"):
        print(f"  https://cdn.statically.io/gist/pineapplesour/{GIST_ID}/raw/{sha}/{fn}")
