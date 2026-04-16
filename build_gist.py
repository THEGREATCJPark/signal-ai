#!/usr/bin/env python3
"""docs/index.html의 __ARTICLES_JSON__ 자리에 docs/articles.json 내용을 넣어
gist용 단일 파일 /tmp/signal_gist.html을 생성하고 gist에 푸시한다."""
import json, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).parent
GIST_ID = "a9a6b3f417be5221efd2969fe8da85ed"

tmpl = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
data = json.loads((ROOT / "docs" / "articles.json").read_text(encoding="utf-8"))
inline = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
out = tmpl.replace("__ARTICLES_JSON__", inline)

out_path = Path("/tmp/signal_gist.html")
out_path.write_text(out, encoding="utf-8")

# gist의 파일명은 로컬 파일명을 따르므로 index.html이라는 이름으로 복사
final = out_path.parent / "index.html"
final.write_text(out, encoding="utf-8")

r = subprocess.run(
    ["gh", "gist", "edit", GIST_ID, str(final)],
    capture_output=True, text=True
)
if r.returncode != 0:
    print(f"gist edit failed:\n{r.stderr}", file=sys.stderr)
    sys.exit(1)
print(f"pushed {len(out):,} bytes to gist {GIST_ID}")
print(f"URL: https://gist.githack.com/pineapplesour/{GIST_ID}/raw/index.html")
