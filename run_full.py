#!/usr/bin/env python3
"""
End-to-end automation:
1. Run all crawlers
2. Ingest to Supabase posts
3. Query last 3 days across all sources via Supabase RPC
4. Gemma 4 → headlines + articles (one-shot, 제목:/본문: format)
5. Save JSON + HTML
6. Update public gist

Run: python3 run_full.py
No manual intervention required. Errors = script exits non-zero.
"""
from __future__ import annotations
import json, os, re, sys, time, threading, html as html_mod, subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
from db.posts import list_recent_posts_by_source

ROOT = Path(__file__).parent
GIST_ID = "a9a6b3f417be5221efd2969fe8da85ed"
MODEL = "gemma-4-31b-it"
ENDPOINT_TPL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
KEY_MIN_GAP_S = 3.0

LOG = lambda msg: print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── Keys ────────────────────────────────────────────────────────

class KeyScheduler:
    def __init__(self, keys):
        self.keys = list(keys); self.last_used = {k: 0.0 for k in keys}
        self.lock = threading.Lock(); self._idx = 0
    def acquire(self):
        with self.lock:
            now = time.time()
            best, bw = None, float("inf")
            for i in range(len(self.keys)):
                k = self.keys[(self._idx + i) % len(self.keys)]
                w = max(0, self.last_used[k] + KEY_MIN_GAP_S - now)
                if w < bw: bw, best = w, k
                if w == 0: break
            if bw > 0: time.sleep(bw)
            self.last_used[best] = time.time()
            self._idx = (self.keys.index(best) + 1) % len(self.keys)
            return best

def load_keys():
    p = Path.home() / ".config" / "legal_evidence_rag" / "keys.env"
    for line in p.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            return [k.strip() for k in line.split("=",1)[1].split(",") if k.strip()]
    raise RuntimeError("No API keys found")

def call_gemma(prompt, sched, max_tok=8192, temp=0.5):
    endpoint = ENDPOINT_TPL.format(model=MODEL)
    body = {"contents":[{"parts":[{"text":prompt}]}],
            "generationConfig":{"temperature":temp,"maxOutputTokens":max_tok}}
    for attempt in range(15):
        key = sched.acquire()
        try:
            r = requests.post(f"{endpoint}?key={key}", json=body, timeout=240)
        except Exception as e:
            LOG(f"  net error: {e}"); time.sleep(30); continue
        if r.status_code == 429 or r.status_code >= 500:
            LOG(f"  {r.status_code}, backoff"); time.sleep(60 - (time.time()%60) + 0.5); continue
        if not r.ok:
            LOG(f"  error {r.status_code}: {r.text[:150]}"); time.sleep(10); continue
        try: return r.json()["candidates"][0]["content"]["parts"][0]["text"]
        except: time.sleep(3); continue
    raise RuntimeError("API failed after 15 attempts")

# ── Steps ──────────────────────────────────────────────────────

def step_crawl():
    LOG("[1/6] Running crawlers...")
    r = subprocess.run(["python3", str(ROOT / "crawlers" / "run_all.py")],
                       capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        LOG(f"  crawl output:\n{r.stdout}\n{r.stderr}")
        raise RuntimeError("Crawlers failed")
    LOG(r.stdout.strip().split("\n")[-1])

def step_ingest():
    LOG("[2/6] Ingesting to Supabase posts...")
    r = subprocess.run(["python3", str(ROOT / "db" / "ingest.py")],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f"Ingest failed: {r.stderr}")
    LOG("  " + r.stdout.strip().split("\n")[-1])

def step_query_context(days=3, per_source=15):
    LOG(f"[3/6] Querying last {days} days from Supabase...")
    rows = list_recent_posts_by_source(days=days, per_source=per_source)

    def score_of(row):
        meta = row.get("metadata") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        if not isinstance(meta, dict):
            meta = {}
        return (meta.get("points") or meta.get("score") or meta.get("upvotes")
                or meta.get("likes") or meta.get("num_comments") or row.get("score") or 0)

    context_entries = []
    source_counts = {}
    for row in rows:
        src = row.get("source") or "unknown"
        source_counts[src] = source_counts.get(src, 0) + 1
        preview = (row.get("content") or "")[:350].replace("\n", " ")
        context_entries.append(f"[{src}] ({score_of(row)}) {preview}")

    LOG(f"  {len(rows)} posts across {len(source_counts)} sources selected")
    return "\n\n".join(context_entries), source_counts

def step_generate(context, sched):
    LOG("[4/6] Generating headlines + articles (Gemma)...")
    prompt = f"""다음은 최근 3일간 여러 AI 커뮤니티/소스에서 수집한 인기 포스트들입니다.
소스: Hacker News, Reddit, arXiv, HuggingFace, GeekNews, LessWrong

이 데이터를 바탕으로 AI 뉴스 기사 5~7개를 한국어로 작성해주세요.

규칙:
- 유저이름/닉네임 언급 금지
- 실제 데이터에 나온 내용만 사용 (지어내지 말 것)
- 각 기사 300~600자
- 반드시 아래 형식으로 출력:

제목: (기사 제목)
본문: (기사 본문)

제목: (다음 기사 제목)
본문: (다음 기사 본문)

--- 원본 데이터 ---

{context}

--- 끝 ---

기사 시작:

제목:"""

    t0 = time.time()
    raw = call_gemma(prompt, sched, max_tok=8192)
    elapsed = time.time() - t0
    LOG(f"  Gemma response: {len(raw)} chars in {elapsed:.1f}s")

    full = "제목:" + raw
    (ROOT / "data" / "run_full_raw.txt").write_text(full, encoding="utf-8")

    # Parse 제목:/본문:
    articles = []
    pattern = re.compile(
        r'제목\s*[:：]\s*(.+?)\s*\n+\s*본문\s*[:：]\s*(.+?)(?=\n\s*제목\s*[:：]|\Z)',
        re.DOTALL
    )
    for m in pattern.finditer(full):
        title = m.group(1).strip().strip('"\'*#')
        body = m.group(2).strip().strip('"\'*')
        # Skip prompt-echo articles (English-heavy title or placeholder body)
        title_kr = len(re.findall(r'[\uac00-\ud7af]', title))
        if title_kr / max(len(title), 1) < 0.2:
            continue
        if body.lstrip().startswith(("(Body)", "(본문", "(Title)", "(제목")):
            continue
        if len(title) > 5 and len(body) > 80:
            # Clean Korean lines in body
            korean_lines = []
            for line in body.split("\n"):
                kr = len(re.findall(r'[\uac00-\ud7af]', line))
                total = len(line.strip())
                if total > 15 and kr / max(total, 1) > 0.15:
                    clean = re.sub(r'^\s*[\*\-#]\s*', '', line).strip()
                    low = clean.lower()
                    if any(x in low for x in ['constraint', 'check:', 'draft']):
                        continue
                    korean_lines.append(clean)
            body_clean = "\n".join(korean_lines) if korean_lines else body
            # Clean title
            title = title.split("\n")[0].strip()[:150]
            articles.append({"headline": title, "body": body_clean})

    LOG(f"  parsed {len(articles)} articles")
    if not articles:
        raise RuntimeError("No articles parsed — check data/run_full_raw.txt")
    return articles, elapsed

def step_save(articles, source_counts, gemma_elapsed):
    LOG("[5/6] Saving JSON + HTML...")
    now = datetime.now(timezone.utc)
    period_end = now.isoformat()
    period_start = (now - timedelta(days=3)).isoformat()

    data = {
        "version": "run_full",
        "sources": list(source_counts.keys()),
        "source_counts": source_counts,
        "period": {"start": period_start, "end": period_end, "days": 3},
        "generated_at": now.isoformat(),
        "model": MODEL,
        "stats": {
            "api_calls": 1,
            "gemma_seconds": round(gemma_elapsed, 1),
            "total_source_posts": sum(source_counts.values()),
        },
        "articles": [
            {"id": f"full-{i+1:03d}", **a, "body_length": len(a["body"])}
            for i, a in enumerate(articles)
        ],
    }
    (ROOT / "docs" / "articles_full.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # HTML
    html = render_html(articles, source_counts, now)
    (ROOT / "docs" / "index.html").write_text(html, encoding="utf-8")
    LOG(f"  articles_full.json ({len(articles)} articles)")
    LOG(f"  docs/index.html ({len(html)} bytes)")

def render_html(articles, source_counts, now):
    def esc(s): return html_mod.escape(s or "")
    cards = []
    for i, a in enumerate(articles):
        body_html = esc(a["body"]).replace("\n\n", "</p><p>").replace("\n", "<br/>")
        cards.append(f"""
        <div class="news-card" onclick="this.classList.toggle('open')">
          <div class="card-header">
            <span class="card-num">{i+1:02d}</span>
            <h2 class="card-headline">{esc(a['headline'])}</h2>
            <svg class="chevron" viewBox="0 0 24 24"><path d="M6 9l6 6 6-6"/></svg>
          </div>
          <div class="card-body">
            <div class="article"><p>{body_html}</p></div>
          </div>
        </div>""")

    src_badges = " · ".join(f"{s}({n})" for s, n in source_counts.items())
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Signal Daily</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700;900&family=Inter:wght@400;500;600;700&display=swap');
:root{{--bg:#f8f9fa;--surface:#fff;--text:#1a1a2e;--text-2:#4a4a68;--text-3:#8888a0;--accent:#1d4ed8;--accent-bg:#eff6ff;--border:#e5e7eb;--shadow:0 1px 3px rgba(0,0,0,.08);--radius:10px}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Noto Sans KR','Inter',sans-serif;background:var(--bg);color:var(--text);-webkit-font-smoothing:antialiased}}
.wrap{{max-width:720px;margin:0 auto;padding:20px}}
.masthead{{text-align:center;padding:40px 0 32px;border-bottom:2px solid var(--text)}}
.masthead h1{{font-size:2.2rem;font-weight:900;letter-spacing:-.04em;margin-bottom:4px}}
.masthead .sub{{font-size:.8rem;color:var(--text-3);margin-top:8px}}
.masthead .period{{display:inline-block;font-size:.75rem;font-weight:600;color:var(--accent);background:var(--accent-bg);padding:4px 12px;border-radius:20px;margin-top:10px}}
.masthead .sources{{font-size:.7rem;color:var(--text-3);margin-top:8px;font-family:'Inter',monospace}}
.news-card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:10px;box-shadow:var(--shadow);overflow:hidden;cursor:pointer;transition:box-shadow .2s}}
.news-card:hover{{box-shadow:0 4px 12px rgba(0,0,0,.1)}}
.card-header{{display:flex;align-items:center;gap:12px;padding:16px 20px}}
.card-num{{font-family:'Inter';font-size:.7rem;font-weight:700;color:var(--accent);background:var(--accent-bg);padding:3px 8px;border-radius:4px;flex-shrink:0}}
.card-headline{{font-size:1rem;font-weight:600;line-height:1.4;flex:1}}
.chevron{{width:20px;height:20px;flex-shrink:0;fill:none;stroke:var(--text-3);stroke-width:2;transition:transform .2s}}
.news-card.open .chevron{{transform:rotate(180deg)}}
.card-body{{max-height:0;overflow:hidden;transition:max-height .4s ease}}
.news-card.open .card-body{{max-height:3000px}}
.article{{padding:0 20px 20px;color:var(--text-2);font-size:.92rem;line-height:1.8;border-top:1px solid var(--border);padding-top:16px;margin:0 20px}}
.article p{{margin-bottom:10px}}
.footer{{text-align:center;padding:24px 0;font-size:.7rem;color:var(--text-3);border-top:1px solid var(--border);margin-top:32px}}
</style>
</head>
<body>
<div class="wrap">
  <div class="masthead">
    <h1>SIGNAL DAILY</h1>
    <div class="sub">AI Multi-Source Digest</div>
    <div class="period">최근 3일 · {now.strftime('%Y-%m-%d')}</div>
    <div class="sources">{esc(src_badges)}</div>
  </div>
  <div style="margin-top:24px">{''.join(cards)}</div>
  <div class="footer">Signal Daily · Gemma 4 · {now.strftime('%Y-%m-%d %H:%M UTC')}</div>
</div>
</body></html>"""

def step_publish():
    LOG("[6/6] Publishing to gist...")
    r = subprocess.run(
        ["gh", "gist", "edit", GIST_ID, str(ROOT / "docs" / "index.html")],
        capture_output=True, text=True, timeout=60
    )
    if r.returncode != 0:
        LOG(f"  gist edit failed: {r.stderr}")
        raise RuntimeError("Gist publish failed")
    url = f"https://gist.githack.com/pineapplesour/{GIST_ID}/raw/index.html"
    LOG(f"  published: {url}")
    return url

# ── Main ───────────────────────────────────────────────────────

def main():
    t0 = time.time()
    keys = load_keys()
    LOG(f"Loaded {len(keys)} API keys")
    sched = KeyScheduler(keys)

    step_crawl()
    step_ingest()
    context, source_counts = step_query_context(days=3, per_source=15)
    articles, gemma_elapsed = step_generate(context, sched)
    step_save(articles, source_counts, gemma_elapsed)
    url = step_publish()

    total = time.time() - t0
    LOG("")
    LOG(f"=== DONE in {total:.1f}s ===")
    LOG(f"URL: {url}")

if __name__ == "__main__":
    main()
