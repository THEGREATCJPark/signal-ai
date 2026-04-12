#!/usr/bin/env python3
"""
Reprocess: Extract topics programmatically from chunk summaries,
then use Gemma 4 only for Korean article body writing.
"""
import json, os, re, sys, time, threading, html as html_mod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import requests

MODEL = "gemma-4-31b-it"
ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
MAX_CONCURRENT = 5
KEY_MIN_GAP_S = 3.0
MIN_LEN = 200

class KeyScheduler:
    def __init__(self, keys):
        self.keys = list(keys)
        self.last_used = {k: 0.0 for k in keys}
        self.lock = threading.Lock()
        self._idx = 0
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
    if p.exists():
        for line in p.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                return [k.strip() for k in line.split("=",1)[1].split(",") if k.strip()]
    return []

def call_gemma(prompt, sched):
    endpoint = ENDPOINT.format(model=MODEL)
    body = {"contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.5, "maxOutputTokens": 8192}}
    for attempt in range(15):
        key = sched.acquire()
        try:
            r = requests.post(f"{endpoint}?key={key}", json=body, timeout=180)
        except: time.sleep(30); continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(60 - (time.time() % 60) + 0.5); continue
        if not r.ok: time.sleep(10); continue
        try: return r.json()["candidates"][0]["content"]["parts"][0]["text"]
        except: time.sleep(3); continue
    raise RuntimeError("API failed")

def call_validated(prompt, sched):
    for i in range(8):
        raw = call_gemma(prompt, sched)
        if len(raw.strip()) >= MIN_LEN: return raw.strip()
    raise RuntimeError("No substantial response")

# ── Topic extraction (programmatic, no LLM) ──

TOPICS = [
    {
        "headline": "Claude Mythos 후폭풍: AI 커뮤니티를 뒤흔든 자율 해킹 모델 논쟁",
        "keywords": ["mythos", "glasswing", "zero-day", "hacking", "swe-bench",
                      "cybersecurity", "autonomous", "exploit", "anthropic"],
    },
    {
        "headline": "Claude Opus vs GPT-5.5 Spud: 코딩 AI 왕좌의 게임",
        "keywords": ["spud", "gpt-5.5", "gpt-5.4", "opus 4.6", "opus", "claude",
                      "coding", "sonnet", "benchmark", "deepseek"],
    },
    {
        "headline": "GPT Image 2 실전 테스트: 이미지 생성 AI의 새로운 기준?",
        "keywords": ["image 2", "image generation", "nano banana", "nbp", "nb2",
                      "gpt image", "image 1.5", "grok image", "watermark"],
    },
    {
        "headline": "Gemini vs Claude vs GPT: 삼파전 벤치마크 비교 분석",
        "keywords": ["gemini", "benchmark", "comparison", "google", "gemini 3",
                      "flash", "pro", "eval"],
    },
    {
        "headline": "Meta Muse Spark·Qwen·Grok: 도전자들의 반격",
        "keywords": ["muse spark", "meta", "qwen", "grok", "llama", "open source",
                      "open-source", "challenger"],
    },
    {
        "headline": "AI 에이전트 시대: MCP, 코딩 도구, 자동화의 최전선",
        "keywords": ["agent", "mcp", "cursor", "windsurf", "tool", "automation",
                      "coding tool", "dispatch", "computer use"],
    },
    {
        "headline": "OpenAI 내부 소식: 모델 은퇴와 차세대 로드맵",
        "keywords": ["openai", "retiring", "codex", "roadmap", "o3", "o4",
                      "sora", "gpt-6"],
    },
]

def match_topic(summary_lower, topic):
    return sum(1 for kw in topic["keywords"] if kw.lower() in summary_lower)

def collect_topic_evidence(chunks, topic):
    """Collect relevant text snippets for a topic from all chunks."""
    evidence = []
    for cr in chunks:
        s = cr["summary"]
        score = match_topic(s.lower(), topic)
        if score >= 1:
            # Extract relevant lines
            for line in s.split("\n"):
                line_lower = line.lower().strip()
                if any(kw.lower() in line_lower for kw in topic["keywords"]):
                    clean = re.sub(r"^\s*[\*\-]\s*", "", line).strip()
                    if len(clean) > 20:
                        evidence.append(clean[:200])
    return evidence[:15]  # Cap at 15 snippets

# ── HTML ──
def esc(s): return html_mod.escape(s or "")
def render_md(s):
    if not s: return ""
    t = esc(s)
    t = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", t)
    t = re.sub(r"(?<!\*)\*([^\*\n]+?)\*(?!\*)", r"<em>\1</em>", t)
    t = re.sub(r"`(.+?)`", r"<code>\1</code>", t)
    t = re.sub(r"^(\s*)[\*\-]\s+(.+)$", r"\1<li>\2</li>", t, flags=re.MULTILINE)
    t = re.sub(r"^(\s*)\d+\.\s+(.+)$", r"\1<li>\2</li>", t, flags=re.MULTILINE)
    t = re.sub(r"((?:<li>.*?</li>\n?)+)", r"<ul>\1</ul>", t)
    t = t.replace("\n\n", "</p><p>")
    t = t.replace("\n", "<br/>")
    return t

def generate_html(topics_data, meta):
    cards = []
    for i, td in enumerate(topics_data):
        cards.append(f"""
      <div class="news-card" onclick="this.classList.toggle('open')">
        <div class="card-header">
          <span class="card-num">{i+1:02d}</span>
          <h2 class="card-headline">{esc(td['headline'])}</h2>
          <svg class="chevron" viewBox="0 0 24 24"><path d="M6 9l6 6 6-6"/></svg>
        </div>
        <div class="card-body">
          <div class="article">{render_md(td['article'])}</div>
        </div>
      </div>""")

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Signal Daily</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700;900&family=Inter:wght@400;500;600;700&display=swap');
:root {{
  --bg:#f8f9fa;--surface:#fff;--text:#1a1a2e;--text-2:#4a4a68;
  --text-3:#8888a0;--accent:#1d4ed8;--accent-bg:#eff6ff;
  --border:#e5e7eb;--shadow:0 1px 3px rgba(0,0,0,.08);--radius:10px;
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Noto Sans KR','Inter',sans-serif;background:var(--bg);color:var(--text);-webkit-font-smoothing:antialiased}}
.wrap{{max-width:720px;margin:0 auto;padding:20px}}
.masthead{{text-align:center;padding:40px 0 32px;border-bottom:2px solid var(--text)}}
.masthead h1{{font-size:2.2rem;font-weight:900;letter-spacing:-.04em;margin-bottom:4px}}
.masthead .sub{{font-size:.8rem;color:var(--text-3);margin-top:8px}}
.masthead .period{{display:inline-block;font-size:.75rem;font-weight:600;color:var(--accent);background:var(--accent-bg);padding:4px 12px;border-radius:20px;margin-top:10px}}
.news-card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:10px;box-shadow:var(--shadow);overflow:hidden;cursor:pointer;transition:box-shadow .2s}}
.news-card:hover{{box-shadow:0 4px 12px rgba(0,0,0,.1)}}
.card-header{{display:flex;align-items:center;gap:12px;padding:16px 20px}}
.card-num{{font-family:'Inter';font-size:.7rem;font-weight:700;color:var(--accent);background:var(--accent-bg);padding:3px 8px;border-radius:4px;flex-shrink:0}}
.card-headline{{font-size:1rem;font-weight:600;line-height:1.4;flex:1;color:var(--text)}}
.chevron{{width:20px;height:20px;flex-shrink:0;fill:none;stroke:var(--text-3);stroke-width:2;stroke-linecap:round;stroke-linejoin:round;transition:transform .2s}}
.news-card.open .chevron{{transform:rotate(180deg)}}
.card-body{{max-height:0;overflow:hidden;transition:max-height .4s ease}}
.news-card.open .card-body{{max-height:3000px}}
.article{{padding:0 20px 20px;color:var(--text-2);font-size:.92rem;line-height:1.8;border-top:1px solid var(--border);padding-top:16px;margin:0 20px}}
.article p{{margin-bottom:10px}}
.article strong{{color:var(--text)}}
.article code{{background:#f1f3f5;padding:1px 4px;border-radius:3px;font-size:.85em}}
.article ul{{margin:8px 0;padding-left:18px;list-style:none}}
.article li{{position:relative;padding-left:14px;margin-bottom:4px}}
.article li::before{{content:'';position:absolute;left:0;top:10px;width:4px;height:4px;border-radius:50%;background:var(--accent)}}
.footer{{text-align:center;padding:24px 0;font-size:.7rem;color:var(--text-3);border-top:1px solid var(--border);margin-top:32px}}
@media(max-width:600px){{.wrap{{padding:12px}}.masthead h1{{font-size:1.6rem}}.card-headline{{font-size:.9rem}}}}
</style>
</head>
<body>
<div class="wrap">
  <div class="masthead">
    <h1>SIGNAL DAILY</h1>
    <div class="sub">AI Community Digest</div>
    <div class="period">{esc(meta.get('period',''))}</div>
  </div>
  <div style="margin-top:24px">{"".join(cards)}</div>
  <div class="footer">Signal Daily &mdash; Gemma 4 &middot; {esc(meta.get('generated',''))}</div>
</div>
</body></html>"""

# ── Main ──
def main():
    keys = load_keys()
    if not keys: print("No keys"); sys.exit(1)
    sched = KeyScheduler(keys)
    print(f"{len(keys)} keys")

    state = json.loads(Path("digest_state.json").read_text())
    chunks = state["chunk_results"]
    print(f"{len(chunks)} chunks")

    # Match topics to chunks
    topics_data = []
    for topic in TOPICS:
        evidence = collect_topic_evidence(chunks, topic)
        if not evidence:
            print(f"  Skip: {topic['headline']} (no evidence)")
            continue
        topics_data.append({
            "headline": topic["headline"],
            "evidence": evidence,
        })
        print(f"  {topic['headline']}: {len(evidence)} evidence snippets")

    # Generate articles in parallel
    print(f"\nGenerating {len(topics_data)} articles...")
    def gen_article(td, idx):
        ev_text = "\n".join(f"- {e}" for e in td["evidence"])
        prompt = f""""{td['headline']}"

위 헤드라인에 대한 한국어 뉴스 기사를 써주세요. 유저이름 언급 금지. 300~500자. 아래 사실만 사용.

{ev_text}"""
        print(f"  [{idx+1}] {td['headline'][:30]}...", flush=True)
        article = call_validated(prompt, sched)
        print(f"  [{idx+1}] done ({len(article)} chars)", flush=True)
        return article

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futs = {pool.submit(gen_article, td, i): i for i, td in enumerate(topics_data)}
        for f in as_completed(futs):
            idx = futs[f]
            try: topics_data[idx]["article"] = f.result()
            except Exception as e: print(f"  [{idx+1}] FAILED: {e}"); topics_data[idx]["article"] = ""

    # Generate HTML
    fl = chunks[0]["time_label"] if chunks else ""
    ll = chunks[-1]["time_label"] if chunks else ""
    meta = {
        "period": fl.split("~")[0].strip() + " ~ " + ll.split("~")[-1].strip() if fl else "",
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M KST"),
    }
    html = generate_html(topics_data, meta)
    out = Path("docs/index.html")
    out.parent.mkdir(exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"\nOutput: {out} ({len(html)} bytes)")

    # Save
    Path("headline_state.json").write_text(json.dumps(
        [{"headline": t["headline"], "article": t.get("article","")} for t in topics_data],
        ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
