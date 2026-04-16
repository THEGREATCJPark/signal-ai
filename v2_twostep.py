#!/usr/bin/env python3
"""
V2 (Two-step): Step1: extract 5 topics, Step2: per-topic article (parallel).
All chunk summaries provided as context in both steps.
"""
import json, os, re, sys, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import requests

MODEL = "gemma-4-31b-it"
ENDPOINT_TPL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
KEY_MIN_GAP_S = 3.0
MAX_CONCURRENT = 5

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
    return []

def call_gemma(prompt, sched, max_tok=8192):
    endpoint = ENDPOINT_TPL.format(model=MODEL)
    body = {"contents":[{"parts":[{"text":prompt}]}],
            "generationConfig":{"temperature":0.5,"maxOutputTokens":max_tok}}
    for attempt in range(15):
        key = sched.acquire()
        try:
            r = requests.post(f"{endpoint}?key={key}", json=body, timeout=240)
        except: time.sleep(30); continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(60 - (time.time() % 60) + 0.5); continue
        if not r.ok: time.sleep(10); continue
        try: return r.json()["candidates"][0]["content"]["parts"][0]["text"]
        except: time.sleep(3); continue
    raise RuntimeError("failed")

def extract_korean(text):
    lines = []
    for line in text.split("\n"):
        kr = len(re.findall(r'[\uac00-\ud7af]', line))
        total = len(line.strip())
        if total > 10 and kr / max(total,1) > 0.15:
            clean = re.sub(r'^\s*[\*\-#]\s*', '', line).strip()
            lines.append(clean)
    return "\n".join(lines)

def parse_titles(text):
    """Extract Korean titles from various Gemma output formats."""
    titles = []
    patterns = [
        # *Draft N (xxx):* 제목...  or  *Draft N:* 제목...
        r'\*Draft\s*\d+[^:\n]*[:：]\*\s*(.+)',
        # 1. 제목  (Korean)
        r'^\s*\d+[\.\)]\s*(.+[가-힣].+)$',
    ]
    for pat in patterns:
        if titles: break
        for m in re.finditer(pat, text, re.MULTILINE):
            t = m.group(1).strip()
            # Cut trailing English commentary in parens (Reflects..., Good, etc.)
            t = re.sub(r'\s*\([A-Z][^)]{10,}\)\s*\.?\s*$', '', t)
            # Remove decorators
            t = t.strip('"\'*#[]:. ')
            if 10 <= len(t) <= 200:
                kr = len(re.findall(r'[\uac00-\ud7af]', t))
                if kr / max(len(t),1) > 0.2:
                    titles.append(t)
    # Dedupe
    seen = set(); uniq = []
    for t in titles:
        key = t[:30]
        if key not in seen:
            seen.add(key); uniq.append(t)
    return uniq

def parse_body(text):
    """Extract body content after '본문:' marker or just Korean content."""
    m = re.search(r'본문\s*[:：]\s*([\s\S]+)', text)
    if m: return extract_korean(m.group(1))
    return extract_korean(text)

def main():
    keys = load_keys()
    sched = KeyScheduler(keys)
    print(f"V2 Two-step — {len(keys)} keys")

    state = json.loads(Path("digest_state.json").read_text())
    chunks = state["chunk_results"]
    print(f"{len(chunks)} chunks loaded")

    context_parts = []
    for i, cr in enumerate(chunks):
        context_parts.append(f"[청크 {i+1} · {cr['time_label']}]\n{cr['summary'][:2000]}")
    context = "\n\n".join(context_parts)
    print(f"Context: {len(context)} chars ≈ {len(context)//3} tokens")

    # ── Step 1: Extract topic titles ──
    print("\n[Step 1] Extracting 5 topic titles...")
    t0 = time.time()
    title_prompt = f"""다음은 디스코드 AI 커뮤니티 서버의 시간대별 대화 분석입니다. 이 내용을 바탕으로 작성할 만한 AI 뉴스 기사의 제목 5개를 한국어로 뽑아주세요.

규칙:
- 유저이름 언급 금지
- 실제 대화에 나온 내용만 반영
- 1줄씩, 번호를 매겨주세요

--- 분석 데이터 ---

{context}

--- 끝 ---

이제 제목 5개:

1."""
    t_raw = call_gemma(title_prompt, sched, max_tok=1024)
    t_full = "1." + t_raw
    t_elapsed = time.time() - t0
    Path("v2_step1_raw.txt").write_text(t_full)

    titles = parse_titles(t_full)
    print(f"  Got {len(titles)} titles in {t_elapsed:.1f}s")
    for i, t in enumerate(titles): print(f"    {i+1}. {t}")

    if not titles:
        print("ERROR: No titles parsed"); sys.exit(1)

    titles = titles[:5]

    # ── Step 2: Per-title article (parallel) ──
    print(f"\n[Step 2] Writing {len(titles)} articles in parallel...")

    def write_article(title, idx):
        prompt = f""""{title}"

위 제목으로 한국어 AI 뉴스 기사를 작성해주세요. 300~500자. 유저이름 언급 금지. 실제 대화 내용만 기반.

--- 참고 데이터 ---

{context}

--- 끝 ---

본문:"""
        print(f"  [{idx+1}] {title[:40]}...", flush=True)
        t1 = time.time()
        raw = call_gemma(prompt, sched, max_tok=2048)
        e = time.time() - t1
        body = parse_body(raw)
        print(f"  [{idx+1}] done ({len(body)} chars, {e:.1f}s)", flush=True)
        return {"headline": title, "body": body, "raw": raw, "elapsed": e}

    s2_t0 = time.time()
    articles = [None] * len(titles)
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futs = {pool.submit(write_article, t, i): i for i, t in enumerate(titles)}
        for f in as_completed(futs):
            idx = futs[f]
            try: articles[idx] = f.result()
            except Exception as e: print(f"  [{idx+1}] FAILED: {e}"); articles[idx] = None

    s2_elapsed = time.time() - s2_t0
    articles = [a for a in articles if a]

    # Structured output
    fl = chunks[0]["time_label"] if chunks else ""
    ll = chunks[-1]["time_label"] if chunks else ""
    output = {
        "version": "v2_twostep",
        "source": "discord",
        "guild": "Dev Mode",
        "channel": "general",
        "period": {"start": fl.split("~")[0].strip(), "end": ll.split("~")[-1].strip()},
        "generated_at": datetime.now().isoformat() + "+09:00",
        "model": MODEL,
        "stats": {
            "chunks_input": len(chunks),
            "context_chars": len(context),
            "step1_seconds": round(t_elapsed, 1),
            "step2_seconds": round(s2_elapsed, 1),
            "total_seconds": round(t_elapsed + s2_elapsed, 1),
            "api_calls": 1 + len(titles),
        },
        "articles": [
            {
                "id": f"v2-{i+1:03d}",
                "headline": a["headline"],
                "body": a["body"],
                "body_length": len(a["body"]),
            }
            for i, a in enumerate(articles)
        ],
    }
    Path("docs/articles_v2.json").write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\nSaved: docs/articles_v2.json ({len(output['articles'])} articles)")
    print(f"Stats: {output['stats']['api_calls']} API calls, {output['stats']['total_seconds']}s total")

if __name__ == "__main__":
    main()
