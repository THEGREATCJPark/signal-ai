#!/usr/bin/env python3
"""
V1 (One-shot): Feed all chunk summaries → single Gemma call → parse 제목:/본문: format
"""
import json, os, re, sys, time, threading, html as html_mod
from datetime import datetime
from pathlib import Path
import requests

MODEL = "gemma-4-31b-it"
ENDPOINT_TPL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
KEY_MIN_GAP_S = 3.0

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

def call_gemma(prompt, sched, max_tok=16384):
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

def parse_articles(text):
    """Parse 제목:/본문: loose format"""
    # Try multiple patterns
    articles = []
    # Pattern: 제목: ... 본문: ...
    pattern = re.compile(r'제목\s*[:：]\s*(.+?)\s*\n+\s*본문\s*[:：]\s*(.+?)(?=\n\s*제목\s*[:：]|\Z)', re.DOTALL)
    for m in pattern.finditer(text):
        title = m.group(1).strip().strip('"\'*#')
        body = m.group(2).strip().strip('"\'*')
        if len(title) > 5 and len(body) > 50:
            articles.append({"headline": title, "body": body})
    return articles

def extract_korean(text):
    """Extract Korean-dominant lines."""
    lines = []
    for line in text.split("\n"):
        kr = len(re.findall(r'[\uac00-\ud7af]', line))
        total = len(line.strip())
        if total > 10 and kr / max(total,1) > 0.15:
            clean = re.sub(r'^\s*[\*\-#]\s*', '', line).strip()
            lines.append(clean)
    return "\n".join(lines)

def main():
    keys = load_keys()
    sched = KeyScheduler(keys)
    print(f"V1 One-shot — {len(keys)} keys")

    state = json.loads(Path("digest_state.json").read_text())
    chunks = state["chunk_results"]
    print(f"{len(chunks)} chunks loaded")

    # Build context: all chunk summaries
    context_parts = []
    for i, cr in enumerate(chunks):
        context_parts.append(f"[청크 {i+1} · {cr['time_label']}]\n{cr['summary'][:2000]}")
    context = "\n\n".join(context_parts)

    total_tokens = len(context) // 3
    print(f"Context: {len(context)} chars ≈ {total_tokens} tokens")

    prompt = f"""다음은 디스코드 AI 커뮤니티 서버의 시간대별 대화 분석입니다. 이 내용을 바탕으로 AI 뉴스 기사 5개를 작성해주세요.

규칙:
- 유저이름 언급 금지
- 한국어로 작성
- 각 기사는 300~500자
- 실제 대화에 나온 사실만 사용 (지어내지 마세요)
- 반드시 아래 형식으로 작성:

제목: (여기에 기사 제목)
본문: (여기에 기사 본문)

제목: (다음 기사 제목)
본문: (다음 기사 본문)

(총 5개)

--- 분석 데이터 ---

{context}

--- 끝 ---

이제 기사 5개를 작성해주세요:

제목:"""

    print(f"\nCalling Gemma (single call)...")
    t0 = time.time()
    raw = call_gemma(prompt, sched, max_tok=8192)
    elapsed = time.time() - t0
    print(f"Response: {len(raw)} chars in {elapsed:.1f}s")

    # Prepend "제목:" since we ended prompt with it
    raw_full = "제목:" + raw

    # Save raw for debugging
    Path("v1_raw.txt").write_text(raw_full)

    articles = parse_articles(raw_full)
    print(f"Parsed {len(articles)} articles")

    # Clean Korean from each
    for a in articles:
        a["body"] = extract_korean(a["body"])
        a["headline"] = extract_korean(a["headline"]).replace("\n", " ")[:100]

    # Structured output
    fl = chunks[0]["time_label"] if chunks else ""
    ll = chunks[-1]["time_label"] if chunks else ""
    output = {
        "version": "v1_oneshot",
        "source": "discord",
        "guild": "Dev Mode",
        "channel": "general",
        "period": {"start": fl.split("~")[0].strip(), "end": ll.split("~")[-1].strip()},
        "generated_at": datetime.now().isoformat() + "+09:00",
        "model": MODEL,
        "stats": {
            "chunks_input": len(chunks),
            "context_chars": len(context),
            "response_chars": len(raw_full),
            "elapsed_seconds": round(elapsed, 1),
            "api_calls": 1,
        },
        "articles": [
            {"id": f"v1-{i+1:03d}", **a, "body_length": len(a["body"])}
            for i, a in enumerate(articles)
        ],
    }
    Path("docs/articles_v1.json").write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\nSaved: docs/articles_v1.json ({len(output['articles'])} articles)")
    print(f"Stats: 1 API call, {elapsed:.1f}s")

if __name__ == "__main__":
    main()
