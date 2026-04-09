#!/usr/bin/env python3
"""
Signal Discord Digest Pipeline
- Reads Discord text export
- Chunks into ~10k-token pieces
- Per-chunk: summary + evidence selection + time anchoring via Gemma 4
- Final chronological summary
- Outputs modern news-style static HTML report
"""
from __future__ import annotations

import argparse
import html as html_mod
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

# ── Config ──────────────────────────────────────────────────────

DEFAULT_MODEL = "gemma-4-31b-it"
ENDPOINT_TPL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
CHARS_PER_TOKEN = 3.5
MAX_CONCURRENT = 5
KEY_MIN_GAP_S = 3.0
MIN_CONTENT_LEN = 200

ENV_KEY_FILES = [
    Path.home() / ".config" / "legal_evidence_rag" / "keys.env",
]

# ── Key management ──────────────────────────────────────────────

class KeyScheduler:
    def __init__(self, keys: list[str]):
        self.keys = list(keys)
        self.last_used: dict[str, float] = {k: 0.0 for k in keys}
        self.lock = threading.Lock()
        self._idx = 0

    def acquire(self) -> str:
        with self.lock:
            now = time.time()
            best_key = None
            best_wait = float("inf")
            for i in range(len(self.keys)):
                idx = (self._idx + i) % len(self.keys)
                k = self.keys[idx]
                wait = max(0, self.last_used[k] + KEY_MIN_GAP_S - now)
                if wait < best_wait:
                    best_wait = wait
                    best_key = k
                    if wait == 0:
                        break
            if best_wait > 0:
                time.sleep(best_wait)
            self.last_used[best_key] = time.time()
            self._idx = (self.keys.index(best_key) + 1) % len(self.keys)
            return best_key


def load_keys() -> list[str]:
    for var in ("GEMINI_API_KEYS", "GOOGLE_API_KEYS"):
        val = os.environ.get(var, "")
        if val:
            return [k.strip() for k in val.split(",") if k.strip()]
    for var in ("GEMINI_API_KEY", "GOOGLE_API_KEY"):
        val = os.environ.get(var, "")
        if val.strip():
            return [val.strip()]
    for path in ENV_KEY_FILES:
        if path.exists():
            for line in path.read_text().splitlines():
                line = line.strip()
                if line.startswith("#") or "=" not in line:
                    continue
                _, v = line.split("=", 1)
                keys = [k.strip() for k in v.split(",") if k.strip()]
                if keys:
                    return keys
    return []

# ── API call ────────────────────────────────────────────────────

def call_gemma(prompt, model, scheduler, temperature=0.5, max_tokens=8192):
    endpoint = ENDPOINT_TPL.format(model=model)
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
    }
    for attempt in range(20):
        key = scheduler.acquire()
        try:
            resp = requests.post(f"{endpoint}?key={key}", json=body,
                                 headers={"Content-Type": "application/json"}, timeout=180)
        except requests.RequestException as e:
            print(f"    Network error (attempt {attempt+1}): {e}", flush=True)
            _backoff(None); continue

        if resp.status_code == 429 or resp.status_code >= 500:
            print(f"    API {resp.status_code} (key ...{key[-6:]}), backing off", flush=True)
            _backoff(resp); continue

        if not resp.ok:
            print(f"    API error {resp.status_code}: {resp.text[:200]}", flush=True)
            _backoff(resp); continue

        data = resp.json()
        try:
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            if text: return text
        except (KeyError, IndexError):
            pass
        print(f"    Empty response, retrying...", flush=True)
        time.sleep(3)
    raise RuntimeError("Failed after 20 attempts")


def _backoff(resp):
    if resp is not None:
        ra = resp.headers.get("Retry-After")
        if ra:
            try: time.sleep(float(ra)); return
            except ValueError: pass
    now = time.time()
    time.sleep(60 - (now % 60) + (hash(str(now)) % 500) / 1000)


def call_gemma_validated(prompt, model, scheduler):
    for attempt in range(10):
        raw = call_gemma(prompt, model, scheduler)
        if len(raw.strip()) >= MIN_CONTENT_LEN:
            return raw.strip()
        print(f"    Response too short ({len(raw)} chars), retrying ({attempt+1}/10)...", flush=True)
    raise RuntimeError("Failed to get substantial response after 10 retries")

# ── Tokenizer / chunking ───────────────────────────────────────

def estimate_tokens(text):
    return int(len(text) / CHARS_PER_TOKEN)

def chunk_text(text, token_limit):
    msg_re = re.compile(r"^\[\d{4}\. \d{1,2}\. \d{1,2}\. (?:오전|오후) \d{1,2}:\d{2}\]")
    lines = text.split("\n")
    messages, current = [], []
    for line in lines:
        if msg_re.match(line) and current:
            messages.append("\n".join(current))
            current = [line]
        else:
            current.append(line)
    if current: messages.append("\n".join(current))

    chunks, buf, buf_tokens = [], [], 0
    for msg in messages:
        t = estimate_tokens(msg)
        if buf_tokens + t > token_limit and buf:
            chunks.append("\n\n".join(buf))
            buf, buf_tokens = [], 0
        buf.append(msg)
        buf_tokens += t
    if buf: chunks.append("\n\n".join(buf))
    return chunks

def extract_time_range(chunk):
    ts_re = re.compile(r"\[(\d{4})\. (\d{1,2})\. (\d{1,2})\. (오전|오후) (\d{1,2}):(\d{2})\]")
    first = last = None
    for m in ts_re.finditer(chunk):
        y, mo, d, period, h, mi = m.groups()
        hour = int(h)
        if period == "오후" and hour != 12: hour += 12
        if period == "오전" and hour == 12: hour = 0
        dt = datetime(int(y), int(mo), int(d), hour, int(mi))
        if first is None or dt < first: first = dt
        if last is None or dt > last: last = dt
    fmt = lambda d: d.strftime("%Y-%m-%d %H:%M") if d else "?"
    return (first.isoformat() if first else None,
            last.isoformat() if last else None,
            f"{fmt(first)} ~ {fmt(last)}")

# ── Prompts ─────────────────────────────────────────────────────

def build_chunk_prompt(chunk, idx, total, time_label):
    return f"""다음 디스코드 대화({time_label})를 한국어로 분석해주세요.

누가 무슨 이야기를 했는지 구체적으로 요약하세요. 유저이름과 기술적 세부사항을 반드시 포함하세요.
가장 중요한 발언 3~5개를 원문 인용하고 왜 중요한지 설명하세요.
주요 사건을 시간순으로 나열하세요.

{chunk}"""

def build_final_prompt(chunk_results):
    sections = [f"[{cr['time_label']}] {cr['summary'][:600]}" for cr in chunk_results]
    body = "\n---\n".join(sections)
    return f"""다음은 디스코드 서버 3일간(2026-04-06~09) 대화의 시간대별 요약입니다. 이를 종합해서 한국어로 뉴스레터를 작성해주세요.

헤드라인: 3일간 가장 핵심적인 내용을 한 문장으로 작성하고, 3~5문장으로 전체 개요를 써주세요.
주요 스토리: 3~5개 주요 토픽을 각각 소제목 + 3~5문장으로 정리해주세요.
타임라인: 3일간 주요 사건을 시간순으로 나열해주세요.
인상적인 발언: 대화 중 가장 중요했거나 재미있었던 발언 5개를 골라주세요.
활발한 참여자: 누가 가장 활발했고 어떤 기여를 했는지 정리해주세요.

요약 데이터:
{body}"""

# ── HTML rendering ──────────────────────────────────────────────

def esc(s):
    return html_mod.escape(s or "")

def render_md(s):
    if not s: return ""
    t = esc(s)
    t = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", t)
    t = re.sub(r"(?<!\*)\*([^\*\n]+?)\*(?!\*)", r"<em>\1</em>", t)
    t = re.sub(r"`(.+?)`", r"<code>\1</code>", t)
    t = re.sub(r"^##\s*(.+)$", r"<h4>\1</h4>", t, flags=re.MULTILINE)
    t = re.sub(r"^#\s*(.+)$", r"<h3>\1</h3>", t, flags=re.MULTILINE)
    t = re.sub(r"^(\s*)[\*\-]\s+(.+)$", r"\1<li>\2</li>", t, flags=re.MULTILINE)
    t = re.sub(r"^(\s*)\d+\.\s+(.+)$", r"\1<li>\2</li>", t, flags=re.MULTILINE)
    t = re.sub(r"((?:<li>.*?</li>\n?)+)", r"<ul>\1</ul>", t)
    t = re.sub(r"^&gt;\s*(.+)$", r"<blockquote>\1</blockquote>", t, flags=re.MULTILINE)
    t = t.replace("\n\n", "</p><p>")
    return f"<div class='rendered-content'>{t}</div>"

def generate_html(chunk_results, final_text, meta):
    cards = []
    for i, cr in enumerate(chunk_results):
        cards.append(f"""
        <details class="chunk-detail">
          <summary><span class="chunk-time">{esc(cr['time_label'])}</span> Chunk {i+1}</summary>
          <div class="chunk-body">{render_md(cr['summary'])}</div>
        </details>""")

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Signal Daily - Discord Digest</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=Inter:wght@300;400;500;600;700&display=swap');
:root {{
  --bg:#fafafa;--surface:#fff;--text-primary:#1a1a2e;--text-secondary:#555570;
  --text-muted:#8888a0;--accent:#2563eb;--accent-light:#eff6ff;
  --border:#e5e7eb;--border-light:#f3f4f6;--tag-bg:#e0e7ff;--tag-text:#3730a3;
  --shadow:0 1px 3px rgba(0,0,0,.06),0 1px 2px rgba(0,0,0,.04);--radius:12px;
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Noto Sans KR','Inter',-apple-system,sans-serif;background:var(--bg);color:var(--text-primary);line-height:1.7;-webkit-font-smoothing:antialiased}}
.container{{max-width:760px;margin:0 auto;padding:0 20px}}
.site-header{{border-bottom:1px solid var(--border);padding:24px 0;margin-bottom:32px}}
.site-header .container{{display:flex;justify-content:space-between;align-items:center}}
.site-name{{font-size:1.1rem;font-weight:700;letter-spacing:-.02em;color:var(--accent)}}
.site-meta{{font-size:.8rem;color:var(--text-muted)}}
.hero{{margin-bottom:40px}}
.hero h1{{font-size:2rem;font-weight:700;line-height:1.3;letter-spacing:-.03em;margin-bottom:16px}}
.period-badge{{display:inline-block;font-size:.75rem;font-weight:500;color:var(--tag-text);background:var(--tag-bg);padding:4px 10px;border-radius:20px;margin-bottom:12px}}
.section{{margin-bottom:36px}}
.section-label{{font-size:.7rem;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:var(--accent);margin-bottom:12px}}
.story-card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px 24px;margin-bottom:16px;box-shadow:var(--shadow)}}
.chunk-detail{{border:1px solid var(--border-light);border-radius:8px;margin-bottom:8px;overflow:hidden}}
.chunk-detail summary{{padding:10px 16px;cursor:pointer;font-size:.85rem;color:var(--text-muted);background:var(--bg);user-select:none}}
.chunk-detail summary:hover{{background:var(--border-light)}}
.chunk-time{{font-family:'Inter',monospace;font-size:.8rem;color:var(--accent);margin-right:8px}}
.chunk-body{{padding:16px;font-size:.9rem;color:var(--text-secondary)}}
blockquote{{border-left:2px solid var(--border);padding-left:12px;margin:8px 0;color:var(--text-muted);font-style:italic}}
code{{background:var(--border-light);padding:2px 5px;border-radius:3px;font-size:.85em}}
h3{{font-size:1rem;font-weight:600;margin:12px 0 6px}}
h4{{font-size:.9rem;font-weight:600;margin:8px 0 4px}}
ul{{margin:8px 0;padding-left:20px;list-style:none}}
li{{position:relative;padding-left:14px;margin-bottom:6px;color:var(--text-secondary);font-size:.95rem}}
li::before{{content:'';position:absolute;left:0;top:10px;width:5px;height:5px;border-radius:50%;background:var(--accent)}}
strong{{color:var(--text-primary)}} em{{color:var(--text-secondary)}}
.rendered-content{{line-height:1.8}}
.site-footer{{border-top:1px solid var(--border);padding:20px 0;margin-top:40px;text-align:center;font-size:.75rem;color:var(--text-muted)}}
@media(max-width:600px){{.hero h1{{font-size:1.5rem}}.container{{padding:0 16px}}}}
</style>
</head>
<body>
<header class="site-header"><div class="container">
  <div class="site-name">Signal Daily</div>
  <div class="site-meta">{esc(meta.get('guild',''))} &middot; {esc(meta.get('generated',''))}</div>
</div></header>
<main class="container">
  <div class="hero">
    <span class="period-badge">{esc(meta.get('period',''))}</span>
    <h1>Discord 3-Day Digest</h1>
  </div>
  <div class="section">
    <div class="section-label">Overview</div>
    <div class="story-card">{render_md(final_text)}</div>
  </div>
  <div class="section">
    <div class="section-label">Detailed Analysis ({len(chunk_results)} chunks)</div>
    {"".join(cards)}
  </div>
</main>
<footer class="site-footer"><div class="container">
  Signal Daily &mdash; Auto-generated Discord digest powered by Gemma 4
</div></footer>
</body></html>"""

# ── Main ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Discord Digest Pipeline")
    parser.add_argument("input", help="Discord text export file")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--chunk-tokens", type=int, default=10000)
    parser.add_argument("--output", default="docs/index.html")
    parser.add_argument("--state", default="digest_state.json")
    args = parser.parse_args()

    keys = load_keys()
    if not keys: print("ERROR: No API keys found.", file=sys.stderr); sys.exit(1)
    print(f"Loaded {len(keys)} API keys")
    scheduler = KeyScheduler(keys)

    state = {"chunks": [], "chunk_results": [], "final_text": ""}
    state_path = Path(args.state)
    if state_path.exists():
        state = json.loads(state_path.read_text())
        print(f"Resumed: {len(state.get('chunk_results',[]))} chunks done")

    text = Path(args.input).read_text(encoding="utf-8")
    print(f"Input: {len(text)} chars, ~{estimate_tokens(text)} tokens")

    guild = ""
    for line in text.split("\n")[:10]:
        if line.startswith("Guild: "): guild = line[7:].strip()

    if not state["chunks"]:
        state["chunks"] = chunk_text(text, args.chunk_tokens)
        print(f"Created {len(state['chunks'])} chunks")
    else:
        print(f"Using {len(state['chunks'])} chunks from state")

    for i, c in enumerate(state["chunks"]):
        _, _, label = extract_time_range(c)
        print(f"  Chunk {i+1}: ~{estimate_tokens(c)} tokens, {label}")

    existing = {cr["chunk_index"] for cr in state.get("chunk_results", [])}
    todo = [i for i in range(len(state["chunks"])) if i not in existing]

    if todo:
        print(f"\nPhase 1: Analyzing {len(todo)} chunks...")
        def process_chunk(idx):
            chunk = state["chunks"][idx]
            first_iso, _, time_label = extract_time_range(chunk)
            prompt = build_chunk_prompt(chunk, idx, len(state["chunks"]), time_label)
            print(f"  Chunk {idx+1}: sending...", flush=True)
            summary = call_gemma_validated(prompt, args.model, scheduler)
            print(f"  Chunk {idx+1}: done ({len(summary)} chars)", flush=True)
            return {"chunk_index": idx, "time_label": time_label, "first_time": first_iso, "summary": summary}

        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
            futures = {pool.submit(process_chunk, i): i for i in todo}
            for future in as_completed(futures):
                try:
                    state["chunk_results"].append(future.result())
                    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2))
                except Exception as e:
                    print(f"  Chunk {futures[future]+1} FAILED: {e}", flush=True)

    state["chunk_results"].sort(key=lambda x: x.get("first_time") or f"z{x['chunk_index']}")
    print(f"\nPhase 1 complete: {len(state['chunk_results'])} chunks")

    if not state.get("final_text"):
        print("\nPhase 2: Final digest...")
        final_prompt = build_final_prompt(state["chunk_results"])
        print(f"  Prompt: ~{estimate_tokens(final_prompt)} tokens")
        state["final_text"] = call_gemma_validated(final_prompt, args.model, scheduler)
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2))
        print(f"  Done: {len(state['final_text'])} chars")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fl = state["chunk_results"][0]["time_label"] if state["chunk_results"] else ""
    ll = state["chunk_results"][-1]["time_label"] if state["chunk_results"] else ""
    meta = {
        "period": fl.split("~")[0].strip() + " ~ " + ll.split("~")[-1].strip() if fl else "",
        "guild": guild,
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M KST"),
    }
    html_content = generate_html(state["chunk_results"], state["final_text"], meta)
    out_path.write_text(html_content, encoding="utf-8")
    print(f"\nOutput: {out_path} ({len(html_content)} bytes)")

if __name__ == "__main__":
    main()
