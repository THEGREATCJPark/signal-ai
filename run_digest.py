#!/usr/bin/env python3
"""
Signal Discord Digest Pipeline
- Reads Discord text export
- Chunks into ~50k-token pieces
- Per-chunk: summary + evidence selection + time anchoring via Gemma 4
- Final chronological summary
- Outputs static HTML report
"""
from __future__ import annotations

import argparse
import html
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

def call_gemma(
    prompt: str,
    model: str,
    scheduler: KeyScheduler,
    temperature: float = 0.3,
    max_tokens: int = 8192,
) -> str:
    endpoint = ENDPOINT_TPL.format(model=model)
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
    }
    for attempt in range(20):
        key = scheduler.acquire()
        try:
            resp = requests.post(
                f"{endpoint}?key={key}",
                json=body,
                headers={"Content-Type": "application/json"},
                timeout=120,
            )
        except requests.RequestException as e:
            print(f"    Network error (attempt {attempt+1}): {e}", flush=True)
            _backoff(resp=None)
            continue

        if resp.status_code == 429 or resp.status_code >= 500:
            print(f"    API {resp.status_code} (key ...{key[-6:]}), backing off", flush=True)
            _backoff(resp)
            continue

        if not resp.ok:
            print(f"    API error {resp.status_code}: {resp.text[:200]}", flush=True)
            _backoff(resp)
            continue

        data = resp.json()
        text = ""
        try:
            text = data["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError):
            print(f"    Empty response, retrying...", flush=True)
            time.sleep(3)
            continue

        if text:
            return text

    raise RuntimeError("Failed after 20 attempts")


def _backoff(resp=None):
    if resp is not None:
        ra = resp.headers.get("Retry-After")
        if ra:
            try:
                time.sleep(float(ra))
                return
            except ValueError:
                pass
    now = time.time()
    to_next_min = 60 - (now % 60)
    jitter = (hash(str(now)) % 500) / 1000
    time.sleep(to_next_min + jitter)

# ── Structured output parsing ───────────────────────────────────

def parse_envelope(text: str) -> dict[int, str]:
    m = re.search(r"<\s*data\s*>([\s\S]*?)<\s*/\s*data\s*>", text, re.IGNORECASE)
    if not m:
        raise ValueError("Missing <data> wrapper")
    inner = m.group(1)
    items = {}
    for im in re.finditer(r"<\s*id:(\d+)\s*>([\s\S]*?)<\s*/\s*id:\1\s*>", inner, re.IGNORECASE):
        items[int(im.group(1))] = im.group(2).strip()
    if not items:
        raise ValueError("No <id:n> items")
    return items


def call_gemma_structured(prompt: str, model: str, scheduler: KeyScheduler) -> dict[int, str]:
    for _ in range(10):
        raw = call_gemma(prompt, model, scheduler)
        try:
            return parse_envelope(raw)
        except ValueError as e:
            print(f"    Structured parse failed: {e}, retrying...", flush=True)
    raise RuntimeError("Structured output failed after 10 retries")

# ── Tokenizer / chunking ───────────────────────────────────────

def estimate_tokens(text: str) -> int:
    return int(len(text) / CHARS_PER_TOKEN)


def chunk_text(text: str, token_limit: int) -> list[str]:
    msg_re = re.compile(r"^\[\d{4}\. \d{1,2}\. \d{1,2}\. (?:오전|오후) \d{1,2}:\d{2}\]")
    lines = text.split("\n")
    messages: list[str] = []
    current: list[str] = []

    for line in lines:
        if msg_re.match(line) and current:
            messages.append("\n".join(current))
            current = [line]
        else:
            current.append(line)
    if current:
        messages.append("\n".join(current))

    chunks: list[str] = []
    buf: list[str] = []
    buf_tokens = 0

    for msg in messages:
        t = estimate_tokens(msg)
        if buf_tokens + t > token_limit and buf:
            chunks.append("\n\n".join(buf))
            buf = []
            buf_tokens = 0
        buf.append(msg)
        buf_tokens += t

    if buf:
        chunks.append("\n\n".join(buf))
    return chunks


def extract_time_range(chunk: str) -> tuple[str | None, str | None, str]:
    ts_re = re.compile(r"\[(\d{4})\. (\d{1,2})\. (\d{1,2})\. (오전|오후) (\d{1,2}):(\d{2})\]")
    first = last = None
    for m in ts_re.finditer(chunk):
        y, mo, d, period, h, mi = m.groups()
        hour = int(h)
        if period == "오후" and hour != 12:
            hour += 12
        if period == "오전" and hour == 12:
            hour = 0
        dt = datetime(int(y), int(mo), int(d), hour, int(mi))
        if first is None or dt < first:
            first = dt
        if last is None or dt > last:
            last = dt
    fmt = lambda d: d.strftime("%Y-%m-%d %H:%M") if d else "?"
    return (
        first.isoformat() if first else None,
        last.isoformat() if last else None,
        f"{fmt(first)} ~ {fmt(last)}",
    )

# ── Prompts ─────────────────────────────────────────────────────

def build_chunk_prompt(chunk: str, idx: int, total: int, time_label: str) -> str:
    return f"""당신은 디스코드 서버 대화 분석 전문가입니다.

아래는 디스코드 서버 대화 기록의 {total}개 청크 중 {idx + 1}번째입니다.
시간 범위: {time_label}

다음 작업을 수행하세요:

1. **요약**: 이 구간에서 논의된 주요 주제, 핵심 결론, 중요 사건을 구체적으로 요약하세요.
2. **핵심 근거**: 가장 중요한 발언 3~7개를 원문 인용과 함께 선택하고, 왜 중요한지 한 줄씩 설명하세요.
3. **시간 앵커**: 주요 사건/전환점의 정확한 시간을 기록하세요.

반드시 아래 형식으로 출력하세요:

<data>
<id:1>요약 내용</id:1>
<id:2>핵심 근거 목록 (각 근거를 번호와 원문 인용으로)</id:2>
<id:3>시간 앵커 목록 (시각: 사건 형태로)</id:3>
</data>

--- 대화 기록 시작 ---
{chunk}
--- 대화 기록 끝 ---"""


def build_final_prompt(chunk_results: list[dict]) -> str:
    sections = []
    for i, cr in enumerate(chunk_results):
        sections.append(f"""## 청크 {i+1} ({cr['time_label']})

### 요약
{cr['summary']}

### 핵심 근거
{cr['evidence']}

### 시간 앵커
{cr['time_anchors']}""")

    body = "\n\n---\n\n".join(sections)
    return f"""당신은 디스코드 서버 활동 보고서 작성 전문가입니다.

아래는 디스코드 서버 3일치 대화를 시간순으로 청크별 분석한 결과입니다.
모든 청크의 분석 결과를 종합하여 최종 보고서를 작성하세요.

요구사항:
1. **전체 타임라인**: 3일간 시간 흐름에 따른 주요 사건/논의를 시간순으로 정리
2. **주제별 정리**: 핵심 주제를 분류하고 각 주제의 논의 내용과 결론을 정리
3. **핵심 인사이트**: 서버에서 가장 중요했던 결정, 발견, 합의사항
4. **주요 참여자 활동**: 활발한 참여자와 그들의 주요 기여

반드시 아래 형식으로 출력하세요:

<data>
<id:1>전체 타임라인 (시간순 주요 사건)</id:1>
<id:2>주제별 정리</id:2>
<id:3>핵심 인사이트</id:3>
<id:4>주요 참여자 활동</id:4>
</data>

--- 청크별 분석 결과 ---
{body}
--- 끝 ---"""

# ── HTML generation ─────────────────────────────────────────────

def generate_html(chunk_results: list[dict], final: dict, meta: dict) -> str:
    def esc(s):
        return html.escape(s or "")

    def md(s):
        if not s:
            return ""
        t = esc(s)
        t = t.replace("\n", "<br/>")
        t = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", t)
        t = re.sub(r"`(.+?)`", r"<code>\1</code>", t)
        return t

    chunk_cards = []
    for i, cr in enumerate(chunk_results):
        chunk_cards.append(f"""
        <div class="chunk-card">
          <div class="time-anchor">{esc(cr['time_label'])}</div>
          <div class="summary"><strong>Chunk {i+1} Summary</strong><br/>{md(cr['summary'])}</div>
          <div class="evidence"><strong>Key Evidence</strong><br/>{md(cr['evidence'])}</div>
          <div class="anchors"><strong>Time Anchors</strong><br/>{md(cr['time_anchors'])}</div>
        </div>""")

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Signal - Discord 3-Day Digest</title>
<style>
:root {{
  --bg: #4a5942; --secondary-bg: #3e4637;
  --accent: #c4b550; --secondary-accent: #958831;
  --text: #dedfd6; --secondary-text: #d8ded3; --text-3: #a0aa95;
  --border-light: #8c9284; --border-dark: #292c21;
}}
*,*::before,*::after {{ box-sizing: border-box; }}
* {{ margin: 0; padding: 0; }}
@font-face {{
  font-family: ArialPixel;
  src: url("https://cdn.jsdelivr.net/gh/ekmas/cs16.css@main/ArialPixel.ttf") format("truetype");
}}
body {{
  font-family: ArialPixel, system-ui, sans-serif;
  background: var(--bg); color: var(--text);
  padding: 20px; line-height: 1.6;
}}
h1 {{ color: var(--accent); text-align: center; font-size: 1.8em; margin-bottom: 4px; font-weight: 400; }}
.subtitle {{ text-align: center; color: var(--text-3); margin-bottom: 20px; font-size: 0.9em; }}
h2 {{ color: var(--accent); font-size: 1.3em; margin: 16px 0 8px; font-weight: 400; }}
h3 {{ color: var(--secondary-accent); font-size: 1.05em; margin: 10px 0 4px; font-weight: 400; }}
.final-summary {{
  padding: 16px; margin-bottom: 20px;
  border: 2px solid var(--accent); background: rgba(0,0,0,0.2);
}}
.chunk-card {{
  margin-bottom: 14px; padding: 10px;
  border: 1px solid var(--border-light); background: rgba(0,0,0,0.15);
}}
.chunk-card .time-anchor {{ color: var(--accent); font-size: 0.9em; margin-bottom: 6px; }}
.chunk-card .summary {{ margin-bottom: 8px; }}
.chunk-card .evidence {{ font-size: 0.9em; color: var(--text-3); margin-bottom: 6px; }}
.chunk-card .anchors {{ font-size: 0.85em; color: var(--text-3); }}
strong {{ color: var(--secondary-text); }}
code {{ background: var(--secondary-bg); padding: 1px 4px; }}
.container {{ max-width: 900px; margin: 0 auto; }}
</style>
</head>
<body>
<div class="container">
<h1>Signal - Discord 3-Day Digest</h1>
<div class="subtitle">{esc(meta.get('period',''))} | {esc(meta.get('guild',''))} | Generated {esc(meta.get('generated',''))}</div>

<div class="final-summary">
<h2>Final Digest</h2>
<h3>Timeline</h3>
<div>{md(final.get('timeline',''))}</div>
<h3>Topics</h3>
<div>{md(final.get('topics',''))}</div>
<h3>Insights</h3>
<div>{md(final.get('insights',''))}</div>
<h3>Contributors</h3>
<div>{md(final.get('contributors',''))}</div>
</div>

<h2>Chunk Details ({len(chunk_results)} chunks)</h2>
{"".join(chunk_cards)}
</div>
</body>
</html>"""

# ── Main pipeline ───────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Discord Digest Pipeline")
    parser.add_argument("input", help="Discord text export file")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--chunk-tokens", type=int, default=50000)
    parser.add_argument("--output", default="docs/index.html")
    parser.add_argument("--state", default="digest_state.json", help="State file for resume")
    args = parser.parse_args()

    keys = load_keys()
    if not keys:
        print("ERROR: No API keys found. Set GEMINI_API_KEYS or check keys.env", file=sys.stderr)
        sys.exit(1)
    print(f"Loaded {len(keys)} API keys")

    scheduler = KeyScheduler(keys)

    # Load or resume state
    state: dict[str, Any] = {"chunks": [], "chunk_results": [], "final": None}
    state_path = Path(args.state)
    if state_path.exists():
        state = json.loads(state_path.read_text())
        print(f"Resumed state: {len(state.get('chunk_results',[]))} chunks already done")

    # Read input
    text = Path(args.input).read_text(encoding="utf-8")
    print(f"Input: {len(text)} chars, ~{estimate_tokens(text)} tokens")

    # Extract metadata from header
    guild = ""
    for line in text.split("\n")[:10]:
        if line.startswith("Guild: "):
            guild = line[7:].strip()

    # Chunk
    if not state["chunks"]:
        state["chunks"] = chunk_text(text, args.chunk_tokens)
        print(f"Created {len(state['chunks'])} chunks")
    else:
        print(f"Using {len(state['chunks'])} chunks from saved state")

    for i, c in enumerate(state["chunks"]):
        _, _, label = extract_time_range(c)
        print(f"  Chunk {i+1}: ~{estimate_tokens(c)} tokens, {label}")

    # Phase 1: Per-chunk analysis (parallel)
    existing = {cr["chunk_index"] for cr in state.get("chunk_results", [])}
    todo = [i for i in range(len(state["chunks"])) if i not in existing]

    if todo:
        print(f"\nPhase 1: Analyzing {len(todo)} chunks (5 concurrent, {len(keys)} keys)...")

        def process_chunk(idx):
            chunk = state["chunks"][idx]
            first_iso, _, time_label = extract_time_range(chunk)
            prompt = build_chunk_prompt(chunk, idx, len(state["chunks"]), time_label)
            print(f"  Chunk {idx+1}: sending to {args.model}...", flush=True)
            items = call_gemma_structured(prompt, args.model, scheduler)
            result = {
                "chunk_index": idx,
                "time_label": time_label,
                "first_time": first_iso,
                "summary": items.get(1, ""),
                "evidence": items.get(2, ""),
                "time_anchors": items.get(3, ""),
            }
            print(f"  Chunk {idx+1}: done", flush=True)
            return result

        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
            futures = {pool.submit(process_chunk, i): i for i in todo}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    state["chunk_results"].append(result)
                    # Save checkpoint
                    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2))
                except Exception as e:
                    idx = futures[future]
                    print(f"  Chunk {idx+1} FAILED: {e}", flush=True)

    # Sort chronologically
    state["chunk_results"].sort(key=lambda x: x.get("first_time") or f"z{x['chunk_index']}")
    print(f"\nPhase 1 complete: {len(state['chunk_results'])} chunks analyzed")

    # Phase 2: Final summary
    if not state.get("final"):
        print("\nPhase 2: Generating final digest...")
        final_prompt = build_final_prompt(state["chunk_results"])
        print(f"  Final prompt: ~{estimate_tokens(final_prompt)} tokens")
        items = call_gemma_structured(final_prompt, args.model, scheduler)
        state["final"] = {
            "timeline": items.get(1, ""),
            "topics": items.get(2, ""),
            "insights": items.get(3, ""),
            "contributors": items.get(4, ""),
        }
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2))
        print("  Final digest generated!")

    # Generate HTML
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    meta = {
        "period": state["chunk_results"][0]["time_label"].split("~")[0].strip()
            + " ~ " + state["chunk_results"][-1]["time_label"].split("~")[-1].strip()
        if state["chunk_results"] else "",
        "guild": guild,
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M KST"),
    }
    html_content = generate_html(state["chunk_results"], state["final"], meta)
    out_path.write_text(html_content, encoding="utf-8")
    print(f"\nOutput: {out_path} ({len(html_content)} bytes)")
    print("Done!")


if __name__ == "__main__":
    main()
