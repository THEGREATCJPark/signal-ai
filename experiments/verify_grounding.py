#!/usr/bin/env python3
"""Experiment A: per-article chat-grounding.

각 기사에 대해:
1. 제목+본문의 핵심 키워드로 Discord 채팅에서 관련 메시지 수집
2. 그 스니펫+기사를 LLM에 주고 "joke/speculation/supported/unsupported" 판정
3. 판정 + 근거 인용 + confidence를 JSON으로 받음

목표: DeepSeek 6.7 같은 농담을 잡아낼 수 있는지
상태: docs/articles.json 수정 없음. 결과는 experiments/results/grounding_*.json로 출력.

사용:
  python3 verify_grounding.py --ids 62,111,98,114  # 특정 id만
  python3 verify_grounding.py --all                # 전체
  python3 verify_grounding.py --placement side     # side만
"""
from __future__ import annotations
import argparse, json, re, sys, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from run_hourly import load_keys, KeyScheduler, call_gemma, read_chat_text

ROOT = Path(__file__).parent.parent
ARTICLES = ROOT / "docs" / "articles.json"
CHAT = Path("/home/pineapple/checkpoints/2026-04-17-discord-export-1365049274068631644/channel_1365049274068631644__until_2026-04-17_01-23-52_KST.raw.txt")
RESULTS = Path(__file__).parent / "results"

KEYWORD_RE = re.compile(r'[A-Za-z가-힣]+(?:[.-]\d+(?:\.\d+)?)?|\d+\.\d+(?:\.\d+)?')

def extract_keywords(text: str, top_n: int = 8) -> list[str]:
    """핵심 명사/숫자 추출. 빈도순 + 길이 가중."""
    words = KEYWORD_RE.findall(text)
    stop = {'the','and','or','a','an','is','을','를','의','이','가','에','으로','그','는','은','도'}
    freq = {}
    for w in words:
        if len(w) < 2 or w.lower() in stop: continue
        freq[w] = freq.get(w, 0) + 1
    ranked = sorted(freq.items(), key=lambda x: (-x[1], -len(x[0])))
    return [w for w, _ in ranked[:top_n]]

def find_chat_snippets(chat: str, keywords: list[str], max_chars: int = 6000) -> str:
    """키워드가 하나라도 나오는 메시지 블록 모음. 메시지 경계 기준."""
    msg_re = re.compile(r'(^\[\d{4}\. \d{1,2}\. \d{1,2}\. (?:오전|오후) \d{1,2}:\d{2}\][^\n]*\n)', re.M)
    parts = msg_re.split(chat)
    blocks = []
    i = 1
    while i < len(parts):
        blocks.append(parts[i] + (parts[i+1] if i+1 < len(parts) else ""))
        i += 2
    kw_lower = [k.lower() for k in keywords]
    matched = []
    for b in blocks:
        bl = b.lower()
        if any(k in bl for k in kw_lower):
            matched.append(b)
    joined = "\n".join(matched)
    if len(joined) > max_chars:
        joined = joined[:max_chars] + "\n... (truncated)"
    return joined

PROMPT = """당신은 팩트체커입니다. 아래 '기사'가 '원본 Discord 채팅 스니펫'에 의해 얼마나 뒷받침되는지 판정하세요.

## 판정 기준
- supported: 기사 핵심 주장이 채팅에 명시적으로 여러 번 또는 구체적 근거와 함께 등장
- partial: 일부 주장은 있지만 세부는 추측/과장
- speculation: 채팅은 가능성/루머만 언급. 기사는 단정하듯 썼음
- joke: 채팅 문맥이 명백히 농담/밈/빈정거림 ('when?', '~6.7이다' 같은 장난스러운 표현)
- unsupported: 채팅에서 해당 주제가 안 나오거나, 근거 없이 지어낸 듯

## 기사
제목: {title}
본문: {body}

## 원본 Discord 스니펫 (키워드: {keywords})
{snippets}

## 출력 (JSON만, 다른 설명 금지)
{{
  "verdict": "supported|partial|speculation|joke|unsupported",
  "confidence": 0.0-1.0,
  "evidence_quote": "채팅 원문에서 직접 인용 (없으면 \\"\\")",
  "reason": "한 문장 근거"
}}
"""

def verify_article(article: dict, chat: str, sched) -> dict:
    kws = extract_keywords(article["headline"] + " " + article["body"][:300])
    snippets = find_chat_snippets(chat, kws, max_chars=6000)
    prompt = PROMPT.format(
        title=article["headline"],
        body=article["body"][:500],
        keywords=", ".join(kws),
        snippets=snippets or "(해당 키워드로 찾은 채팅 없음)",
    )
    raw = call_gemma(prompt, sched, max_tok=4096, temp=0.1, json_mode=True)
    s = raw.strip()
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s, flags=re.M).strip()
    start = s.find('{'); end = s.rfind('}')
    try:
        obj = json.loads(s[start:end+1])
    except Exception as e:
        obj = {"verdict": "parse_error", "confidence": 0.0, "evidence_quote": "", "reason": str(e), "raw": raw[:500]}
    obj["id"] = article["id"]
    obj["headline"] = article["headline"]
    obj["placement"] = article.get("placement")
    obj["keywords"] = kws
    obj["snippets_len"] = len(snippets)
    return obj

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--ids", help="comma-separated suffix matches (e.g., '62,111')")
    g.add_argument("--all", action="store_true")
    g.add_argument("--placement", choices=("top","main","side"))
    args = ap.parse_args()

    RESULTS.mkdir(exist_ok=True)
    data = json.loads(ARTICLES.read_text())
    articles = data["articles"]

    if args.ids:
        wanted = set(args.ids.split(","))
        targets = [a for a in articles if any(a["id"].endswith(w) or a["id"] == w for w in wanted)]
    elif args.all:
        targets = articles
    else:
        targets = [a for a in articles if a.get("placement") == args.placement]

    print(f"verifying {len(targets)} articles...")
    chat = read_chat_text(CHAT)
    keys = load_keys()
    sched = KeyScheduler(keys)

    results = []
    for i, a in enumerate(targets, 1):
        t0 = time.time()
        r = verify_article(a, chat, sched)
        dt = time.time() - t0
        tag = r["verdict"]
        color = {"supported":"✓","partial":"~","speculation":"?","joke":"🃏","unsupported":"✗","parse_error":"!"}[tag] if tag in {"supported","partial","speculation","joke","unsupported","parse_error"} else "?"
        print(f"[{i}/{len(targets)}] {color} {tag:12s} conf={r.get('confidence',0):.2f} {a['id']} — {a['headline'][:50]}  ({dt:.1f}s)")
        results.append(r)

    out = RESULTS / f"grounding_{datetime.now().strftime('%H%M%S')}.json"
    out.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    print(f"\nsaved → {out}")

    # summary
    from collections import Counter
    c = Counter(r["verdict"] for r in results)
    print("\n== summary ==")
    for k, v in c.most_common():
        print(f"  {k}: {v}")

if __name__ == "__main__":
    main()
