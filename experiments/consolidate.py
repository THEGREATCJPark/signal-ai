#!/usr/bin/env python3
"""Experiment B: 전체 기사 토픽 클러스터링 + 충돌 감지.

한 번의 LLM 호출로 모든 기사를 토픽별로 묶고, 각 클러스터에서:
- duplicate: 같은 사실 반복 → 하나만 유지
- contradiction: 충돌 (예: '출시 예정' vs '출시됨') → 더 구체적/최근인 것 유지
- none: 독립적 토픽

결과는 실제로 적용하지 않음. JSON으로 제안만 출력.
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent.parent))
from run_hourly import load_keys, KeyScheduler, call_gemma

ROOT = Path(__file__).parent.parent
ARTICLES = ROOT / "docs" / "articles.json"
RESULTS = Path(__file__).parent / "results"

PROMPT = """당신은 AI 뉴스 편집장입니다. 아래 기사 목록을 읽고 같은 topic끼리 묶으세요. 각 클러스터에서 중복·충돌이 있으면 처리 방안을 제안하세요.

## 입력 (id | 제목 | 본문 요약)
{articles}

## 출력 (JSON만, 다른 설명 금지)
{{
  "clusters": [
    {{
      "topic": "짧은 한글 topic",
      "ids": ["id1", "id2", "id3"],
      "issue": "duplicate|contradiction|none",
      "note": "왜 이 판정인지 한 문장",
      "keep": ["id_best"],
      "drop": ["id_worse"]
    }}
  ]
}}

## 규칙
- 2개 이상 같은 사실·같은 대상을 다루면 cluster로 묶기
- duplicate: 본질적으로 같은 내용 → 가장 구체적이거나 최근인 id를 keep, 나머지 drop
- contradiction: 상충 (예: '출시 예정' vs '이미 출시') → 더 구체적·단정적인 쪽을 keep
- 독립 topic은 clusters에 포함하지 말 것 (노이즈 줄이기)
- keep과 drop은 ids의 부분집합. keep ∪ drop = ids. keep과 drop은 disjoint.
"""

def main():
    data = json.loads(ARTICLES.read_text())
    articles = data["articles"]

    # id 짧게 (뒤 3자리만 사용)
    short = {}
    for a in articles:
        s = a["id"].split("-")[-1]  # e.g., '01', '114'
        short[s] = a["id"]

    lines = []
    for a in articles:
        s = a["id"].split("-")[-1]
        body = a["body"].replace('\n', ' ')  # 풀 바디
        lines.append(f"{s} | {a['headline']} | {body}")
    block = "\n".join(lines)

    prompt = PROMPT.format(articles=block)
    print(f"prompt: {len(prompt):,} chars, {len(articles)} articles")

    sched = KeyScheduler(load_keys())
    raw = call_gemma(prompt, sched, max_tok=32768, temp=0.2, json_mode=True)

    s = raw.strip()
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s, flags=re.M).strip()
    start = s.find('{'); end = s.rfind('}')
    try:
        obj = json.loads(s[start:end+1])
    except Exception as e:
        print(f"parse fail: {e}")
        print("raw:", raw[:800])
        return

    # expand short ids to real ids
    idmap = {a["id"].split("-")[-1]: a for a in articles}
    clusters = obj.get("clusters", [])
    print(f"\n== {len(clusters)} clusters ==\n")
    for c in clusters:
        issue = c.get("issue", "?")
        tag = {"duplicate":"📋","contradiction":"⚡","none":"·"}.get(issue, "?")
        print(f"{tag} [{issue}] {c.get('topic','?')}  ({len(c.get('ids',[]))}개)")
        print(f"   note: {c.get('note','')}")
        for iid in c.get("ids", []):
            a = idmap.get(iid)
            if a:
                marker = "✓keep" if iid in c.get("keep",[]) else ("✗drop" if iid in c.get("drop",[]) else " ")
                print(f"   {marker}  [{a.get('placement','?'):5s}] {iid}: {a['headline'][:60]}")
            else:
                print(f"   ??  {iid}: (not found)")
        print()

    out = RESULTS / f"consolidate_{datetime.now().strftime('%H%M%S')}.json"
    RESULTS.mkdir(exist_ok=True)
    out.write_text(json.dumps(obj, ensure_ascii=False, indent=2))
    print(f"saved → {out}")

    # summary counts
    n_dup = sum(1 for c in clusters if c.get("issue") == "duplicate")
    n_con = sum(1 for c in clusters if c.get("issue") == "contradiction")
    n_drops = sum(len(c.get("drop",[])) for c in clusters)
    print(f"\n== summary ==  duplicates={n_dup}  contradictions={n_con}  total_drops={n_drops}")

if __name__ == "__main__":
    main()
