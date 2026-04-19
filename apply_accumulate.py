#!/usr/bin/env python3
"""One-off: 지난 11개 기사 복원 + 오늘 21개 유지 → 태깅 → cross-dedup → classify.

이전 run들이 기사를 wipe해서 accumulate 흐름 깨졌음. 이걸로 복구.
- 11(yesterday merge 결과) + 21(today scan 원본) = 32 시작점
- LLM 배치로 category/trust 태깅 (기사마다 news/rumor + high/low)
- 21(new) vs 11(existing) cross-dedup (같은 내용만 drop)
- classify → TOP/MAIN/SIDE
- 모든 kept 기사는 archive에 노출
"""
import json, re, sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
sys.path.insert(0, str(Path(__file__).parent))
from run_hourly import (
    load_keys, KeyScheduler, call_gemma,
    cross_existing_dedup, _classify_and_save,
    KST, LOG, MODEL, ROOT
)

TAG_PROMPT = """아래 기사들을 한 건씩 category·trust 태그 부여.
- category: "news" = 공식 발표/모델 카드/공식 블로그 근거 / "rumor" = 찌라시/루머/개인 트윗만/유출 주장
- trust: "high" = 여러 출처, 구체 근거, 공식 / "low" = 단일 언급, 애매, 농담일 수도

## 기사
{block}

## 출력 (JSON만, 다른 텍스트 금지)
{{"tags":[{{"id":"ID","category":"news|rumor","trust":"high|low"}}, ...]}}

모든 id에 대해 판정. 누락 금지."""

def tag_articles(articles, sched):
    if not articles:
        return {}
    lines = []
    for a in articles:
        body = (a.get("body") or "").replace("\n", " ")[:260]
        lines.append(f"{a['id']} | {a['headline']} | {body}")
    prompt = TAG_PROMPT.format(block="\n".join(lines))
    LOG(f"tag_articles: {len(articles)}, prompt {len(prompt):,} chars")
    raw = call_gemma(prompt, sched, max_tok=8192, temp=0.1, json_mode=True)
    s = raw.strip()
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s, flags=re.M).strip()
    start = s.find('{'); end = s.rfind('}')
    tags = {}
    try:
        if start != -1 and end > start:
            obj = json.loads(s[start:end+1])
            for t in obj.get("tags", []) or []:
                iid = str(t.get("id", "")).strip().strip('"\'')
                cat = str(t.get("category", "")).strip().lower()
                trust = str(t.get("trust", "")).strip().lower()
                if cat not in ("news", "rumor"): cat = "rumor"
                if trust not in ("high", "low"): trust = "low"
                tags[iid] = {"category": cat, "trust": trust}
    except Exception as e:
        LOG(f"  tag parse fail: {e}; 기본값(rumor/low)")
    # missing은 기본 rumor/low
    for a in articles:
        if a["id"] not in tags:
            tags[a["id"]] = {"category": "rumor", "trust": "low"}
    return tags

def main():
    # 11 복원 + 21 combine
    prev_path = Path("/tmp/prev_11.json")
    if not prev_path.exists():
        import subprocess
        subprocess.run(["git", "-C", str(ROOT), "show", "e5ac5a6:docs/articles.json"],
                       stdout=open(prev_path, "w"), check=True)
    prev = json.loads(prev_path.read_text())
    curr_path = ROOT / "docs" / "articles.json"
    curr = json.loads(curr_path.read_text())

    prev_articles = prev["articles"]
    curr_articles = curr["articles"]
    LOG(f"restored: {len(prev_articles)} (old/existing), current: {len(curr_articles)} (new/today)")

    sched = KeyScheduler(load_keys())
    now = datetime.now(KST)

    # Tag all 32
    all_32 = prev_articles + curr_articles
    # 중복 id 방지
    seen = set(); dedup = []
    for a in all_32:
        if a["id"] in seen: continue
        seen.add(a["id"]); dedup.append(a)
    all_32 = dedup
    LOG(f"total unique: {len(all_32)}")

    tags = tag_articles(all_32, sched)
    for a in all_32:
        t = tags.get(a["id"], {})
        a["category"] = t.get("category", "rumor")
        a["trust"] = t.get("trust", "low")

    # Split back: prev_articles = existing, curr_articles = new
    existing_ids = {a["id"] for a in prev_articles}
    existing = [a for a in all_32 if a["id"] in existing_ids]
    new_cands = [a for a in all_32 if a["id"] not in existing_ids]
    LOG(f"existing: {len(existing)}, new candidates: {len(new_cands)}")

    # Cross-existing dedup (new 중 existing과 같은 내용만 drop)
    new_kept, _ = cross_existing_dedup(new_cands, existing, sched)

    # state 구성 (existing는 그대로, new_kept는 classify로 placement 받음)
    state = {
        "schema_version": 2,
        "last_run_at": now.isoformat(),
        "generated_at": now.isoformat(),
        "model": MODEL,
        "articles": existing,  # 기존 11 유지
        "decision_log": [],
    }

    _classify_and_save(state, new_kept, now, sched)

if __name__ == "__main__":
    main()
