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

def _tag_batch_once(articles, sched):
    """한 번 호출. 성공한 id:tag dict 반환."""
    lines = []
    for a in articles:
        body = (a.get("body") or "").replace("\n", " ")[:260]
        lines.append(f"{a['id']} | {a['headline']} | {body}")
    prompt = TAG_PROMPT.format(block="\n".join(lines))
    raw = call_gemma(prompt, sched, max_tok=8192, temp=0.1, json_mode=True)
    s = raw.strip()
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s, flags=re.M).strip()
    # 여러 JSON 객체 가능성 → 첫 {~} 균형찬 추출
    tags = {}
    start = s.find('{')
    if start == -1:
        return tags
    depth = 0; end = -1; in_str = False; esc = False
    for j in range(start, len(s)):
        ch = s[j]
        if esc: esc = False; continue
        if ch == '\\' and in_str: esc = True; continue
        if ch == '"': in_str = not in_str; continue
        if in_str: continue
        if ch == '{': depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0: end = j; break
    if end == -1:
        return tags
    try:
        obj = json.loads(s[start:end+1])
        for t in obj.get("tags", []) or []:
            iid = str(t.get("id", "")).strip().strip('"\'')
            cat = str(t.get("category", "")).strip().lower()
            trust = str(t.get("trust", "")).strip().lower()
            if cat not in ("news", "rumor"): cat = "rumor"
            if trust not in ("high", "low"): trust = "low"
            if iid: tags[iid] = {"category": cat, "trust": trust}
    except Exception:
        pass
    return tags

def tag_articles(articles, sched, max_retries=3):
    """누락된 id만 모아 재시도. 최종 남으면 rumor/low default."""
    if not articles:
        return {}
    LOG(f"tag_articles: {len(articles)} total")
    all_tags = {}
    remaining = list(articles)
    for attempt in range(1, max_retries + 1):
        if not remaining: break
        got = _tag_batch_once(remaining, sched)
        for iid, t in got.items():
            all_tags[iid] = t
        prev = len(remaining)
        remaining = [a for a in remaining if a["id"] not in all_tags]
        LOG(f"  attempt {attempt}: got {len(got)} new, {len(remaining)} missing")
        if len(remaining) == prev and attempt < max_retries:
            # 진전 없으면 한번 더 시도 (다른 key 쓰이길)
            continue
    # 남은 건 default
    for a in articles:
        if a["id"] not in all_tags:
            all_tags[a["id"]] = {"category": "rumor", "trust": "low"}
    return all_tags

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
