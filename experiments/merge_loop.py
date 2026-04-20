#!/usr/bin/env python3
"""Experiment C: Merge + coverage-patch 루프.

Round 1: 모든 candidate 기사 → LLM이 '최종 퍼블릭 기사'들로 재작성.
  각 final article에 merged_from=[candidate ids] 표시, 최하단에 discard=[ids].
Round 2~3: 이전 final + 언급 안 된(unreferenced) candidates 주고
  "이 중 추가할 거 있나?" → 있으면 new final + merged_from 표시.
  또 언급 안 된 것 재투입. 최대 3 rounds.
Final: 모든 published final 기사.

docs/articles.json 수정 안 함. 결과는 experiments/results/merge_loop_*.json.
"""
from __future__ import annotations
import json, re, sys, time
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent.parent))
from run_hourly import load_keys, KeyScheduler, call_gemma

ROOT = Path(__file__).parent.parent
ARTICLES = ROOT / "docs" / "articles.json"
RESULTS = Path(__file__).parent / "results"
MAX_ROUNDS = 3

ROUND1_PROMPT = """당신은 AI 뉴스 편집장입니다. 아래 candidate 기사들을 검토해 '퍼블릭에 나갈 최종 기사 목록'을 작성하세요.

## 지시
- 중복·유사 내용은 병합: 하나의 최종 기사로 합치고 merged_from에 사용한 candidate id들 모두 기록
- 모순되는 내용은 더 최신·구체적인 쪽으로 정리 (예: '출시 예정'보다 '출시 완료'가 맞다면 후자 기준)
- 명백히 가치 없거나 사실성 의심되는 건 discard 배열에 id만
- 각 최종 기사는 **400~700자 본문** (한국어). 제목 간결·구체.
- candidate 원문에 없는 사실 지어내지 말 것. 병합은 사실의 합집합.
- merged_from에는 candidate id를 문자열로 (예: "98", "114"). 숫자 앞 0 붙이지 말 것.

## 입력 candidate 기사 (id | 제목 | 본문)
{candidates}

## 출력 (JSON만, 다른 텍스트 금지)
{{
  "final": [
    {{"headline": "제목", "body": "본문", "merged_from": ["id1","id2",...]}}
  ],
  "discard": ["id3","id4"]
}}
"""

PATCH_PROMPT = """이전 round에서 이미 작성된 최종 기사들이 있습니다. 아래 '언급 안 된' candidate 기사들 중 최종 퍼블릭에 **추가할 가치가 있는** 것만 가려내 새 기사로 씁니다.

## 이미 확정된 최종 기사 (참고용, 수정 금지)
{existing_finals}

## 언급 안 된 candidate 기사들
{unreferenced}

## 지시
- 이미 확정된 기사와 실질적 중복이면 추가 금지 → discard에 id만
- 완전 새 소식·독립 가치 있는 것만 새 final 기사로
- 새 기사에는 사용한 candidate id들을 merged_from에 기록
- 추가할 게 없으면 final=[] 로 반환

## 출력 (JSON만)
{{
  "final": [
    {{"headline": "제목", "body": "본문", "merged_from": ["id",...]}}
  ],
  "discard": ["id",...]
}}
"""

def cand_block(arts, id_key="id"):
    lines = []
    for a in arts:
        s = a[id_key].split("-")[-1]
        body = a["body"].replace('\n', ' ')
        lines.append(f"{s} | {a['headline']} | {body}")
    return "\n".join(lines)

def final_block(finals):
    lines = []
    for i, f in enumerate(finals, 1):
        lines.append(f"[F{i}] {f['headline']}\n    {f['body'][:300]}")
    return "\n".join(lines) or "(없음)"

def extract_json(text):
    s = text.strip()
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s, flags=re.M).strip()
    start = s.find('{'); end = s.rfind('}')
    if start == -1 or end <= start:
        raise ValueError("no JSON")
    return json.loads(s[start:end+1])

def tolerant_extract(text):
    """Manually extract each final article + discard list, resilient to broken JSON."""
    finals = []
    # Find all {..."headline": "...", "body": "...", "merged_from": [...]...} using balanced braces
    # Scan for "headline" pattern then extract surrounding object manually.
    i = 0
    while i < len(text):
        m = re.search(r'"headline"\s*:\s*"', text[i:])
        if not m:
            break
        # find start of the object: nearest '{' before the headline
        headline_pos = i + m.start()
        obj_start = text.rfind('{', 0, headline_pos)
        if obj_start == -1:
            i = headline_pos + 1
            continue
        # scan forward to find balanced close
        depth = 0
        in_str = False
        esc = False
        end_pos = -1
        for j in range(obj_start, len(text)):
            ch = text[j]
            if esc:
                esc = False; continue
            if ch == '\\' and in_str:
                esc = True; continue
            if ch == '"':
                in_str = not in_str; continue
            if in_str: continue
            if ch == '{': depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    end_pos = j; break
        if end_pos == -1:
            i = headline_pos + 1
            continue
        chunk = text[obj_start:end_pos+1]
        # extract fields with regex (body can have newlines)
        hl_m = re.search(r'"headline"\s*:\s*"((?:[^"\\]|\\.)*)"', chunk)
        bd_m = re.search(r'"body"\s*:\s*"((?:[^"\\]|\\.)*)"', chunk, re.S)
        mf_m = re.search(r'"merged_from"\s*:\s*\[([^\]]*)\]', chunk, re.S)
        if hl_m and bd_m:
            try:
                hl = json.loads('"' + hl_m.group(1) + '"')
                bd = json.loads('"' + bd_m.group(1) + '"')
            except Exception:
                hl = hl_m.group(1)
                bd = bd_m.group(1)
            merged_from = []
            if mf_m:
                merged_from = [x.strip().strip('"\'') for x in mf_m.group(1).split(',') if x.strip()]
            finals.append({"headline": hl, "body": bd, "merged_from": merged_from})
        i = end_pos + 1

    # discard list
    discards = []
    dm = re.search(r'"discard"\s*:\s*\[([^\]]*)\]', text, re.S)
    if dm:
        discards = [x.strip().strip('"\'') for x in dm.group(1).split(',') if x.strip()]

    return {"final": finals, "discard": discards}

def normalize_ids(id_list, short_to_full):
    """LLM이 short id로 뱉은 걸 full id로 매핑. 0-padding, 공백, 부분 일치 관대."""
    # build lookup with normalized keys (strip leading zeros)
    lookup = {}
    for k, v in short_to_full.items():
        lookup[k] = v
        lookup[k.lstrip('0') or '0'] = v  # '098' → '98', '001' → '1'
    out = []
    seen = set()
    for x in id_list or []:
        x = str(x).strip().strip('"\'')
        if not x: continue
        stripped = x.lstrip('0') or '0'
        v = lookup.get(x) or lookup.get(stripped)
        if v and v not in seen:
            out.append(v); seen.add(v)
        elif x in short_to_full.values() and x not in seen:
            out.append(x); seen.add(x)
    return out

def run_round(candidates, existing_finals, sched, round_num, short_to_full):
    if round_num == 1:
        prompt = ROUND1_PROMPT.format(candidates=cand_block(candidates))
    else:
        prompt = PATCH_PROMPT.format(
            existing_finals=final_block(existing_finals),
            unreferenced=cand_block(candidates),
        )
    print(f"\n== Round {round_num}: {len(candidates)} candidates, {len(existing_finals)} existing finals ==")
    print(f"   prompt size: {len(prompt):,} chars")
    t0 = time.time()
    raw = call_gemma(prompt, sched, max_tok=32768, temp=0.3, json_mode=True)
    dt = time.time() - t0
    RESULTS.mkdir(exist_ok=True)
    raw_path = RESULTS / f"merge_raw_r{round_num}_{datetime.now().strftime('%H%M%S')}.txt"
    raw_path.write_text(raw, encoding="utf-8")
    try:
        obj = extract_json(raw)
    except Exception as e:
        print(f"   PARSE FAIL: {e}  (raw → {raw_path.name})")
        # try tolerant extraction: find each {...} object in "final" array
        obj = tolerant_extract(raw)
        if not obj["final"] and not obj["discard"]:
            print(f"   tolerant also failed")
            return {"final": [], "discard": []}
        print(f"   tolerant recovered: {len(obj['final'])} final, {len(obj['discard'])} discard")
    finals = obj.get("final", []) or []
    discards = obj.get("discard", []) or []
    # normalize ids
    for f in finals:
        f["merged_from"] = normalize_ids(f.get("merged_from", []), short_to_full)
    discards_full = normalize_ids(discards, short_to_full)
    print(f"   → {len(finals)} finals, {len(discards_full)} discards  ({dt:.1f}s)")
    return {"final": finals, "discard": discards_full}

def unreferenced_ids(all_candidates, merged_ids_set, discard_ids_set):
    return [c for c in all_candidates if c["id"] not in merged_ids_set and c["id"] not in discard_ids_set]

def main():
    data = json.loads(ARTICLES.read_text())
    # 실험: 새로 뽑힌 기사 = placement가 이미 할당된 기사 중 legacy 제외(실사용 맥락 재현)
    # 여기선 그냥 전체 116 기사를 candidates로 취급해 정확도 테스트
    candidates = data["articles"]
    short_to_full = {a["id"].split("-")[-1]: a["id"] for a in candidates}
    all_ids = {a["id"] for a in candidates}

    sched = KeyScheduler(load_keys())
    print(f"총 candidate: {len(candidates)}")

    all_finals = []
    all_discards = set()
    mentioned = set()
    round_log = []

    # Round 1: all candidates
    r1 = run_round(candidates, [], sched, 1, short_to_full)
    all_finals.extend(r1["final"])
    all_discards |= set(r1["discard"])
    for f in r1["final"]:
        mentioned |= set(f["merged_from"])
    round_log.append({"round": 1, "candidate_count": len(candidates),
                      "new_finals": len(r1["final"]), "new_discards": len(r1["discard"])})

    # Rounds 2, 3: coverage patch
    for rn in range(2, MAX_ROUNDS + 1):
        unref = [c for c in candidates if c["id"] not in mentioned and c["id"] not in all_discards]
        if not unref:
            print(f"\n== 모든 candidate 처리됨. Round {rn} 생략 ==")
            break
        rN = run_round(unref, all_finals, sched, rn, short_to_full)
        all_finals.extend(rN["final"])
        all_discards |= set(rN["discard"])
        for f in rN["final"]:
            mentioned |= set(f["merged_from"])
        round_log.append({"round": rn, "candidate_count": len(unref),
                          "new_finals": len(rN["final"]), "new_discards": len(rN["discard"])})

    still_unref = [c["id"] for c in candidates if c["id"] not in mentioned and c["id"] not in all_discards]

    # save
    result = {
        "generated_at": datetime.now().isoformat(),
        "rounds": round_log,
        "final_count": len(all_finals),
        "discard_count": len(all_discards),
        "still_unreferenced_count": len(still_unref),
        "still_unreferenced_ids": still_unref,
        "final": all_finals,
        "discard": sorted(all_discards),
    }
    out = RESULTS / f"merge_loop_{datetime.now().strftime('%H%M%S')}.json"
    RESULTS.mkdir(exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2))

    # summary
    print(f"\n== SUMMARY ==")
    for r in round_log:
        print(f"  Round {r['round']}: cand={r['candidate_count']}, +{r['new_finals']} finals, +{r['new_discards']} discards")
    print(f"  최종 final: {len(all_finals)}")
    print(f"  discard: {len(all_discards)}")
    print(f"  처리 안 된 (drop by default): {len(still_unref)}")
    print(f"saved → {out}")

    print(f"\n== FINAL ARTICLES ==")
    for i, f in enumerate(all_finals, 1):
        print(f"  [{i:02d}] {f['headline']}")
        print(f"       merged_from: {f['merged_from']}")

if __name__ == "__main__":
    main()
