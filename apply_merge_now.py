#!/usr/bin/env python3
"""One-off: 이미 실행한 merge_loop 결과를 현재 articles.json에 적용하고 재분류.

- experiments/results/merge_loop_*.json 중 최신 사용 (재호출 없이 캐시 재활용)
- 기존 116 기사 → 16 merged 로 교체
- TOP/MAIN/SIDE 재분류 (기존 placement/decision_log 리셋)
- articles.json 저장 + gist 푸시
"""
import json, sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
sys.path.insert(0, str(Path(__file__).parent))
from run_hourly import load_keys, KeyScheduler, _classify_and_save, LOG, KST

ROOT = Path(__file__).parent
ARTICLES = ROOT / "docs" / "articles.json"

def main():
    # find latest merge_loop result
    results_dir = ROOT / "experiments" / "results"
    candidates = sorted(results_dir.glob("merge_loop_*.json"))
    if not candidates:
        print("no merge_loop_*.json found in experiments/results")
        sys.exit(1)
    merge_file = candidates[-1]
    LOG(f"using {merge_file.name}")
    merged = json.loads(merge_file.read_text())
    LOG(f"  final={merged['final_count']}, discard={merged['discard_count']}")

    state = json.loads(ARTICLES.read_text())
    source_by_id = {a["id"]: a for a in state["articles"]}

    now = datetime.now(KST)
    finals = merged["final"]
    new_articles = []
    for i, f in enumerate(finals):
        mf = f.get("merged_from", [])
        src_dates = [source_by_id[m]["created_at"] for m in mf if m in source_by_id]
        created = max(src_dates) if src_dates else now.isoformat()
        new_articles.append({
            "id": f"art-{now.strftime('%Y%m%d%H%M')}-m{i+1:02d}",
            "headline": f["headline"],
            "body": f["body"],
            "created_at": created,
            "placement": None,
            "placed_at": now.isoformat(),
            "merged_from": mf,
        })

    # Reset state — replace all articles with merged ones
    state["articles"] = []
    state["decision_log"] = []
    state["last_run_at"] = None

    LOG(f"classifying {len(new_articles)} merged articles...")
    sched = KeyScheduler(load_keys())
    _classify_and_save(state, new_articles, now, sched)

if __name__ == "__main__":
    main()
