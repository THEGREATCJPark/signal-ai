#!/usr/bin/env python3
"""원본 scan 33개 중 현재 articles.json에 없는 15개를 복원 + 태그 + classify.

이전 dedup이 과하게 잘라냈음. 엄격한 기준으로 재판정 — 루머·예측·다른 측면은 모두 유지.
"""
import json, sys
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent))
from run_hourly import load_keys, KeyScheduler, _classify_and_save, KST, LOG, MODEL, ROOT
from apply_accumulate import tag_articles

def main():
    curr = json.loads(Path("/home/pineapple/bunjum2/signal/docs/articles.json").read_text())
    cache = json.loads(Path("/home/pineapple/bunjum2/signal/data/scan_cache_recovered.json").read_text())
    curr_titles = {a['headline'] for a in curr['articles']}
    missing = [a for a in cache if a['headline'] not in curr_titles]
    LOG(f"복원 대상: {len(missing)}개")
    for m in missing:
        LOG(f"  + {m['headline']}")

    now = datetime.now(KST)
    # 새 id로 재할당 (cache의 원래 id 유지해도 OK)
    for m in missing:
        m.setdefault("placement", None)
        m.setdefault("placed_at", now.isoformat())
        m.setdefault("created_at", now.isoformat())

    sched = KeyScheduler(load_keys())
    tags = tag_articles(missing, sched)
    for a in missing:
        t = tags.get(a["id"], {})
        a["category"] = t.get("category", "rumor")
        a["trust"] = t.get("trust", "low")

    # state: 기존 + restored 모두 한 번에 classify
    state = {
        "schema_version": 2,
        "last_run_at": now.isoformat(),
        "generated_at": now.isoformat(),
        "model": MODEL,
        "articles": curr["articles"],  # 기존 29 유지
        "decision_log": curr.get("decision_log", []),
    }
    LOG(f"classify: {len(state['articles'])} existing + {len(missing)} restored")
    _classify_and_save(state, missing, now, sched)

if __name__ == "__main__":
    main()
