#!/usr/bin/env python3
"""One-off: 이번 scan의 33개 후보를 dedup_cluster (body 재작성 X) + classify로만 처리.

- 기존 11개 (merge에서 합성된) 버림
- scan 원본 제목·본문 그대로 dedup
- classify만 거쳐 TOP/MAIN/SIDE 배치
- 모든 unique 기사가 어딘가에 노출됨 (archive 포함)
"""
import json, sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
sys.path.insert(0, str(Path(__file__).parent))
from run_hourly import load_keys, KeyScheduler, dedup_cluster, _classify_and_save, KST, LOG, MODEL

ROOT = Path(__file__).parent
CACHE = ROOT / "data" / "scan_cache_recovered.json"

def main():
    candidates = json.loads(CACHE.read_text(encoding="utf-8"))
    LOG(f"loaded {len(candidates)} scan candidates")

    # Reset state — scan 원본만으로 재구축
    now = datetime.now(KST)
    state = {
        "schema_version": 2,
        "last_run_at": now.isoformat(),
        "generated_at": now.isoformat(),
        "model": MODEL,
        "articles": [],
        "decision_log": [],
    }

    sched = KeyScheduler(load_keys())

    kept, dropped = dedup_cluster(candidates, sched)
    LOG(f"dedup_cluster: kept {len(kept)}, dropped {len(dropped)}")

    _classify_and_save(state, kept, now, sched)

if __name__ == "__main__":
    main()
