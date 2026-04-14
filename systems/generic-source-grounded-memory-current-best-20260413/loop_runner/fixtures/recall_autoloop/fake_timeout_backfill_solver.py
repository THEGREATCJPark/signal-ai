#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--round", type=int, required=True)
    args = parser.parse_args()

    workspace = Path(args.workspace).resolve()
    fixtures_dir = Path(__file__).resolve().parent

    project_state_path = workspace / "project" / "state.txt"
    if project_state_path.parent.exists():
        project_state_path.write_text(f"timeout-round-{args.round}\n", encoding="utf-8")
    project_policy_path = workspace / "project" / "policy.json"
    project_policy_path.write_text(
        json.dumps({"top_k": 9, "bridge_enabled": False, "round": args.round}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (workspace / "project" / "fixed_keywords.json").write_text(
        json.dumps(
            {
                "keywords": ["장치 계정 사용 강요", "하급심 단계 미주장"],
                "replay_artifact": "for_eval/fixed_keywords.json",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    memory_root = workspace / "solver_memory"
    memory_root.mkdir(parents=True, exist_ok=True)
    (memory_root / "turn_log.md").write_text(
        "# Turn Log\n\n- wrote project and verification plan, then hung before for_eval output\n",
        encoding="utf-8",
    )
    (memory_root / "best_code.json").write_text(
        json.dumps({"round": args.round, "policy_path": "project/policy.json"}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (memory_root / "verification.json").write_text(
        json.dumps(
            {
                "commands": [
                    f"python3 {fixtures_dir / 'fake_backfill_for_eval.py'} --workspace . --round {args.round}"
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    time.sleep(10)


if __name__ == "__main__":
    main()
