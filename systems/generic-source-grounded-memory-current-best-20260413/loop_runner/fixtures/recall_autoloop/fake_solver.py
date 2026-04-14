#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--round", type=int, required=True)
    args = parser.parse_args()

    workspace = Path(args.workspace)
    for_eval = workspace / "for_eval"
    for_eval.mkdir(parents=True, exist_ok=True)
    project_state_path = workspace / "project" / "state.txt"
    seed_before = project_state_path.read_text(encoding="utf-8") if project_state_path.exists() else None
    memory_root = workspace / "solver_memory"
    memory_history_path = memory_root / "history.txt"
    memory_before = memory_history_path.read_text(encoding="utf-8") if memory_history_path.exists() else None
    prior_fail_tags: list[str] = []
    round_context_path = workspace / "loop_input" / "round_context.json"
    if round_context_path.exists():
        round_context = json.loads(round_context_path.read_text(encoding="utf-8"))
        prior_fail_tags = list(round_context.get("prior_agreed_fail_tags", []))

    quality = "strong" if args.round >= 2 else "weak"
    if project_state_path.parent.exists():
        project_state_path.write_text(f"round-{args.round}\n", encoding="utf-8")
    memory_root.mkdir(parents=True, exist_ok=True)
    memory_history_path.write_text((memory_before or "") + f"round-{args.round}\n", encoding="utf-8")
    (memory_root / "turn_log.md").write_text(f"# Turn {args.round}\n\n- quality: {quality}\n", encoding="utf-8")
    (memory_root / "best_code.json").write_text(
        json.dumps({"round": args.round, "best_project_state": f"round-{args.round}\n"}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (memory_root / "verification.json").write_text(
        json.dumps({"round": args.round, "checks": ["fake-check"]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (for_eval / "artifact.json").write_text(
        json.dumps(
            {
                "quality": quality,
                "round": args.round,
                "seed_before": seed_before,
                "memory_before": memory_before,
                "prior_fail_tags": prior_fail_tags,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (workspace / "solver_result.json").write_text(
        json.dumps(
            {
                "round": args.round,
                "quality": quality,
                "seed_before": seed_before,
                "memory_before": memory_before,
                "prior_fail_tags": prior_fail_tags,
                "for_eval_dir": str(for_eval),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (workspace / "solver_summary.md").write_text(
        f"# Fake Solver Summary\n\n- round: {args.round}\n- quality: {quality}\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
