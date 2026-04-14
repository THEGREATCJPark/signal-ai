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
    if project_state_path.parent.exists():
        project_state_path.write_text(f"round-{args.round}\n", encoding="utf-8")

    memory_root = workspace / "solver_memory"
    memory_root.mkdir(parents=True, exist_ok=True)
    (memory_root / "turn_log.md").write_text(f"# Turn {args.round}\n\n- degraded candidate\n", encoding="utf-8")
    (memory_root / "best_code.json").write_text(
        json.dumps({"round": args.round, "best_project_state": f"round-{args.round}\n"}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (memory_root / "verification.json").write_text(
        json.dumps({"round": args.round, "checks": ["partial-solver-check"]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    (for_eval / "artifact.json").write_text(
        json.dumps(
            {
                "quality": "strong",
                "round": args.round,
                "seed_before": None,
                "memory_before": None,
                "prior_fail_tags": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (for_eval / "selected.json").write_text(
        json.dumps({"selected_ids": [f"candidate-{args.round}", "dossier_0001_smoking_gun"]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (for_eval / "policy.json").write_text(
        json.dumps({"top_k": 2, "bridge_enabled": False}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (for_eval / "evaluation.json").write_text(
        json.dumps({"all_pass": True, "selected_ids": [f"candidate-{args.round}", "dossier_0001_smoking_gun"]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (for_eval / "summary.md").write_text(
        "\n".join(
            [
                "# Summary",
                "",
                "## change_made",
                "partial solver wrote usable artifacts but omitted solver_result.json",
                "",
                "## benchmark_effect",
                "usable candidate exists",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (for_eval / "replay.md").write_text("python3 fake_replay.py\n", encoding="utf-8")


if __name__ == "__main__":
    main()
