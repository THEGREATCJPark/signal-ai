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

    workspace = Path(args.workspace).resolve()
    for_eval = workspace / "for_eval"
    for_eval.mkdir(parents=True, exist_ok=True)

    selected_ids = [f"backfilled-{args.round}", "dossier_0001_smoking_gun"]
    (for_eval / "artifact.json").write_text(
        json.dumps(
            {
                "quality": "strong",
                "round": args.round,
                "selected_ids": selected_ids,
                "backfilled": True,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (for_eval / "selected.json").write_text(
        json.dumps({"selected_ids": selected_ids}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (for_eval / "evaluation.json").write_text(
        json.dumps({"all_pass": True, "selected_ids": selected_ids}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (for_eval / "summary.md").write_text(
        "# Summary\n\n## change_made\nbackfilled missing evaluation artifacts\n",
        encoding="utf-8",
    )
    (for_eval / "replay.md").write_text(
        "python3 fake_backfill_for_eval.py --workspace .\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
