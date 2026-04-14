#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--round", type=int, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--mode", default="quality")
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()

    workspace = Path(args.workspace)
    artifact = json.loads((workspace / "for_eval" / "artifact.json").read_text(encoding="utf-8"))
    quality = artifact["quality"]
    issues = []
    extra_result_lines = []
    if args.mode == "crash":
        print(f"crashing evaluator {args.label}", file=sys.stderr)
        raise SystemExit(2)
    if args.mode == "write_eval_code":
        candidate_root = workspace / "evaluation_code_candidates" / "replay_probe"
        candidate_root.mkdir(parents=True, exist_ok=True)
        (candidate_root / "manifest.json").write_text(
            json.dumps(
                {
                    "name": "replay_probe",
                    "purpose": "evaluation_code",
                    "evaluation_only": True,
                    "entrypoint": "check_replay.py",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (candidate_root / "check_replay.py").write_text(
            "from pathlib import Path\n\nassert Path('for_eval/replay.md').exists()\n",
            encoding="utf-8",
        )
        library_seen = (workspace / "evaluation_code_library" / "round_0001" / "codex_eval" / "replay_probe" / "check_replay.py").exists()
        extra_result_lines.append(f"- library_seen: {str(library_seen).lower()}")
        all_pass = quality == "strong"
        fail_tags = [] if all_pass else ["AC-1"]
        pass_tags = ["AC-2"] if not all_pass else ["AC-1", "AC-2"]
        verdict = "terminate" if all_pass else "progress"
    elif args.mode == "sleep":
        time.sleep(args.sleep_seconds)
        all_pass = quality == "strong"
        fail_tags = [] if all_pass else ["AC-1"]
        pass_tags = ["AC-2"] if not all_pass else ["AC-1", "AC-2"]
        verdict = "terminate" if all_pass else "progress"
    elif args.mode == "always_no_progress":
        all_pass = False
        fail_tags = ["AC-1"]
        pass_tags = ["AC-2"]
        verdict = "no_progress"
    elif args.mode == "issue_ledger":
        all_pass = False
        fail_tags = ["AC-1"]
        pass_tags = ["AC-2"]
        verdict = "progress"
        issues = [
            {
                "issue_id": "missing-recall",
                "summary": "top60 recall is still too low",
                "severity": "high",
                "evidence": [f"{args.label}:top60=16/20"],
            },
            {
                "summary": "false positives still include generic military casebooks",
                "severity": "medium" if args.label == "codex_eval" else "low",
                "evidence": [f"{args.label}:fp=군인_징계_정확도순_최근_5년"],
            },
        ]
    elif args.mode == "no_evidence_pass":
        all_pass = True
        fail_tags = []
        pass_tags = ["QUERY_ANSWERED"]
        verdict = "terminate"
    else:
        all_pass = quality == "strong"
        fail_tags = [] if all_pass else ["AC-1"]
        pass_tags = ["AC-2"] if not all_pass else ["AC-1", "AC-2"]
        verdict = "terminate" if all_pass else "progress"

    (workspace / "result.json").write_text(
        json.dumps(
            {
                "evaluator": args.label,
                "all_pass": all_pass,
                "fail_tags": fail_tags,
                "pass_tags": pass_tags,
                "verdict": verdict,
                "evidence": [] if args.mode == "no_evidence_pass" else [f"quality={quality}", f"round={args.round}"],
                "issues": issues,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    issue_lines = "\n".join(f"- {issue['summary']}" for issue in issues) if issues else "- (none)"
    (workspace / "result.md").write_text(
        "\n".join(
            [
                f"# Fake Eval {args.label}",
                "",
                f"- all_pass: {str(all_pass).lower()}",
                f"- verdict: {verdict}",
                *extra_result_lines,
                "",
                "## Issues",
                issue_lines,
                "",
            ]
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
