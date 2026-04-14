#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd -P)"
PROJECT_DIR="$SCRIPT_DIR"
OUT_DIR="$WORKSPACE_DIR/for_eval"
MEMORY_DIR="$WORKSPACE_DIR/solver_memory"
QUERY_FILE="${QUERY_FILE:-$WORKSPACE_DIR/references/query.md}"
POLICY_FILE="${POLICY_FILE:-$PROJECT_DIR/policy.json}"
DATA_ROOT="${DATA_ROOT:-$WORKSPACE_DIR/data}"

mkdir -p "$OUT_DIR" "$MEMORY_DIR"

python3 "$PROJECT_DIR/memory_search_system.py" \
  --data-root "$DATA_ROOT" \
  --query-file "$QUERY_FILE" \
  --policy-file "$POLICY_FILE" \
  --out-dir "$OUT_DIR"

python3 - "$WORKSPACE_DIR" "$PROJECT_DIR" "$OUT_DIR" "$MEMORY_DIR" <<'PY'
import json
import sys
import time
from pathlib import Path

workspace = Path(sys.argv[1])
project = Path(sys.argv[2])
out_dir = Path(sys.argv[3])
memory = Path(sys.argv[4])

evaluation = json.loads((out_dir / "evaluation.json").read_text(encoding="utf-8"))
selected = json.loads((out_dir / "selected.json").read_text(encoding="utf-8"))
policy = json.loads((project / "policy.json").read_text(encoding="utf-8"))
required_for_eval = [
    "answer.md",
    "evidence_ledger.json",
    "coverage_report.json",
    "run_manifest.json",
    "selected.json",
    "evaluation.json",
    "summary.md",
    "replay.md",
]
artifact_check = {
    name: {
        "exists": (out_dir / name).exists(),
        "bytes": (out_dir / name).stat().st_size if (out_dir / name).exists() else 0,
    }
    for name in required_for_eval
}
missing = [name for name, status in artifact_check.items() if not status["exists"] or status["bytes"] <= 0]

result = {
    "round": int(workspace.name.split("_")[-1]) if workspace.name.split("_")[-1].isdigit() else 0,
    "status": "incomplete" if missing else ("ready" if evaluation.get("all_pass") else "ready_with_failures"),
    "quality": "solver_candidate_with_honest_self_evaluation",
    "evaluation_all_pass": bool(evaluation.get("all_pass")),
    "pass_tags": evaluation.get("pass_tags", []),
    "fail_tags": evaluation.get("fail_tags", []),
    "policy": policy,
    "policy_path": "project/policy.json",
    "selected_ids": selected.get("selected_ids", []),
    "candidate_ids": selected.get("candidate_ids", []),
    "artifacts": sorted(str(path.relative_to(workspace)) for path in out_dir.rglob("*") if path.is_file()),
    "artifact_check": artifact_check,
    "missing_artifacts": missing,
    "changed_files": sorted(
        str(path.relative_to(workspace))
        for path in project.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    ),
    "for_eval_dir": str(out_dir),
}
(workspace / "solver_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

memory.mkdir(parents=True, exist_ok=True)
(memory / "MEMORY.md").write_text(
    "\n".join([
        "# Solver Memory",
        "",
        "## FINAL_GOAL",
        "- Build a generic, source-grounded memory/search system for large corpora.",
        "",
        "## CURRENT_FOCUS",
        "- Improve the current project code until the evaluators pass the domain-data query and six generic failure modes.",
        "",
        "## LAST_RUN",
        f"- timestamp: {time.strftime('%Y-%m-%dT%H:%M:%S%z')}",
        f"- selected_count: {len(selected.get('selected_ids', []))}",
        f"- pass_tags: {', '.join(evaluation.get('pass_tags', []))}",
        f"- fail_tags: {', '.join(evaluation.get('fail_tags', []))}",
        f"- missing_artifacts: {', '.join(missing) if missing else '(none)'}",
        "",
    ]) + "\n",
    encoding="utf-8",
)
(memory / "turn_log.md").write_text(
    "\n".join([
        "# Turn Log",
        "",
        "- Ran `bash project/run_memory_search_task.sh`.",
        f"- Wrote artifacts under `{out_dir}`.",
        f"- Artifact check missing: {', '.join(missing) if missing else '(none)'}",
        "",
    ]) + "\n",
    encoding="utf-8",
)
(memory / "best_code.json").write_text(
    json.dumps(
        {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "code_paths": ["project/memory_search_system.py", "project/run_memory_search_task.sh", "project/policy.json"],
            "artifact_paths": ["for_eval/evidence_ledger.json", "for_eval/answer.md", "for_eval/coverage_report.json"],
        },
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)
(memory / "verification.json").write_text(
    json.dumps(
        {
            "commands": ["python3 -m unittest project/test_memory_search_system.py -v", "bash project/run_memory_search_task.sh"],
            "artifact_check": artifact_check,
            "missing_artifacts": missing,
        },
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)
PY
