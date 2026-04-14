#!/usr/bin/env bash
set -euo pipefail

WORKSPACE="$1"
ROUND="$2"
RUN_ROOT="$3"
BASE="/path/to/generic_memory_domain_search_loop"
PROMPT="$WORKSPACE/solver_prompt.md"
LAST="$WORKSPACE/solver_last_message.txt"

cd "$WORKSPACE"
mkdir -p solver_memory for_eval

cat > "$PROMPT" <<'EOF'
You are the solver in a generic memory/search-system loop.

Read first:
- references/goal.md
- references/query.md
- references/repo_notes.md
- loop_input/handoff.md
- loop_input/converged_feedback.md if present

You own only this workspace. Edit files under `project/` and `solver_memory/`. Do not edit evaluator references. Do not print API keys or secrets.

Task:
Build a generic memory/search/recommendation system for large corpora, then execute it on `/mnt/d/korean-domain-data` for the query in `references/query.md`.

Hard requirements:
- Preserve raw source records by locator/hash/span. Do not rely only on lossy summaries.
- Address the six failure modes in `references/goal.md`.
- Avoid overfitting: query-specific terms may be a config/test adapter, but the architecture must be reusable.
- Use streaming/indexed/bounded processing so the method can scale toward ~1M records.
- If you use an API, prefer available environment-backed Gemma/Gemini 10-key scheduling, Codex OAuth, Claude Code OAuth, or free/local APIs. Read credentials from the environment only and do not log them. A deterministic fallback is acceptable and must remain replayable.
- Write concrete code and run it. Vague design prose is not enough.

Required artifacts:
- `for_eval/answer.md`
- `for_eval/evidence_ledger.json`
- `for_eval/coverage_report.json`
- `for_eval/run_manifest.json`
- `for_eval/selected.json`
- `for_eval/evaluation.json`
- `for_eval/summary.md`
- `for_eval/replay.md`
- `solver_result.json`
- `solver_memory/MEMORY.md`
- `solver_memory/turn_log.md`
- `solver_memory/best_code.json`
- `solver_memory/verification.json`

You may start from the seed by running:

```bash
bash project/run_memory_search_task.sh
```

Before writing `solver_result.json`, verify that the run artifacts exist. If the current candidate is not complete, still write honest artifacts that say what is missing so the critics can feed back concrete failures.
EOF

(
  while true; do
    date +%s > solver_memory/live_heartbeat.txt
    sleep 300
  done
) &
HEARTBEAT_PID=$!
trap 'kill "$HEARTBEAT_PID" 2>/dev/null || true' EXIT

set +e
env CODEX_EXEC_MODEL="${CODEX_EXEC_MODEL:-gpt-5.4}" \
  CODEX_EXEC_REASONING_EFFORT="${CODEX_EXEC_REASONING_EFFORT:-high}" \
  /path/to/scripts/codex_exec_minimal.sh \
  --cd "$WORKSPACE" \
  --skip-git-repo-check \
  --dangerously-bypass-approvals-and-sandbox \
  --add-dir /mnt/d/korean-domain-data \
  --output-last-message "$LAST" \
  - < "$PROMPT"
CODEX_STATUS=$?
set -e

if [[ ! -f solver_result.json || ! -d for_eval ]]; then
  echo "solver did not emit full artifacts; running seed/backfill command" >> solver_memory/turn_log.md
  bash project/run_memory_search_task.sh
fi

if [[ "$CODEX_STATUS" -ne 0 ]]; then
  python3 - "$WORKSPACE" "$CODEX_STATUS" <<'PY'
import json, sys
from pathlib import Path
workspace = Path(sys.argv[1])
status = int(sys.argv[2])
path = workspace / "solver_result.json"
payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
payload["codex_status"] = status
payload["status"] = payload.get("status", "ready_with_codex_error")
path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
fi
