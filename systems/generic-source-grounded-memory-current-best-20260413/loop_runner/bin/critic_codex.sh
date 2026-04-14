#!/usr/bin/env bash
set -euo pipefail

WORKSPACE="$1"
ROUND="$2"
RUN_ROOT="$3"
BASE="/path/to/generic_memory_domain_search_loop"
PROMPT="$WORKSPACE/critic_codex_prompt.md"
LAST="$WORKSPACE/critic_codex_last_message.txt"

cd "$WORKSPACE"

cat > "$PROMPT" <<'EOF'
You are critic_codex. You evaluate the solver output. Do not solve the task yourself except to run checks that verify or falsify the solver's claims.

Read:
- references/goal.md
- references/query.md
- references/evaluation_contract.md
- loop_input/handoff.md
- solver_result.json
- for_eval/*
- project/*
- solver_memory/* if present

Focus:
- Does the system actually answer the Messenger/TOKEN and phone-password/brute-force query with source-backed evidence or audited negative evidence?
- Does it solve the six generic failure modes, or merely keyword-search?
- Are raw locators/spans/hashes/replay commands present?
- Is it generic and scalable, or hard-coded to the benchmark?

You may run `bash project/run_memory_search_task.sh` if artifacts are missing or you need to reproduce. Do not edit project code. You may create `evaluation_code_candidates/<tool_name>/` only for reusable evaluation-only helpers.

Write `result.json` only, with the schema in `references/evaluation_contract.md`. Use allowed verdicts only: terminate/progress/no_progress/regress. Be strict. If partially improved, use verdict `progress`, not `terminate`.
EOF

set +e
env CODEX_EXEC_MODEL="${CODEX_EVAL_MODEL:-gpt-5.4}" \
  CODEX_EXEC_REASONING_EFFORT="${CODEX_EVAL_REASONING_EFFORT:-high}" \
  /path/to/scripts/codex_exec_minimal.sh \
  --cd "$WORKSPACE" \
  --skip-git-repo-check \
  --dangerously-bypass-approvals-and-sandbox \
  --add-dir /mnt/d/korean-domain-data \
  --output-last-message "$LAST" \
  - < "$PROMPT"
STATUS=$?
set -e

if [[ "$STATUS" -ne 0 || ! -f result.json ]] || ! python3 "$BASE/bin/validate_eval_result.py" result.json; then
  bash "$BASE/bin/critic_local_fallback.sh" "$WORKSPACE"
fi
