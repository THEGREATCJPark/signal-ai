#!/usr/bin/env bash
set -euo pipefail

WORKSPACE="$1"
ROUND="$2"
RUN_ROOT="$3"
PROMPT="$WORKSPACE/converger_prompt.md"
LAST="$WORKSPACE/converger_last_message.txt"

cd "$WORKSPACE"

cat > "$PROMPT" <<'EOF'
You are the feedback converger. Do not solve the memory-system task and do not edit project code.

Read:
- critic_feedback/index.md
- critic_feedback/*.md
- solver_result.json
- for_eval/*
- loop_input/handoff.md

Write `converged_feedback.md`.

Rules:
- Include only stable duplicate signals, agreed fail tags, or runner-visible concrete artifact failures.
- Do not include one-off speculation.
- Do not prescribe a narrow hard-coded solution unless both critics identify the same concrete overfit/missing mechanism.
- Preserve pass tags explicitly if both critics agree they passed.
- Keep it natural language, but concrete enough for the next solver to act.
EOF

set +e
env CODEX_EXEC_MODEL="${CODEX_CONVERGER_MODEL:-gpt-5.4}" \
  CODEX_EXEC_REASONING_EFFORT="${CODEX_CONVERGER_REASONING_EFFORT:-medium}" \
  /path/to/scripts/codex_exec_minimal.sh \
  --cd "$WORKSPACE" \
  --skip-git-repo-check \
  --dangerously-bypass-approvals-and-sandbox \
  --output-last-message "$LAST" \
  - < "$PROMPT"
STATUS=$?
set -e

if [[ "$STATUS" -ne 0 || ! -f converged_feedback.md ]]; then
  cat > converged_feedback.md <<'MD'
# Converged Feedback

The converger failed to produce a valid feedback file. Use the runner's fallback merged issue ledger and agreed fail tags for the next solver turn.
MD
fi
