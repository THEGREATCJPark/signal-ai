#!/usr/bin/env bash
set -euo pipefail

WORKSPACE="$1"
ROUND="$2"
RUN_ROOT="$3"
PROMPT="$WORKSPACE/critic_claude_prompt.md"
LAST="$WORKSPACE/critic_claude_last_message.txt"

cd "$WORKSPACE"

cat > "$PROMPT" <<'EOF'
You are critic_claude. You are an adversarial evaluator, not a solver.

Read:
- references/goal.md
- references/query.md
- references/evaluation_contract.md
- loop_input/handoff.md
- solver_result.json
- for_eval/*
- project/*
- solver_memory/* if present

Your main job is to catch false convergence:
- Generic architecture claims that are not reflected in executable artifacts.
- Keyword hits that do not prove TOKEN/SIM -> Messenger login or brute-force/passcode unlocking.
- "Not found" answers without audited negative search.
- Any source conclusion that is not grounded to original record locators/spans/hashes.
- Scalability claims that would fail on about one million records.

Do not patch the solver. Write `result.json` with the schema from `references/evaluation_contract.md`. Use only terminate/progress/no_progress/regress.
EOF

claude -p \
  --dangerously-skip-permissions \
  --add-dir "$WORKSPACE" \
  --add-dir /mnt/d/korean-domain-data \
  --output-format text \
  < "$PROMPT" > "$LAST"
