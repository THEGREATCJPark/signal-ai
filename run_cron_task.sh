#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR"
DEFAULT_PYTHON="/home/pineapple/miniconda3/bin/python3"
if [ -z "${FIRST_LIGHT_PYTHON:-}" ]; then
  if [ -x "$DEFAULT_PYTHON" ]; then
    FIRST_LIGHT_PYTHON="$DEFAULT_PYTHON"
  else
    FIRST_LIGHT_PYTHON="$(command -v python3)"
  fi
fi
exec "$FIRST_LIGHT_PYTHON" scripts/automation_gate.py -- ./run_cron.sh
