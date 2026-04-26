#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR"
export PATH="/home/pineapple/bin:/home/pineapple/.local/bin:/home/pineapple/.dotnet:/home/pineapple/.dotnet/tools:/usr/local/bin:/usr/bin:/bin:$PATH"
DEFAULT_PYTHON="/home/pineapple/miniconda3/bin/python3"
if [ -z "${FIRST_LIGHT_PYTHON:-}" ]; then
  if [ -x "$DEFAULT_PYTHON" ]; then
    FIRST_LIGHT_PYTHON="$DEFAULT_PYTHON"
  else
    FIRST_LIGHT_PYTHON="$(command -v python3)"
  fi
fi
exec "$FIRST_LIGHT_PYTHON" scripts/local_crawl_handoff_gate.py -- \
  "$FIRST_LIGHT_PYTHON" scripts/dispatch_local_crawl_handoff.py
