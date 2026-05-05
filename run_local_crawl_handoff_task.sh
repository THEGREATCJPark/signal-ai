#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR"
export PATH="/home/pineapple/bin:/home/pineapple/.local/bin:/home/pineapple/.dotnet:/home/pineapple/.dotnet/tools:/usr/local/bin:/usr/bin:/bin:$PATH"
DEFAULT_PYTHON="/home/pineapple/miniconda3/bin/python3"
if [ -z "${AI_FRONTIER_PYTHON:-}" ]; then
  if [ -x "$DEFAULT_PYTHON" ]; then
    AI_FRONTIER_PYTHON="$DEFAULT_PYTHON"
  else
    AI_FRONTIER_PYTHON="$(command -v python3)"
  fi
fi

CONFIG="${DISCORD_EXPORT_CONFIG:-$SCRIPT_DIR/discord_export_config.env}"
if [ -f "$CONFIG" ]; then
  set -a
  . <(grep -E '^DISCORD_TOKEN=' "$CONFIG")
  set +a
fi

exec "$AI_FRONTIER_PYTHON" scripts/local_crawl_handoff_gate.py -- \
  "$AI_FRONTIER_PYTHON" scripts/dispatch_local_crawl_handoff.py
