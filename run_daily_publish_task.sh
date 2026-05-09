#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

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

exec "$AI_FRONTIER_PYTHON" scripts/daily_publish_local.py --platform both --allow-partial
