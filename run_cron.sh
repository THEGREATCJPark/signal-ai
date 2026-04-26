#!/bin/bash
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

CONFIG="${DISCORD_EXPORT_CONFIG:-$SCRIPT_DIR/discord_export_config.env}"
if [ -f "$CONFIG" ]; then
  set -a
  . <(grep -E '^DISCORD_TOKEN=' "$CONFIG")
  set +a
fi

exec "$FIRST_LIGHT_PYTHON" run_hourly.py
