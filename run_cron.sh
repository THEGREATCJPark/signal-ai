#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

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
  TOKEN_ENV="$(mktemp)"
  grep -E '^DISCORD_TOKEN=' "$CONFIG" > "$TOKEN_ENV" || true
  set -a
  . "$TOKEN_ENV"
  set +a
  rm -f "$TOKEN_ENV"
fi

exec "$AI_FRONTIER_PYTHON" run_hourly.py
