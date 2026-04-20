#!/bin/bash
set -euo pipefail

cd /home/pineapple/bunjum2/signal

ENV_FILE="/home/pineapple/bunjum2/signal/.env"
if [ -f "$ENV_FILE" ]; then
  set -a
  . <(grep -E '^(SUPABASE_URL|SUPABASE_ANON_KEY|SUPABASE_SERVICE_ROLE_KEY)=' "$ENV_FILE" || true)
  set +a
fi

CONFIG="/home/pineapple/bunjum2/signal/discord_export_config.env"
if [ -f "$CONFIG" ]; then
  set -a
  . <(grep -E '^DISCORD_TOKEN=' "$CONFIG")
  set +a
fi

exec /home/pineapple/miniconda3/bin/python3 run_hourly.py
