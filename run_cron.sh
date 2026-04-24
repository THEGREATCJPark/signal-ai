#!/bin/bash
set -euo pipefail

cd /home/pineapple/bunjum2/signal

CONFIG="/home/pineapple/bunjum2/signal/discord_export_config.env"
if [ -f "$CONFIG" ]; then
  set -a
  . <(grep -E '^DISCORD_TOKEN=' "$CONFIG")
  set +a
fi

exec /home/pineapple/miniconda3/bin/python3 run_hourly.py
