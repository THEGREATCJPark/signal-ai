#!/usr/bin/env bash
set -u

LOG_FILE="/tmp/signal_daily.log"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

main() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] task wrapper start"
  cd "$SCRIPT_DIR"
  ./run_cron.sh
  local rc=$?
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] task wrapper exit=$rc"
  return "$rc"
}

main >> "$LOG_FILE" 2>&1
