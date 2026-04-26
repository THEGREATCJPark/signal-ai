#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR"
export PATH="/home/pineapple/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
exec /home/pineapple/miniconda3/bin/python3 scripts/local_crawl_handoff_gate.py -- \
  /home/pineapple/miniconda3/bin/python3 scripts/dispatch_local_crawl_handoff.py
