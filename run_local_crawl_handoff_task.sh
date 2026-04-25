#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR"
exec /home/pineapple/miniconda3/bin/python3 scripts/local_crawl_handoff_gate.py -- \
  /home/pineapple/miniconda3/bin/python3 scripts/dispatch_local_crawl_handoff.py
