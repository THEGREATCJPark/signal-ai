#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
INPUT_PATH="${1:-$ROOT_DIR/../references/recall_engine_대화기록_전체.md}"
OUT_DIR="${2:-$ROOT_DIR/runtime_output}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
python3 "$ROOT_DIR/memory_builder.py" --input "$INPUT_PATH" --out-dir "$OUT_DIR"
