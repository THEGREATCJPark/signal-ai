#!/usr/bin/env bash
# Signal Discord Digest — full automated pipeline
# Discord 수집 → 청크 분석 → 헤드라인 → 한국어 기사 → HTML → gist 배포
#
# Usage: ./run_daily.sh
# Requires: discord_export_text_only.py, run_digest.py, reprocess_headlines.py
#           keys in ~/.config/legal_evidence_rag/keys.env

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

CHANNEL_ID="1365049274068631644"
DAYS_BACK=3
GIST_ID="a9a6b3f417be5221efd2969fe8da85ed"

# Calculate after-kst (3 days ago at 00:00:00)
AFTER_KST=$(date -d "-${DAYS_BACK} days" +"%Y-%m-%d 00:00:00")

echo "[1/5] Exporting Discord (last ${DAYS_BACK} days from ${AFTER_KST})..."
python3 discord_export_text_only.py \
  --channel "$CHANNEL_ID" \
  --after-kst "$AFTER_KST" \
  --no-upload

# Find the latest export file
EXPORT_FILE=$(ls -t /mnt/d/Downloads/general_*.txt 2>/dev/null | head -1)
if [ -z "$EXPORT_FILE" ]; then
  echo "ERROR: No export file found" >&2
  exit 1
fi
echo "  Export: $EXPORT_FILE"
cp "$EXPORT_FILE" discord_3day_export.txt

# Remove old state to force fresh run
rm -f digest_state.json headline_state.json

echo "[2/5] Phase 1: Chunking + per-chunk Gemma 4 analysis..."
python3 run_digest.py discord_3day_export.txt --output docs/index.html

echo "[3/5] Phase 2: Headlines + Korean article generation..."
python3 reprocess_headlines.py

echo "[4/5] Deploying to gist..."
if command -v gh &>/dev/null && [ -n "$GIST_ID" ]; then
  gh gist edit "$GIST_ID" docs/index.html 2>/dev/null && \
    echo "  Gist updated: https://htmlpreview.github.io/?https://gist.githubusercontent.com/pineapplesour/${GIST_ID}/raw/index.html" || \
    echo "  Gist update failed (non-fatal)"
fi

echo "[5/5] Committing and pushing to branch..."
git add docs/index.html run_digest.py run_daily.sh reprocess_headlines.py .gitignore discord_export_text_only.py nitter_crawler.html
git commit -m "chore: daily digest $(date +%Y-%m-%d)" --allow-empty || true
git push origin HEAD || echo "  Push failed (non-fatal)"

echo ""
echo "=== Done ==="
echo "View: https://htmlpreview.github.io/?https://gist.githubusercontent.com/pineapplesour/${GIST_ID}/raw/index.html"
