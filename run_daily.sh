#!/usr/bin/env bash
# Signal Discord Digest — daily automated run
# Usage: ./run_daily.sh
# Requires: discord_export_text_only.py, run_digest.py, keys in ~/.config/legal_evidence_rag/keys.env

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

CHANNEL_ID="1365049274068631644"
DAYS_BACK=3

# Calculate after-kst (3 days ago at 00:00:00)
AFTER_KST=$(date -d "-${DAYS_BACK} days" +"%Y-%m-%d 00:00:00")

echo "[1/4] Exporting Discord (last ${DAYS_BACK} days from ${AFTER_KST})..."
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

# Copy to working dir
cp "$EXPORT_FILE" discord_3day_export.txt

# Remove old state to force fresh run
rm -f digest_state.json

echo "[2/4] Running Gemma 4 digest pipeline..."
python3 run_digest.py discord_3day_export.txt --output docs/index.html

echo "[3/4] Committing and pushing..."
git add docs/index.html run_digest.py run_daily.sh .gitignore discord_export_text_only.py
git commit -m "chore: daily digest $(date +%Y-%m-%d)" --allow-empty || true
git push origin HEAD

echo "[4/4] Done! Site: https://thegreatcjpark.github.io/signal-ai/"
