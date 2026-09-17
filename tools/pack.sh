#!/usr/bin/env bash
# Pack zpoint into a distributable zip (excludes configs — they're secrets,
# build artifacts, caches and backup files).
# Property of the @ily_bio research channel (@iliyahsatam).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ZP_ROOT="${ZP_ROOT_OVERRIDE:-$SCRIPT_DIR}"
OUT="$ZP_ROOT/zpoint.zip"
cd "$ZP_ROOT"
rm -f "$OUT"
zip -rq "$OUT" . \
  -x "configs/*" -x "*.pyc" -x "*__pycache__*" -x "*/__pycache__/*" \
  -x "zpoint.zip" -x "zpoint.zip.*" -x "*.apk" -x "*.idsig" -x "zpoint-apps.zip" \
  -x "android/.gradle/*" -x "android/app/build/*" -x "android/app/.cxx/*" \
  -x "android/local.properties" -x "android/.idea/*" -x "*/.DS_Store" \
  -x "*.log" -x "*.zip.pre-audit-backup" \
echo "✔ packed: $OUT"
echo "  size: $(du -h "$OUT" | cut -f1)  files: $(unzip -l "$OUT" | tail -1 | awk '{print $2}')"
