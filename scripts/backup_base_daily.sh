#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/opt/bratstvo_koltsa/backups/base"
LOG_DIR="/opt/bratstvo_koltsa/backups"
TS="$(date +%Y%m%d_%H%M%S)"
DEST="$BASE_DIR/base_${TS}"
ARCHIVE="${DEST}.tar.gz"
SUM_FILE="${ARCHIVE}.sha256"
ENV_FILE="/opt/bratstvo_koltsa/backups/.env"

mkdir -p "$BASE_DIR" "$LOG_DIR"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

runuser -u postgres -- pg_basebackup -D "$DEST" -Fp -X stream -c fast -R
tar -C "$BASE_DIR" -czf "$ARCHIVE" "base_${TS}"
rm -rf "$DEST"
sha256sum "$ARCHIVE" > "$SUM_FILE"

# Keep 14 days of base backups.
find "$BASE_DIR" -type f -name "base_*.tar.gz" -mtime +14 -delete
find "$BASE_DIR" -type f -name "base_*.tar.gz.sha256" -mtime +14 -delete

if [ -n "${OFFSITE_TARGET:-}" ] && command -v rclone >/dev/null 2>&1; then
  if ! rclone copy "$ARCHIVE" "$OFFSITE_TARGET/base/" --transfers=1 --checkers=2 --contimeout=15s --timeout=5m; then
    /opt/bratstvo_koltsa/scripts/notify_telegram.sh "base_backup_offsite_failed file=$(basename "$ARCHIVE")"
  fi
  if ! rclone copy "$SUM_FILE" "$OFFSITE_TARGET/base/" --transfers=1 --checkers=2 --contimeout=15s --timeout=2m; then
    /opt/bratstvo_koltsa/scripts/notify_telegram.sh "base_backup_checksum_upload_failed file=$(basename "$SUM_FILE")"
  fi
fi

echo "[$(date -Is)] base_backup_ok file=$ARCHIVE" >> "$LOG_DIR/backup.log"
