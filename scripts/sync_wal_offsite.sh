#!/usr/bin/env bash
set -euo pipefail

BASE="/opt/bratstvo_koltsa/backups"
WAL_DIR="$BASE/wal"
ENV_FILE="$BASE/.env"

mkdir -p "$WAL_DIR"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

# Keep 14 days of WAL locally.
find "$WAL_DIR" -type f -mtime +14 -delete

if [ -z "${OFFSITE_TARGET:-}" ] || ! command -v rclone >/dev/null 2>&1; then
  exit 0
fi

if ! rclone sync "$WAL_DIR" "$OFFSITE_TARGET/wal/" --transfers=2 --checkers=4 --contimeout=15s --timeout=10m; then
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "wal_offsite_sync_failed"
  exit 1
fi

echo "[$(date -Is)] wal_offsite_sync_ok" >> "$BASE/backup.log"
