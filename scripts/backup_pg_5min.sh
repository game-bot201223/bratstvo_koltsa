#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="/opt/bratstvo_koltsa/backups/pgdump"
LOG_DIR="/opt/bratstvo_koltsa/backups"
DB_NAME="gamedb"
TS="$(date +%Y%m%d_%H%M%S)"
OUT="$BACKUP_DIR/gamedb_${TS}.sql.gz"
SUM_FILE="${OUT}.sha256"
ENV_FILE="/opt/bratstvo_koltsa/backups/.env"

mkdir -p "$BACKUP_DIR" "$LOG_DIR"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

runuser -u postgres -- pg_dump "$DB_NAME" | gzip -9 > "$OUT"
sha256sum "$OUT" > "$SUM_FILE"

# Keep 14 days of 5-minute dumps.
find "$BACKUP_DIR" -type f -name "gamedb_*.sql.gz" -mtime +14 -delete
find "$BACKUP_DIR" -type f -name "gamedb_*.sql.gz.sha256" -mtime +14 -delete

# Optional offsite sync (rclone remote, e.g. b2:bucket/path)
if [ -n "${OFFSITE_TARGET:-}" ] && command -v rclone >/dev/null 2>&1; then
  if ! rclone copy "$OUT" "$OFFSITE_TARGET/pgdump/" --transfers=1 --checkers=2 --contimeout=15s --timeout=2m; then
    /opt/bratstvo_koltsa/scripts/notify_telegram.sh "backup_offsite_failed file=$(basename "$OUT")"
  fi
  if ! rclone copy "$SUM_FILE" "$OFFSITE_TARGET/pgdump/" --transfers=1 --checkers=2 --contimeout=15s --timeout=2m; then
    /opt/bratstvo_koltsa/scripts/notify_telegram.sh "backup_offsite_checksum_upload_failed file=$(basename "$SUM_FILE")"
  fi
fi

echo "[$(date -Is)] backup_ok file=$OUT" >> "$LOG_DIR/backup.log"
