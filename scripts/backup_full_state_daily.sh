#!/usr/bin/env bash
set -euo pipefail

BASE="/opt/bratstvo_koltsa/backups"
OUT_DIR="$BASE/fullstate"
TS="$(date +%Y%m%d_%H%M%S)"
ARCHIVE="$OUT_DIR/fullstate_${TS}.tar.gz"
SUM_FILE="${ARCHIVE}.sha256"
ENV_FILE="$BASE/.env"

mkdir -p "$OUT_DIR"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

# Export operational metadata
crontab -l > "$tmpdir/root.crontab" 2>/dev/null || true
systemctl list-unit-files --type=service > "$tmpdir/services.txt" || true
runuser -u postgres -- psql -d postgres -At -c "select rolname from pg_roles order by rolname;" > "$tmpdir/pg_roles.txt"

tar -czf "$ARCHIVE" \
  -C / \
  etc/nginx \
  etc/letsencrypt \
  etc/systemd/system/game-backend.service \
  opt/bratstvo_koltsa/backend \
  var/www/game \
  "$tmpdir"
sha256sum "$ARCHIVE" > "$SUM_FILE"

# Keep 14 days of full-state archives.
find "$OUT_DIR" -type f -name "fullstate_*.tar.gz" -mtime +14 -delete
find "$OUT_DIR" -type f -name "fullstate_*.tar.gz.sha256" -mtime +14 -delete

if [ -n "${OFFSITE_TARGET:-}" ] && command -v rclone >/dev/null 2>&1; then
  if ! rclone copy "$ARCHIVE" "$OFFSITE_TARGET/fullstate/" --transfers=1 --checkers=2 --contimeout=15s --timeout=10m; then
    /opt/bratstvo_koltsa/scripts/notify_telegram.sh "fullstate_offsite_failed file=$(basename "$ARCHIVE")"
  fi
  if ! rclone copy "$SUM_FILE" "$OFFSITE_TARGET/fullstate/" --transfers=1 --checkers=2 --contimeout=15s --timeout=2m; then
    /opt/bratstvo_koltsa/scripts/notify_telegram.sh "fullstate_offsite_checksum_upload_failed file=$(basename "$SUM_FILE")"
  fi
fi

echo "[$(date -Is)] fullstate_backup_ok file=$ARCHIVE" >> "$BASE/backup.log"
