#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="/opt/bratstvo_koltsa/backups/pgdump"
LOG_DIR="/opt/bratstvo_koltsa/backups"
TEST_DB="gamedb_restore_test"
ENV_FILE="/opt/bratstvo_koltsa/backups/.env"

cleanup() {
  runuser -u postgres -- psql -d postgres -c "DROP DATABASE IF EXISTS $TEST_DB;" >/dev/null || true
}
trap cleanup EXIT

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

LATEST="$(ls -1t "$BACKUP_DIR"/gamedb_*.sql.gz 2>/dev/null | head -n 1 || true)"
if [ -z "$LATEST" ]; then
  echo "[$(date -Is)] restore_test_skip no_backup_files" >> "$LOG_DIR/backup.log"
  exit 0
fi

runuser -u postgres -- psql -d postgres -c "DROP DATABASE IF EXISTS $TEST_DB;" >/dev/null
runuser -u postgres -- psql -d postgres -c "CREATE DATABASE $TEST_DB;" >/dev/null
gunzip -c "$LATEST" | runuser -u postgres -- psql -d "$TEST_DB" >/dev/null

# Minimal integrity checks.
runuser -u postgres -- psql -d "$TEST_DB" -v ON_ERROR_STOP=1 -c "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='players';" >/dev/null
runuser -u postgres -- psql -d "$TEST_DB" -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.players;" >/dev/null

runuser -u postgres -- psql -d postgres -c "DROP DATABASE $TEST_DB;" >/dev/null
echo "[$(date -Is)] restore_test_ok source=$LATEST" >> "$LOG_DIR/backup.log"
/opt/bratstvo_koltsa/scripts/notify_telegram.sh "restore_test_ok source=$(basename "$LATEST")"
