#!/usr/bin/env bash
set -euo pipefail

BASE="/opt/bratstvo_koltsa/backups"
ENV_FILE="$BASE/.env"
TMP_DIR="/opt/bratstvo_koltsa/dr_test_tmp"
TEST_DB="gamedb_dr_offsite_test"
LOG="$BASE/backup.log"

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

if [ -z "${OFFSITE_TARGET:-}" ]; then
  echo "[$(date -Is)] dr_offsite_test_fail no_offsite_target" >> "$LOG"
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "dr_offsite_test_fail no_offsite_target"
  exit 1
fi

cleanup() {
  runuser -u postgres -- psql -d postgres -c "DROP DATABASE IF EXISTS ${TEST_DB};" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$TMP_DIR"

latest_sql="$(rclone lsf "$OFFSITE_TARGET/pgdump/" 2>/dev/null | awk '/^gamedb_.*\.sql\.gz$/ {print}' | sort | tail -n1 || true)"
if [ -z "$latest_sql" ]; then
  echo "[$(date -Is)] dr_offsite_test_fail no_offsite_sql" >> "$LOG"
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "dr_offsite_test_fail no_offsite_sql"
  exit 1
fi

rclone copy "$OFFSITE_TARGET/pgdump/$latest_sql" "$TMP_DIR/" --transfers=1 --checkers=2
rclone copy "$OFFSITE_TARGET/pgdump/${latest_sql}.sha256" "$TMP_DIR/" --transfers=1 --checkers=2 || true
gzip -t "$TMP_DIR/$latest_sql"
if [ -f "$TMP_DIR/${latest_sql}.sha256" ]; then
  (cd "$TMP_DIR" && sha256sum -c "${latest_sql}.sha256")
fi

runuser -u postgres -- psql -d postgres -c "DROP DATABASE IF EXISTS ${TEST_DB};" >/dev/null
runuser -u postgres -- psql -d postgres -c "CREATE DATABASE ${TEST_DB};" >/dev/null
gunzip -c "$TMP_DIR/$latest_sql" | runuser -u postgres -- psql -d "$TEST_DB" >/dev/null
runuser -u postgres -- psql -d "$TEST_DB" -v ON_ERROR_STOP=1 -c "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='players';" >/dev/null
runuser -u postgres -- psql -d "$TEST_DB" -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.players;" >/dev/null

latest_full="$(rclone lsf "$OFFSITE_TARGET/fullstate/" 2>/dev/null | awk '/^fullstate_.*\.tar\.gz$/ {print}' | sort | tail -n1 || true)"
if [ -n "$latest_full" ]; then
  rclone copy "$OFFSITE_TARGET/fullstate/$latest_full" "$TMP_DIR/" --transfers=1 --checkers=2
  rclone copy "$OFFSITE_TARGET/fullstate/${latest_full}.sha256" "$TMP_DIR/" --transfers=1 --checkers=2 || true
  gzip -t "$TMP_DIR/$latest_full"
  if [ -f "$TMP_DIR/${latest_full}.sha256" ]; then
    (cd "$TMP_DIR" && sha256sum -c "${latest_full}.sha256")
  fi
  tar -tzf "$TMP_DIR/$latest_full" >/dev/null
fi

echo "[$(date -Is)] dr_offsite_test_ok sql=$latest_sql fullstate=${latest_full:-none}" >> "$LOG"
/opt/bratstvo_koltsa/scripts/notify_telegram.sh "dr_offsite_test_ok sql=$latest_sql fullstate=${latest_full:-none}"
