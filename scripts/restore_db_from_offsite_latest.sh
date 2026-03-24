#!/usr/bin/env bash
set -euo pipefail

BASE="/opt/bratstvo_koltsa/backups"
ENV_FILE="$BASE/.env"
DB_NAME="${1:-gamedb}"
TMP_DIR="/opt/bratstvo_koltsa/restore_tmp"
TEST_DB="${DB_NAME}_restore_validate"

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

if [ -z "${OFFSITE_TARGET:-}" ]; then
  echo "OFFSITE_TARGET is empty" >&2
  exit 1
fi

mkdir -p "$TMP_DIR"
latest_file="$(rclone lsf "$OFFSITE_TARGET/pgdump/" 2>/dev/null | grep '^gamedb_.*\.sql\.gz$' | sort | tail -n1 || true)"
if [ -z "$latest_file" ]; then
  echo "No offsite pgdump files found" >&2
  exit 1
fi

echo "Restoring from: $latest_file"
rclone copy "$OFFSITE_TARGET/pgdump/$latest_file" "$TMP_DIR/" --transfers=1 --checkers=2
rclone copy "$OFFSITE_TARGET/pgdump/${latest_file}.sha256" "$TMP_DIR/" --transfers=1 --checkers=2 || true
gzip -t "$TMP_DIR/$latest_file"
if [ -f "$TMP_DIR/${latest_file}.sha256" ]; then
  (cd "$TMP_DIR" && sha256sum -c "${latest_file}.sha256")
else
  echo "WARN: checksum file not found for $latest_file" >&2
fi

runuser -u postgres -- psql -d postgres -c "DROP DATABASE IF EXISTS ${TEST_DB};" >/dev/null
runuser -u postgres -- psql -d postgres -c "CREATE DATABASE ${TEST_DB};" >/dev/null
gunzip -c "$TMP_DIR/$latest_file" | runuser -u postgres -- psql -d "${TEST_DB}" >/dev/null
runuser -u postgres -- psql -d "${TEST_DB}" -v ON_ERROR_STOP=1 -c "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='players';" >/dev/null

runuser -u postgres -- psql -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DB_NAME}' AND pid <> pg_backend_pid();" >/dev/null || true
runuser -u postgres -- psql -d postgres -c "DROP DATABASE IF EXISTS ${DB_NAME};" >/dev/null
runuser -u postgres -- psql -d postgres -c "ALTER DATABASE ${TEST_DB} RENAME TO ${DB_NAME};" >/dev/null

echo "DB restore completed: ${DB_NAME}"
