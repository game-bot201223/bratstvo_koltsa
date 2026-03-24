#!/usr/bin/env bash
set -euo pipefail

BASE="/opt/bratstvo_koltsa/backups"
ENV_FILE="$BASE/.env"
LOG="$BASE/health.log"

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

if [ -z "${OFFSITE_TARGET:-}" ] || ! command -v rclone >/dev/null 2>&1; then
  echo "[$(date -Is)] offsite_check_skip no_offsite_target" >> "$LOG"
  exit 0
fi

latest_remote="$(rclone lsf "$OFFSITE_TARGET/pgdump/" --format t --separator '|' 2>/dev/null | sort -r | head -n1 || true)"
if [ -z "$latest_remote" ]; then
  echo "[$(date -Is)] offsite_check_fail no_remote_pgdump" >> "$LOG"
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "offsite_check_fail no_remote_pgdump"
  exit 1
fi

# rclone format t gives RFC3339 time prefix before separator
remote_ts="${latest_remote%%|*}"
remote_epoch="$(date -d "$remote_ts" +%s 2>/dev/null || echo 0)"
now_epoch="$(date +%s)"
age_sec=$((now_epoch - remote_epoch))

if [ "$age_sec" -gt 1800 ]; then
  echo "[$(date -Is)] offsite_check_fail stale_remote_pgdump age_sec=$age_sec" >> "$LOG"
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "offsite_check_fail stale_remote_pgdump age_sec=$age_sec"
  exit 1
fi

echo "[$(date -Is)] offsite_check_ok age_sec=$age_sec" >> "$LOG"
