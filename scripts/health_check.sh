#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="/opt/bratstvo_koltsa/backups"
ENV_FILE="/opt/bratstvo_koltsa/backups/.env"
mkdir -p "$LOG_DIR"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

if ! systemctl is-active --quiet postgresql; then
  echo "[$(date -Is)] health_fail postgresql_inactive" >> "$LOG_DIR/health.log"
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "health_fail postgresql_inactive"
  exit 1
fi

if ! systemctl is-active --quiet game-backend.service; then
  echo "[$(date -Is)] health_fail game_backend_inactive" >> "$LOG_DIR/health.log"
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "health_fail game_backend_inactive"
  exit 1
fi

if ! curl -fsS http://127.0.0.1:8081/health >/dev/null; then
  echo "[$(date -Is)] health_fail backend_health_endpoint" >> "$LOG_DIR/health.log"
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "health_fail backend_health_endpoint"
  exit 1
fi

if ! curl -fsS https://bratstvokoltsa.com/health >/dev/null; then
  echo "[$(date -Is)] health_fail public_health_endpoint" >> "$LOG_DIR/health.log"
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "health_fail public_health_endpoint"
  exit 1
fi

# Verify recent pg_dump exists (not older than 10 minutes).
latest_dump="$(ls -1t /opt/bratstvo_koltsa/backups/pgdump/gamedb_*.sql.gz 2>/dev/null | head -n 1 || true)"
if [ -z "$latest_dump" ]; then
  echo "[$(date -Is)] health_fail no_recent_dump" >> "$LOG_DIR/health.log"
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "health_fail no_recent_dump"
  exit 1
fi
latest_mtime="$(stat -c %Y "$latest_dump")"
now_ts="$(date +%s)"
if [ $((now_ts - latest_mtime)) -gt 600 ]; then
  echo "[$(date -Is)] health_fail stale_dump file=$(basename "$latest_dump")" >> "$LOG_DIR/health.log"
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "health_fail stale_dump file=$(basename "$latest_dump")"
  exit 1
fi

echo "[$(date -Is)] health_ok" >> "$LOG_DIR/health.log"
