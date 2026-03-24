#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="/opt/bratstvo_koltsa/backups"
mkdir -p "$LOG_DIR"

DB_NAME="${GAME_DB_NAME:-gamedb}"
KEEP_OPS_DAYS="${KEEP_OPS_DAYS:-3}"
KEEP_EVENTS_DAYS="${KEEP_EVENTS_DAYS:-14}"

runuser -u postgres -- psql -d "$DB_NAME" -v ON_ERROR_STOP=1 <<SQL
delete from public.player_write_ops
where created_at < now() - interval '${KEEP_OPS_DAYS} days';

delete from public.player_write_events
where created_at < now() - interval '${KEEP_EVENTS_DAYS} days';
SQL

echo "[$(date -Is)] cleanup_realtime_tables_ok keep_ops_days=${KEEP_OPS_DAYS} keep_events_days=${KEEP_EVENTS_DAYS}" >> "$LOG_DIR/health.log"
