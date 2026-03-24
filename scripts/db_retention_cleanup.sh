#!/usr/bin/env bash
# Retention cleanup: delete old rows from append-only tables.
# Run daily via cron: 0 4 * * * /opt/game/scripts/db_retention_cleanup.sh
set -euo pipefail

RETENTION_DAYS="${RETENTION_DAYS:-7}"
DB="${GAME_DB_NAME:-gamedb}"
LOG_TAG="db_retention"

run_sql() {
  sudo -u postgres psql -d "$DB" -c "$1" 2>&1
}

echo "[$(date -Iseconds)] $LOG_TAG: starting cleanup (retention=${RETENTION_DAYS}d)"

TABLES=(
  "player_write_ops"
  "player_write_events"
  "realtime_perf_samples"
  "realtime_boss_metrics"
  "boss_damage_events"
  "telegram_webhook_updates"
)

for tbl in "${TABLES[@]}"; do
  result=$(run_sql "DELETE FROM public.${tbl} WHERE created_at < now() - interval '${RETENTION_DAYS} days';" 2>&1) || true
  echo "[$(date -Iseconds)] $LOG_TAG: ${tbl}: ${result}"
done

echo "[$(date -Iseconds)] $LOG_TAG: cleanup complete"
