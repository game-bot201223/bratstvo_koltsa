#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="/opt/bratstvo_koltsa/backups"
ENV_FILE="/opt/bratstvo_koltsa/backups/.env"
mkdir -p "$LOG_DIR"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

DB_NAME="${GAME_DB_NAME:-gamedb}"
WINDOW_MINUTES="${REALTIME_METRICS_WINDOW_MINUTES:-15}"

svc_pg="DOWN"
svc_be_a="DOWN"
svc_be_b="DOWN"
api_local="DOWN"
api_public="DOWN"

if systemctl is-active --quiet postgresql; then svc_pg="UP"; fi
if systemctl is-active --quiet game-backend.service; then svc_be_a="UP"; fi
if systemctl is-active --quiet game-backend-b.service; then svc_be_b="UP"; fi
if curl -fsS http://127.0.0.1:8081/health >/dev/null 2>&1; then api_local="UP"; fi
if curl -fsS https://bratstvokoltsa.com/health >/dev/null 2>&1; then api_public="UP"; fi

latest_dump="$(ls -1t /opt/bratstvo_koltsa/backups/pgdump/gamedb_*.sql.gz 2>/dev/null | head -n 1 || true)"
dump_age_sec=-1
if [ -n "$latest_dump" ]; then
  latest_mtime="$(stat -c %Y "$latest_dump" 2>/dev/null || echo 0)"
  now_ts="$(date +%s)"
  dump_age_sec=$((now_ts - latest_mtime))
fi

offsite_age_sec=-1
if [ -n "${OFFSITE_TARGET:-}" ] && command -v rclone >/dev/null 2>&1; then
  latest_remote="$(rclone lsf "$OFFSITE_TARGET/pgdump/" --format t --separator '|' 2>/dev/null | sort -r | head -n1 || true)"
  if [ -n "$latest_remote" ]; then
    remote_ts="${latest_remote%%|*}"
    remote_epoch="$(date -d "$remote_ts" +%s 2>/dev/null || echo 0)"
    now_epoch="$(date +%s)"
    if [ "$remote_epoch" -gt 0 ]; then
      offsite_age_sec=$((now_epoch - remote_epoch))
    fi
  fi
fi

sql_rt="select coalesce(sum(case when event_type='conflict' then 1 else 0 end),0), coalesce(sum(case when event_type='duplicate' then 1 else 0 end),0) from public.player_write_events where created_at >= now() - interval '${WINDOW_MINUTES} minutes';"
read -r rt_conflicts rt_duplicates <<< "$(runuser -u postgres -- psql -d "$DB_NAME" -tA -F ' ' -v ON_ERROR_STOP=1 -c "$sql_rt" 2>/dev/null || echo '0 0')"
rt_conflicts="${rt_conflicts:-0}"
rt_duplicates="${rt_duplicates:-0}"

status="OK"
if [ "$svc_pg" != "UP" ] || [ "$svc_be_a" != "UP" ] || [ "$svc_be_b" != "UP" ] || [ "$api_local" != "UP" ] || [ "$api_public" != "UP" ]; then
  status="WARN"
fi
if [ "$dump_age_sec" -lt 0 ] || [ "$dump_age_sec" -gt 900 ]; then
  status="WARN"
fi
if [ "$offsite_age_sec" -ge 0 ] && [ "$offsite_age_sec" -gt 1800 ]; then
  status="WARN"
fi

msg="noc_digest status=${status} svc_pg=${svc_pg} be_a=${svc_be_a} be_b=${svc_be_b} api_local=${api_local} api_public=${api_public} dump_age_sec=${dump_age_sec} offsite_age_sec=${offsite_age_sec} rt_window=${WINDOW_MINUTES}m rt_conflicts=${rt_conflicts} rt_duplicates=${rt_duplicates}"
echo "[$(date -Is)] ${msg}" >> "$LOG_DIR/health.log"
/opt/bratstvo_koltsa/scripts/notify_telegram.sh "$msg"
