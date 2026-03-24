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
MAX_CONFLICTS="${REALTIME_MAX_CONFLICTS:-120}"
MAX_DUPLICATES="${REALTIME_MAX_DUPLICATES:-600}"
SPIKE_MIN_BASE_CONFLICTS="${REALTIME_SPIKE_MIN_BASE_CONFLICTS:-10}"
SPIKE_MIN_BASE_DUPLICATES="${REALTIME_SPIKE_MIN_BASE_DUPLICATES:-30}"
SPIKE_FACTOR_CONFLICTS="${REALTIME_SPIKE_FACTOR_CONFLICTS:-3}"
SPIKE_FACTOR_DUPLICATES="${REALTIME_SPIKE_FACTOR_DUPLICATES:-3}"
SLO_P95_SAVE_ACK_MS="${REALTIME_SLO_P95_SAVE_ACK_MS:-450}"
SLO_P95_BOSS_UPDATE_MS="${REALTIME_SLO_P95_BOSS_UPDATE_MS:-350}"
REALTIME_MAX_DUP_EVENT_DROPPED="${REALTIME_MAX_DUP_EVENT_DROPPED:-200}"
REALTIME_MAX_SEQ_GAP_DETECTED="${REALTIME_MAX_SEQ_GAP_DETECTED:-50}"
REALTIME_MAX_REPLAY_EMPTY="${REALTIME_MAX_REPLAY_EMPTY:-60}"
REALTIME_MAX_REPLAY_CATCHUP_DEPTH="${REALTIME_MAX_REPLAY_CATCHUP_DEPTH:-400}"
REALTIME_MAX_REPLAY_EMPTY_RATIO_PCT="${REALTIME_MAX_REPLAY_EMPTY_RATIO_PCT:-70}"
REALTIME_MIN_REPLAY_REQUESTS_FOR_RATIO_ALERT="${REALTIME_MIN_REPLAY_REQUESTS_FOR_RATIO_ALERT:-10}"

sql="select coalesce(sum(case when event_type='conflict' then 1 else 0 end),0) as conflicts, coalesce(sum(case when event_type='duplicate' then 1 else 0 end),0) as duplicates from public.player_write_events where created_at >= now() - interval '${WINDOW_MINUTES} minutes';"
read -r conflicts duplicates <<< "$(runuser -u postgres -- psql -d "$DB_NAME" -tA -F ' ' -v ON_ERROR_STOP=1 -c "$sql")"

sql_slo="select coalesce(round(percentile_cont(0.95) within group (order by value_ms))::int,0) as p95_save_ack from public.realtime_perf_samples where metric_kind='save_ack' and created_at >= now() - interval '${WINDOW_MINUTES} minutes';"
read -r p95_save_ack <<< "$(runuser -u postgres -- psql -d "$DB_NAME" -tA -F ' ' -v ON_ERROR_STOP=1 -c "$sql_slo" 2>/dev/null || echo '0')"
sql_slo_boss="select coalesce(round(percentile_cont(0.95) within group (order by value_ms))::int,0) as p95_boss_update, coalesce(round(avg(value_ms))::int,0) as avg_boss_update from public.realtime_perf_samples where metric_kind='boss_update' and created_at >= now() - interval '${WINDOW_MINUTES} minutes';"
read -r p95_boss_update avg_boss_update <<< "$(runuser -u postgres -- psql -d "$DB_NAME" -tA -F ' ' -v ON_ERROR_STOP=1 -c "$sql_slo_boss" 2>/dev/null || echo '0 0')"
sql_boss="select coalesce(sum(case when metric_kind='boss_update_apply_live' then metric_value else 0 end),0), coalesce(sum(case when metric_kind='boss_update_apply_replay' then metric_value else 0 end),0), coalesce(sum(case when metric_kind='duplicate_event_dropped' then metric_value else 0 end),0), coalesce(sum(case when metric_kind='seq_gap_detected' then metric_value else 0 end),0), coalesce(sum(case when metric_kind='replay_empty' then metric_value else 0 end),0), coalesce(sum(case when metric_kind='replay_catchup_depth' then metric_value else 0 end),0), coalesce(sum(case when metric_kind='replay_requested_total' then metric_value else 0 end),0), coalesce(sum(case when metric_kind='replay_served_events_total' then metric_value else 0 end),0) from public.realtime_boss_metrics where created_at >= now() - interval '${WINDOW_MINUTES} minutes';"
read -r boss_live_applied boss_replay_applied dup_event_dropped seq_gap_detected replay_empty replay_catchup_depth replay_requested_total replay_served_events_total <<< "$(runuser -u postgres -- psql -d "$DB_NAME" -tA -F ' ' -v ON_ERROR_STOP=1 -c "$sql_boss" 2>/dev/null || echo '0 0 0 0 0 0 0 0')"

sql_prev="select coalesce(sum(case when event_type='conflict' then 1 else 0 end),0) as conflicts, coalesce(sum(case when event_type='duplicate' then 1 else 0 end),0) as duplicates from public.player_write_events where created_at >= now() - interval '$((WINDOW_MINUTES * 2)) minutes' and created_at < now() - interval '${WINDOW_MINUTES} minutes';"
read -r prev_conflicts prev_duplicates <<< "$(runuser -u postgres -- psql -d "$DB_NAME" -tA -F ' ' -v ON_ERROR_STOP=1 -c "$sql_prev")"

conflicts="${conflicts:-0}"
duplicates="${duplicates:-0}"
prev_conflicts="${prev_conflicts:-0}"
prev_duplicates="${prev_duplicates:-0}"
p95_save_ack="${p95_save_ack:-0}"
p95_boss_update="${p95_boss_update:-0}"
avg_boss_update="${avg_boss_update:-0}"
boss_live_applied="${boss_live_applied:-0}"
boss_replay_applied="${boss_replay_applied:-0}"
dup_event_dropped="${dup_event_dropped:-0}"
seq_gap_detected="${seq_gap_detected:-0}"
replay_empty="${replay_empty:-0}"
replay_catchup_depth="${replay_catchup_depth:-0}"
replay_requested_total="${replay_requested_total:-0}"
replay_served_events_total="${replay_served_events_total:-0}"
total_applied=$((boss_live_applied + boss_replay_applied))
replay_rate_pct=0
if [ "$total_applied" -gt 0 ]; then
  replay_rate_pct=$((100 * boss_replay_applied / total_applied))
fi
msg="realtime_metrics window=${WINDOW_MINUTES}m conflicts=${conflicts} duplicates=${duplicates} prev_conflicts=${prev_conflicts} prev_duplicates=${prev_duplicates} p95_save_ack_ms=${p95_save_ack} p95_boss_update_ms=${p95_boss_update} avg_boss_update_ms=${avg_boss_update} replay_rate_pct=${replay_rate_pct} dup_event_dropped=${dup_event_dropped} seq_gap_detected=${seq_gap_detected} replay_empty=${replay_empty} replay_catchup_depth=${replay_catchup_depth} replay_requested_total=${replay_requested_total} replay_served_events_total=${replay_served_events_total}"
echo "[$(date -Is)] ${msg}" >> "$LOG_DIR/health.log"

if [ "$conflicts" -gt "$MAX_CONFLICTS" ] || [ "$duplicates" -gt "$MAX_DUPLICATES" ]; then
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "alert_${msg} threshold_conflicts=${MAX_CONFLICTS} threshold_duplicates=${MAX_DUPLICATES}"
fi

spike_conflicts=0
spike_duplicates=0
if [ "$prev_conflicts" -ge "$SPIKE_MIN_BASE_CONFLICTS" ] && [ "$conflicts" -ge $((prev_conflicts * SPIKE_FACTOR_CONFLICTS)) ]; then
  spike_conflicts=1
fi
if [ "$prev_duplicates" -ge "$SPIKE_MIN_BASE_DUPLICATES" ] && [ "$duplicates" -ge $((prev_duplicates * SPIKE_FACTOR_DUPLICATES)) ]; then
  spike_duplicates=1
fi

if [ "$spike_conflicts" -eq 1 ] || [ "$spike_duplicates" -eq 1 ]; then
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "alert_realtime_spike_${msg} spike_factor_conflicts=${SPIKE_FACTOR_CONFLICTS} spike_factor_duplicates=${SPIKE_FACTOR_DUPLICATES}"
fi

if [ "$p95_save_ack" -gt "$SLO_P95_SAVE_ACK_MS" ] || [ "$p95_boss_update" -gt "$SLO_P95_BOSS_UPDATE_MS" ]; then
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "alert_realtime_slo_${msg} slo_p95_save_ack_ms=${SLO_P95_SAVE_ACK_MS} slo_p95_boss_update_ms=${SLO_P95_BOSS_UPDATE_MS}"
fi

if [ "$dup_event_dropped" -gt "$REALTIME_MAX_DUP_EVENT_DROPPED" ] || [ "$seq_gap_detected" -gt "$REALTIME_MAX_SEQ_GAP_DETECTED" ]; then
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "alert_realtime_boss_stream_${msg} max_dup_event_dropped=${REALTIME_MAX_DUP_EVENT_DROPPED} max_seq_gap_detected=${REALTIME_MAX_SEQ_GAP_DETECTED}"
fi

if [ "$replay_empty" -gt "$REALTIME_MAX_REPLAY_EMPTY" ] || [ "$replay_catchup_depth" -gt "$REALTIME_MAX_REPLAY_CATCHUP_DEPTH" ]; then
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "alert_realtime_replay_${msg} max_replay_empty=${REALTIME_MAX_REPLAY_EMPTY} max_replay_catchup_depth=${REALTIME_MAX_REPLAY_CATCHUP_DEPTH}"
fi

replay_empty_ratio_pct=0
if [ "$replay_requested_total" -gt 0 ]; then
  replay_empty_ratio_pct=$((100 * replay_empty / replay_requested_total))
fi
if [ "$replay_requested_total" -ge "$REALTIME_MIN_REPLAY_REQUESTS_FOR_RATIO_ALERT" ] && [ "$replay_empty_ratio_pct" -gt "$REALTIME_MAX_REPLAY_EMPTY_RATIO_PCT" ]; then
  /opt/bratstvo_koltsa/scripts/notify_telegram.sh "alert_realtime_replay_ratio_${msg} replay_empty_ratio_pct=${replay_empty_ratio_pct} max_replay_empty_ratio_pct=${REALTIME_MAX_REPLAY_EMPTY_RATIO_PCT} min_replay_requests=${REALTIME_MIN_REPLAY_REQUESTS_FOR_RATIO_ALERT}"
fi
