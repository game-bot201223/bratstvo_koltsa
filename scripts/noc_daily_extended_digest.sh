#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="/opt/bratstvo_koltsa/backups"
ENV_FILE="/opt/bratstvo_koltsa/backups/.env"
mkdir -p "$LOG_DIR"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

summary="$(python3 - <<'PY'
import datetime as dt
from collections import Counter
from pathlib import Path

base = Path("/opt/bratstvo_koltsa/backups")
logs = [
    base / "health.log",
    base / "cron_health.log",
    base / "cron_backup.log",
]
now = dt.datetime.now(dt.timezone.utc)
cutoff = now - dt.timedelta(hours=24)

markers = [
    "health_fail",
    "offsite_check_fail",
    "alert_",
    "cleanup_realtime_tables",
    "realtime_metrics",
    "noc_digest",
]
counts = Counter()
tops = Counter()
total = 0

def parse_ts(line: str):
    if not line.startswith("["):
        return None
    right = line.find("]")
    if right <= 1:
        return None
    ts_raw = line[1:right]
    try:
        return dt.datetime.fromisoformat(ts_raw)
    except Exception:
        return None

for p in logs:
    if not p.exists():
        continue
    try:
        for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            ts = parse_ts(ln)
            if ts is None:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=dt.timezone.utc)
            if ts < cutoff:
                continue
            total += 1
            body = ln.split("] ", 1)[1] if "] " in ln else ln
            for m in markers:
                if m in body:
                    counts[m] += 1
            if ("fail" in body) or ("alert_" in body):
                evt = body
                if " file=" in evt:
                    evt = evt.split(" file=", 1)[0]
                if " age_sec=" in evt:
                    evt = evt.split(" age_sec=", 1)[0]
                tops[evt] += 1
    except Exception:
        pass

top_items = tops.most_common(3)
top_str = "none"
if top_items:
    top_str = "; ".join([f"{k[:70]} x{v}" for k, v in top_items])

msg = (
    f"noc_daily_24h total={total} "
    f"health_fail={counts['health_fail']} "
    f"offsite_fail={counts['offsite_check_fail']} "
    f"alerts={counts['alert_']} "
    f"rt_metrics={counts['realtime_metrics']} "
    f"cleanup_runs={counts['cleanup_realtime_tables']} "
    f"hourly_noc={counts['noc_digest']} "
    f"top_errors={top_str}"
)
print(msg[:3500])
PY
)"

echo "[$(date -Is)] ${summary}" >> "$LOG_DIR/health.log"
/opt/bratstvo_koltsa/scripts/notify_telegram.sh "$summary"
