#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="/opt/bratstvo_koltsa/backups/.env"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

TOKEN="${TELEGRAM_BOT_TOKEN:-}"
IDS="${ADMIN_TG_IDS:-}"
MSG="${1:-event}"

if [ -z "$TOKEN" ] || [ -z "$IDS" ]; then
  exit 0
fi

TEXT="[$(hostname)] $MSG"
IFS=',; ' read -r -a ARR <<< "$IDS"
for chat_id in "${ARR[@]}"; do
  [ -n "$chat_id" ] || continue
  payload="$(python3 - <<'PY' "$chat_id" "$TEXT"
import json, sys
print(json.dumps({"chat_id": str(sys.argv[1]), "text": str(sys.argv[2])}, ensure_ascii=False))
PY
)"
  curl -fsS -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
    -H "Content-Type: application/json" \
    -d "$payload" >/dev/null || true
done
