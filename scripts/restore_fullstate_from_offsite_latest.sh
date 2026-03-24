#!/usr/bin/env bash
set -euo pipefail

BASE="/opt/bratstvo_koltsa/backups"
ENV_FILE="$BASE/.env"
TMP_DIR="/opt/bratstvo_koltsa/restore_tmp"
STAGE_DIR="/opt/bratstvo_koltsa/restore_stage"

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

if [ -z "${OFFSITE_TARGET:-}" ]; then
  echo "OFFSITE_TARGET is empty" >&2
  exit 1
fi

mkdir -p "$TMP_DIR"
latest_file="$(rclone lsf "$OFFSITE_TARGET/fullstate/" 2>/dev/null | grep '^fullstate_.*\.tar\.gz$' | sort | tail -n1 || true)"
if [ -z "$latest_file" ]; then
  echo "No offsite fullstate archives found" >&2
  exit 1
fi

echo "Downloading fullstate archive: $latest_file"
rclone copy "$OFFSITE_TARGET/fullstate/$latest_file" "$TMP_DIR/" --transfers=1 --checkers=2
rclone copy "$OFFSITE_TARGET/fullstate/${latest_file}.sha256" "$TMP_DIR/" --transfers=1 --checkers=2 || true
gzip -t "$TMP_DIR/$latest_file"
if [ -f "$TMP_DIR/${latest_file}.sha256" ]; then
  (cd "$TMP_DIR" && sha256sum -c "${latest_file}.sha256")
else
  echo "WARN: checksum file not found for $latest_file" >&2
fi

rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR"
echo "Extracting archive into staging: $STAGE_DIR"
tar -xzf "$TMP_DIR/$latest_file" -C "$STAGE_DIR"

for req in etc/nginx etc/letsencrypt opt/bratstvo_koltsa/backend var/www/game; do
  if [ ! -e "$STAGE_DIR/$req" ]; then
    echo "Missing required path in archive stage: $req" >&2
    exit 1
  fi
done

echo "Promoting staged restore into /"
tar -C "$STAGE_DIR" -cf - . | tar -C / -xf -

echo "Reloading services"
systemctl daemon-reload || true
systemctl restart postgresql || true
systemctl restart game-backend.service || true
systemctl restart nginx || true

echo "Fullstate restore completed from: $latest_file"
