#!/usr/bin/env bash
set -euo pipefail

MIG_DIR="/opt/bratstvo_koltsa/supabase/migrations"
DB_NAME="${1:-gamedb}"

if [ ! -d "$MIG_DIR" ]; then
  echo "Migration directory not found: $MIG_DIR" >&2
  exit 1
fi

count=0
for f in "$MIG_DIR"/*.sql; do
  [ -e "$f" ] || continue
  count=$((count+1))
done

echo "Applying $count migration files to $DB_NAME"

runuser -u postgres -- psql -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
create table if not exists public.schema_migrations (
  filename text primary key,
  applied_at timestamptz not null default now()
);
SQL

applied_count="$(runuser -u postgres -- psql -d "$DB_NAME" -tA -v ON_ERROR_STOP=1 -c "select count(*) from public.schema_migrations;")"
players_exists="$(runuser -u postgres -- psql -d "$DB_NAME" -tA -v ON_ERROR_STOP=1 -c "select count(*) from information_schema.tables where table_schema='public' and table_name='players';")"
if [ "${applied_count:-0}" = "0" ] && [ "${players_exists:-0}" != "0" ]; then
  echo "Existing schema detected; bootstrapping schema_migrations history"
  for f in "$MIG_DIR"/*.sql; do
    [ -e "$f" ] || continue
    bn="$(basename "$f")"
    runuser -u postgres -- psql -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "insert into public.schema_migrations(filename) values ('${bn}') on conflict do nothing;" >/dev/null
  done
  echo "MIGRATIONS_BOOTSTRAPPED"
  exit 0
fi

for f in "$MIG_DIR"/*.sql; do
  [ -e "$f" ] || continue
  bn="$(basename "$f")"
  already="$(runuser -u postgres -- psql -d "$DB_NAME" -tA -v ON_ERROR_STOP=1 -c "select 1 from public.schema_migrations where filename='${bn}' limit 1;")"
  if [ "$already" = "1" ]; then
    echo "==> ${bn} (skip, already applied)"
    continue
  fi
  echo "==> ${bn}"
  runuser -u postgres -- psql -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$f" >/dev/null
  runuser -u postgres -- psql -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "insert into public.schema_migrations(filename) values ('${bn}') on conflict do nothing;" >/dev/null
done

echo "MIGRATIONS_OK"
