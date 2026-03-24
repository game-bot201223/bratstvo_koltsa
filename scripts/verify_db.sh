#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${1:-gamedb}"

runuser -u postgres -- psql -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
\echo 'players column count:'
select count(*) from information_schema.columns
where table_schema='public' and table_name='players';

\echo 'critical players columns:'
select column_name from information_schema.columns
where table_schema='public'
  and table_name='players'
  and column_name in ('tg_id','state','xp','gold','silver','tooth','active_session_id','active_device_id')
order by column_name;
SQL
