create table if not exists public.player_write_events (
  id bigserial primary key,
  tg_id text not null,
  request_id text not null,
  event_type text not null check (event_type in ('duplicate', 'conflict')),
  created_at timestamptz not null default now()
);

create index if not exists player_write_events_created_at_idx
  on public.player_write_events (created_at);

create index if not exists player_write_events_type_created_at_idx
  on public.player_write_events (event_type, created_at);
