create table if not exists public.player_write_ops (
  tg_id text not null,
  request_id text not null,
  state_version bigint not null,
  created_at timestamptz not null default now(),
  primary key (tg_id, request_id)
);

create index if not exists player_write_ops_created_at_idx
  on public.player_write_ops (created_at);
