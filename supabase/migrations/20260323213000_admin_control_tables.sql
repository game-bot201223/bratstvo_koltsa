create table if not exists public.admin_player_snapshots (
  id bigserial primary key,
  actor_tg_id text not null,
  target_tg_id text not null,
  state_version bigint not null default 0,
  snapshot jsonb not null,
  note text not null default '',
  created_at timestamptz not null default now()
);

create index if not exists admin_player_snapshots_target_created_idx
  on public.admin_player_snapshots (target_tg_id, created_at desc);

create table if not exists public.admin_audit_log (
  id bigserial primary key,
  actor_tg_id text not null,
  target_tg_id text,
  action text not null,
  details jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists admin_audit_log_target_created_idx
  on public.admin_audit_log (target_tg_id, created_at desc);

create index if not exists admin_audit_log_actor_created_idx
  on public.admin_audit_log (actor_tg_id, created_at desc);
