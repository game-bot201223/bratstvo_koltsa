alter table if exists public.players
  add column if not exists state_version bigint not null default 0;
