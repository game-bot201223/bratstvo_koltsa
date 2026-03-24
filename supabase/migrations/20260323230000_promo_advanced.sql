alter table if exists public.promo_codes
  add column if not exists category text not null default 'all',
  add column if not exists target_mode text not null default 'all';

do $$
begin
  if not exists (
    select 1 from information_schema.table_constraints
    where table_schema='public'
      and table_name='promo_codes'
      and constraint_name='promo_codes_category_check'
  ) then
    alter table public.promo_codes
      add constraint promo_codes_category_check check (category in ('all','newbie','vip','clan'));
  end if;
end $$;

do $$
begin
  if not exists (
    select 1 from information_schema.table_constraints
    where table_schema='public'
      and table_name='promo_codes'
      and constraint_name='promo_codes_target_mode_check'
  ) then
    alter table public.promo_codes
      add constraint promo_codes_target_mode_check check (target_mode in ('all','private'));
  end if;
end $$;

create table if not exists public.promo_code_targets (
  code text not null references public.promo_codes(code) on delete cascade,
  tg_id text not null,
  created_at timestamptz not null default now(),
  primary key (code, tg_id)
);

create index if not exists promo_code_targets_tg_idx
  on public.promo_code_targets (tg_id, code);

alter table if exists public.promo_code_redemptions
  add column if not exists source text not null default 'manual',
  add column if not exists event_type text,
  add column if not exists event_key text;

create index if not exists promo_redemptions_source_idx
  on public.promo_code_redemptions (source, created_at desc);

create table if not exists public.promo_auto_rules (
  id bigserial primary key,
  event_type text not null,
  event_key text not null default '',
  promo_code text not null references public.promo_codes(code) on delete cascade,
  active boolean not null default true,
  note text not null default '',
  created_by_tg_id text not null default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

do $$
begin
  if not exists (
    select 1 from information_schema.table_constraints
    where table_schema='public'
      and table_name='promo_auto_rules'
      and constraint_name='promo_auto_rules_event_type_check'
  ) then
    alter table public.promo_auto_rules
      add constraint promo_auto_rules_event_type_check
      check (event_type in ('first_login','boss_win','holiday'));
  end if;
end $$;

create index if not exists promo_auto_rules_event_idx
  on public.promo_auto_rules (event_type, event_key, active);

create table if not exists public.promo_auto_grants (
  id bigserial primary key,
  tg_id text not null,
  promo_code text not null references public.promo_codes(code) on delete cascade,
  event_type text not null,
  event_key text not null default '',
  created_at timestamptz not null default now(),
  unique (tg_id, promo_code, event_type, event_key)
);

create index if not exists promo_auto_grants_event_idx
  on public.promo_auto_grants (event_type, event_key, created_at desc);
