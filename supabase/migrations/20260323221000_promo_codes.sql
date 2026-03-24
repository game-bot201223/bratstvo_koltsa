create table if not exists public.promo_codes (
  code text primary key,
  title text not null default '',
  note text not null default '',
  rewards jsonb not null default '{}'::jsonb,
  active boolean not null default true,
  used_total integer not null default 0,
  max_total_uses integer not null default 0,
  max_per_user integer not null default 1,
  starts_at timestamptz,
  ends_at timestamptz,
  created_by_tg_id text not null default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists promo_codes_active_updated_idx
  on public.promo_codes (active, updated_at desc);

create table if not exists public.promo_code_redemptions (
  id bigserial primary key,
  code text not null references public.promo_codes(code) on delete cascade,
  tg_id text not null,
  rewards jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists promo_redemptions_code_created_idx
  on public.promo_code_redemptions (code, created_at desc);

create index if not exists promo_redemptions_tg_created_idx
  on public.promo_code_redemptions (tg_id, created_at desc);
