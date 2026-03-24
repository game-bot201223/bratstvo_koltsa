create table if not exists public.promo_campaigns (
  id bigserial primary key,
  title text not null default '',
  promo_code text not null references public.promo_codes(code) on delete cascade,
  starts_at timestamptz,
  ends_at timestamptz,
  force_active boolean not null default false,
  active boolean not null default true,
  last_state text not null default 'unknown',
  note text not null default '',
  created_by_tg_id text not null default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists promo_campaigns_active_idx
  on public.promo_campaigns (active, starts_at, ends_at);
