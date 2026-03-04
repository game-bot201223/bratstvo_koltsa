create table if not exists public.district_daily_leaders (
  day text not null,
  district_key text not null,
  tg_id text not null,
  name text not null,
  fear bigint not null default 0,
  photo_url text,
  updated_at timestamptz not null default now(),
  primary key (day, district_key)
);

alter table public.district_daily_leaders enable row level security;

drop policy if exists district_daily_leaders_select_all on public.district_daily_leaders;
create policy district_daily_leaders_select_all on public.district_daily_leaders
for select using (true);
