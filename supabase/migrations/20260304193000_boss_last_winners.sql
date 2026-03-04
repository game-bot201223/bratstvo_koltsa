create table if not exists public.boss_last_winners (
  boss_id integer primary key,
  tg_id text not null,
  name text not null,
  photo_url text default ''::text not null,
  updated_at timestamptz default now() not null
);

alter table public.boss_last_winners enable row level security;

drop policy if exists boss_last_winners_select_all on public.boss_last_winners;
create policy boss_last_winners_select_all on public.boss_last_winners
for select using (true);
