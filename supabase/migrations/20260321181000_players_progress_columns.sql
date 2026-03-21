do $$
begin
  if to_regclass('public.players') is null then
    raise notice 'players table missing, skipping progress columns migration';
    return;
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='players' and column_name='xp'
  ) then
    execute 'alter table public.players add column xp bigint not null default 0';
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='players' and column_name='gold'
  ) then
    execute 'alter table public.players add column gold bigint not null default 0';
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='players' and column_name='silver'
  ) then
    execute 'alter table public.players add column silver bigint not null default 0';
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='players' and column_name='tooth'
  ) then
    execute 'alter table public.players add column tooth bigint not null default 0';
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='players' and column_name='district_fear_total'
  ) then
    execute 'alter table public.players add column district_fear_total bigint not null default 0';
  end if;
end;
$$;

update public.players
set
  xp = greatest(0, coalesce((state->>'totalXp')::bigint, (state->>'xp')::bigint, xp, 0)),
  gold = greatest(0, coalesce((state->>'gold')::bigint, gold, 0)),
  silver = greatest(0, coalesce((state->>'silver')::bigint, silver, 0)),
  tooth = greatest(0, coalesce((state->>'tooth')::bigint, tooth, 0)),
  district_fear_total = greatest(
    0,
    coalesce(
      (
        select sum(greatest(0, value::bigint))
        from jsonb_each_text(coalesce(state->'districtFear', '{}'::jsonb)) as df(key, value)
      ),
      district_fear_total,
      0
    )
  );

create index if not exists players_xp_idx on public.players using btree (xp desc);
create index if not exists players_gold_idx on public.players using btree (gold desc);
create index if not exists players_silver_idx on public.players using btree (silver desc);
create index if not exists players_tooth_idx on public.players using btree (tooth desc);
create index if not exists players_district_fear_total_idx on public.players using btree (district_fear_total desc);
