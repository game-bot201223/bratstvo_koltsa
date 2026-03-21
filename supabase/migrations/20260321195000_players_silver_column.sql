do $$
begin
  if to_regclass('public.players') is null then
    raise notice 'players table missing, skipping silver column migration';
    return;
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='players' and column_name='silver'
  ) then
    execute 'alter table public.players add column silver bigint not null default 0';
  end if;
end;
$$;

update public.players
set silver = greatest(0, coalesce((state->>'silver')::bigint, silver, 0));

create index if not exists players_silver_idx on public.players using btree (silver desc);
