do $$
begin
  if to_regclass('public.players') is null then
    raise notice 'players table missing, skipping active_device_id migration';
    return;
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='players' and column_name='active_device_id'
  ) then
    execute 'alter table public.players add column active_device_id text';
  end if;
end;
$$;

