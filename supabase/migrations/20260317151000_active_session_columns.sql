do $$
begin
  if to_regclass('public.players') is null then
    raise notice 'players table missing, skipping active_session columns migration';
    return;
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='players' and column_name='active_session_id'
  ) then
    execute 'alter table public.players add column active_session_id text';
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='players' and column_name='active_session_updated_at'
  ) then
    execute 'alter table public.players add column active_session_updated_at timestamptz';
  end if;
end;
$$;
