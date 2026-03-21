do $$
begin
  if to_regclass('public.player_armors') is not null then
    execute 'alter table public.player_armors enable row level security';
  end if;

  if to_regclass('public.player_pets') is not null then
    execute 'alter table public.player_pets enable row level security';
  end if;

  if to_regclass('public.player_businesses') is not null then
    execute 'alter table public.player_businesses enable row level security';
  end if;
end;
$$;
