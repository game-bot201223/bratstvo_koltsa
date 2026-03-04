-- Admin-only world reset (FULL wipe)
-- WARNING: This truncates core game tables.

create or replace function public.admin_world_reset()
returns json
language plpgsql
security definer
as $$
declare
begin
  -- Truncate known tables if they exist.
  if to_regclass('public.boss_help_events') is not null then
    execute 'truncate table public.boss_help_events restart identity cascade';
  end if;
  if to_regclass('public.boss_last_winners') is not null then
    execute 'truncate table public.boss_last_winners restart identity cascade';
  end if;
  if to_regclass('public.player_boss_fights') is not null then
    execute 'truncate table public.player_boss_fights restart identity cascade';
  end if;
  if to_regclass('public.district_leaders') is not null then
    execute 'truncate table public.district_leaders restart identity cascade';
  end if;
  if to_regclass('public.group_fight_entries') is not null then
    execute 'truncate table public.group_fight_entries restart identity cascade';
  end if;
  if to_regclass('public.clans') is not null then
    execute 'truncate table public.clans restart identity cascade';
  end if;

  -- Full wipe: players last.
  if to_regclass('public.players') is not null then
    execute 'truncate table public.players restart identity cascade';
  end if;

  return json_build_object('ok', true);
end;
$$;

revoke all on function public.admin_world_reset() from public;
grant execute on function public.admin_world_reset() to service_role;
