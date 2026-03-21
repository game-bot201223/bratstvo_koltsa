do $$
begin
  if to_regclass('public.players') is null then
    raise notice 'players table missing, skipping player sidecar tables migration';
    return;
  end if;

  if to_regclass('public.player_armors') is null then
    execute 'create table public.player_armors (
      tg_id text primary key references public.players(tg_id) on delete cascade,
      armor_owned jsonb not null default ''{}''::jsonb,
      selected_armor_key text,
      main_character_img text,
      updated_at timestamptz not null default now()
    )';
  end if;

  if to_regclass('public.player_pets') is null then
    execute 'create table public.player_pets (
      tg_id text primary key references public.players(tg_id) on delete cascade,
      pets_owned jsonb not null default ''{}''::jsonb,
      active_pet_id integer not null default 0,
      updated_at timestamptz not null default now()
    )';
  end if;

  if to_regclass('public.player_businesses') is null then
    execute 'create table public.player_businesses (
      tg_id text primary key references public.players(tg_id) on delete cascade,
      district_biz_lvls jsonb not null default ''{}''::jsonb,
      district_biz_daily jsonb not null default ''{"day":"","claimed":{}}''::jsonb,
      district_biz_first_purchase_ts bigint not null default 0,
      district_biz_last_claim_ts bigint not null default 0,
      biz_energy_applied bigint not null default 0,
      updated_at timestamptz not null default now()
    )';
  end if;
end;
$$;

insert into public.player_armors (tg_id, armor_owned, selected_armor_key, main_character_img, updated_at)
select
  p.tg_id,
  coalesce(p.state->'armorOwned', '{}'::jsonb),
  nullif(coalesce(p.state->>'selectedArmorKey', ''), ''),
  nullif(coalesce(p.state->>'mainCharacterImg', ''), ''),
  now()
from public.players p
on conflict (tg_id) do update
set
  armor_owned = excluded.armor_owned,
  selected_armor_key = excluded.selected_armor_key,
  main_character_img = excluded.main_character_img,
  updated_at = now();

insert into public.player_pets (tg_id, pets_owned, active_pet_id, updated_at)
select
  p.tg_id,
  coalesce(p.state->'petsOwned', '{}'::jsonb),
  greatest(0, coalesce((p.state->>'activePetId')::integer, 0)),
  now()
from public.players p
on conflict (tg_id) do update
set
  pets_owned = excluded.pets_owned,
  active_pet_id = excluded.active_pet_id,
  updated_at = now();

insert into public.player_businesses (
  tg_id,
  district_biz_lvls,
  district_biz_daily,
  district_biz_first_purchase_ts,
  district_biz_last_claim_ts,
  biz_energy_applied,
  updated_at
)
select
  p.tg_id,
  coalesce(p.state->'districtBizLvls', '{}'::jsonb),
  coalesce(p.state->'districtBizDaily', '{"day":"","claimed":{}}'::jsonb),
  greatest(0, coalesce((p.state->>'districtBizFirstPurchaseTs')::bigint, 0)),
  greatest(0, coalesce((p.state->>'districtBizLastClaimTs')::bigint, 0)),
  greatest(0, coalesce((p.state->>'_bizEnergyApplied')::bigint, 0)),
  now()
from public.players p
on conflict (tg_id) do update
set
  district_biz_lvls = excluded.district_biz_lvls,
  district_biz_daily = excluded.district_biz_daily,
  district_biz_first_purchase_ts = excluded.district_biz_first_purchase_ts,
  district_biz_last_claim_ts = excluded.district_biz_last_claim_ts,
  biz_energy_applied = excluded.biz_energy_applied,
  updated_at = now();
