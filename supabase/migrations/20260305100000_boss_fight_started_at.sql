-- Add stable fight start anchor for per-fight help cap resets.

ALTER TABLE public.player_boss_fights
ADD COLUMN IF NOT EXISTS fight_started_at timestamptz NOT NULL DEFAULT now();

-- Update apply_boss_damage_v2 to reset fight_started_at when starting a new fight
-- (i.e. after reward_claimed=true, the next hit starts a new fight).
CREATE OR REPLACE FUNCTION public.apply_boss_damage_v2(
  p_owner_tg_id text,
  p_boss_id integer,
  p_dmg bigint,
  p_max_hp bigint,
  p_expires_at timestamptz DEFAULT NULL
)
RETURNS TABLE(
  owner_tg_id text,
  boss_id integer,
  hp bigint,
  max_hp bigint,
  expires_at timestamptz,
  reward_claimed boolean
)
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
declare
  v_owner_tg_id text;
  v_boss_id integer;
  v_hp bigint;
  v_max_hp bigint;
  v_expires_at timestamptz;
  v_reward_claimed boolean;
begin
  if p_owner_tg_id is null or length(trim(p_owner_tg_id)) = 0 then
    raise exception 'bad_owner';
  end if;
  if p_boss_id is null or p_boss_id <= 0 then
    raise exception 'bad_boss_id';
  end if;
  if p_max_hp is null or p_max_hp <= 0 then
    raise exception 'bad_max_hp';
  end if;

  insert into public.player_boss_fights (owner_tg_id, boss_id, hp, max_hp, expires_at, reward_claimed, fight_started_at)
  values (trim(p_owner_tg_id), p_boss_id, p_max_hp, p_max_hp, p_expires_at, false, now())
  on conflict on constraint player_boss_fights_pkey do nothing;

  update public.player_boss_fights f
  set
    hp = case
      when f.reward_claimed then greatest(0, p_max_hp - greatest(0, coalesce(p_dmg, 0)))
      else greatest(0, f.hp - greatest(0, coalesce(p_dmg, 0)))
    end,
    reward_claimed = case when f.reward_claimed then false else f.reward_claimed end,
    max_hp = greatest(f.max_hp, p_max_hp),
    expires_at = coalesce(p_expires_at, f.expires_at),
    fight_started_at = case when f.reward_claimed then now() else f.fight_started_at end,
    updated_at = now()
  where f.owner_tg_id = trim(p_owner_tg_id)
    and f.boss_id = p_boss_id
  returning f.owner_tg_id, f.boss_id, f.hp, f.max_hp, f.expires_at, f.reward_claimed
  into v_owner_tg_id, v_boss_id, v_hp, v_max_hp, v_expires_at, v_reward_claimed;

  return query select v_owner_tg_id, v_boss_id, v_hp, v_max_hp, v_expires_at, v_reward_claimed;
end;
$$;

GRANT ALL ON FUNCTION public.apply_boss_damage_v2(
  p_owner_tg_id text,
  p_boss_id integer,
  p_dmg bigint,
  p_max_hp bigint,
  p_expires_at timestamptz
) TO anon, authenticated, service_role;

-- Keep legacy apply_boss_damage in sync.
CREATE OR REPLACE FUNCTION public.apply_boss_damage(
  p_owner_tg_id text,
  p_boss_id integer,
  p_dmg bigint,
  p_max_hp bigint,
  p_expires_at timestamptz DEFAULT NULL
)
RETURNS TABLE(
  owner_tg_id text,
  boss_id integer,
  hp bigint,
  max_hp bigint,
  expires_at timestamptz,
  reward_claimed boolean
)
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
declare
  v_owner_tg_id text;
  v_boss_id integer;
  v_hp bigint;
  v_max_hp bigint;
  v_expires_at timestamptz;
  v_reward_claimed boolean;
begin
  if p_owner_tg_id is null or length(trim(p_owner_tg_id)) = 0 then
    raise exception 'bad_owner';
  end if;
  if p_boss_id is null or p_boss_id <= 0 then
    raise exception 'bad_boss_id';
  end if;
  if p_max_hp is null or p_max_hp <= 0 then
    raise exception 'bad_max_hp';
  end if;

  insert into public.player_boss_fights (owner_tg_id, boss_id, hp, max_hp, expires_at, reward_claimed, fight_started_at)
  values (trim(p_owner_tg_id), p_boss_id, p_max_hp, p_max_hp, p_expires_at, false, now())
  on conflict on constraint player_boss_fights_pkey do nothing;

  update public.player_boss_fights f
  set
    hp = case
      when f.reward_claimed then greatest(0, p_max_hp - greatest(0, coalesce(p_dmg, 0)))
      else greatest(0, f.hp - greatest(0, coalesce(p_dmg, 0)))
    end,
    reward_claimed = case when f.reward_claimed then false else f.reward_claimed end,
    max_hp = greatest(f.max_hp, p_max_hp),
    expires_at = coalesce(p_expires_at, f.expires_at),
    fight_started_at = case when f.reward_claimed then now() else f.fight_started_at end,
    updated_at = now()
  where f.owner_tg_id = trim(p_owner_tg_id)
    and f.boss_id = p_boss_id
  returning f.owner_tg_id, f.boss_id, f.hp, f.max_hp, f.expires_at, f.reward_claimed
  into v_owner_tg_id, v_boss_id, v_hp, v_max_hp, v_expires_at, v_reward_claimed;

  return query select v_owner_tg_id, v_boss_id, v_hp, v_max_hp, v_expires_at, v_reward_claimed;
end;
$$;
