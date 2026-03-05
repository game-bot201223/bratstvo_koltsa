-- Boss damage audit log (per applied damage).

CREATE TABLE IF NOT EXISTS public.boss_damage_events (
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),

  to_tg_id text NOT NULL,
  boss_id integer NOT NULL,

  dmg_applied bigint NOT NULL,
  hp_before bigint NOT NULL,
  hp_after bigint NOT NULL,
  max_hp bigint NOT NULL,

  source text NOT NULL DEFAULT 'hit',
  from_tg_id text,
  from_name text,
  clan_id text,

  fight_started_at timestamptz NOT NULL
);

ALTER TABLE public.boss_damage_events ENABLE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS boss_damage_events_to_boss_created_idx
  ON public.boss_damage_events (to_tg_id, boss_id, created_at DESC);

CREATE INDEX IF NOT EXISTS boss_damage_events_created_idx
  ON public.boss_damage_events (created_at DESC);

CREATE INDEX IF NOT EXISTS boss_damage_events_source_created_idx
  ON public.boss_damage_events (source, created_at DESC);

ALTER TABLE public.boss_damage_events OWNER TO postgres;

-- Extend apply_boss_damage_v2 to optionally log applied damage.
-- Existing callers remain compatible because added params have defaults.
CREATE OR REPLACE FUNCTION public.apply_boss_damage_v2(
  p_owner_tg_id text,
  p_boss_id integer,
  p_dmg bigint,
  p_max_hp bigint,
  p_expires_at timestamptz DEFAULT NULL,
  p_source text DEFAULT 'hit',
  p_from_tg_id text DEFAULT NULL,
  p_from_name text DEFAULT NULL,
  p_clan_id text DEFAULT NULL
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

  v_hp_before bigint;
  v_reward_claimed_before boolean;
  v_fight_started_at timestamptz;
  v_dmg_applied bigint;
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

  select f.hp, f.reward_claimed
    into v_hp_before, v_reward_claimed_before
  from public.player_boss_fights f
  where f.owner_tg_id = trim(p_owner_tg_id)
    and f.boss_id = p_boss_id;

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

  select f.fight_started_at
    into v_fight_started_at
  from public.player_boss_fights f
  where f.owner_tg_id = trim(p_owner_tg_id)
    and f.boss_id = p_boss_id;

  v_dmg_applied = case
    when coalesce(v_reward_claimed_before, false) then greatest(0, p_max_hp - v_hp)
    else greatest(0, coalesce(v_hp_before, p_max_hp) - v_hp)
  end;

  insert into public.boss_damage_events (
    to_tg_id,
    boss_id,
    dmg_applied,
    hp_before,
    hp_after,
    max_hp,
    source,
    from_tg_id,
    from_name,
    clan_id,
    fight_started_at
  ) values (
    trim(p_owner_tg_id),
    p_boss_id,
    v_dmg_applied,
    coalesce(v_hp_before, p_max_hp),
    v_hp,
    p_max_hp,
    coalesce(nullif(trim(coalesce(p_source, 'hit')), ''), 'hit'),
    nullif(trim(coalesce(p_from_tg_id, '')), ''),
    nullif(trim(coalesce(p_from_name, '')), ''),
    nullif(trim(coalesce(p_clan_id, '')), ''),
    coalesce(v_fight_started_at, now())
  );

  return query select v_owner_tg_id, v_boss_id, v_hp, v_max_hp, v_expires_at, v_reward_claimed;
end;
$$;

GRANT ALL ON FUNCTION public.apply_boss_damage_v2(
  p_owner_tg_id text,
  p_boss_id integer,
  p_dmg bigint,
  p_max_hp bigint,
  p_expires_at timestamptz,
  p_source text,
  p_from_tg_id text,
  p_from_name text,
  p_clan_id text
) TO anon, authenticated, service_role;

-- Keep legacy apply_boss_damage in sync.
CREATE OR REPLACE FUNCTION public.apply_boss_damage(
  p_owner_tg_id text,
  p_boss_id integer,
  p_dmg bigint,
  p_max_hp bigint,
  p_expires_at timestamptz DEFAULT NULL,
  p_source text DEFAULT 'hit',
  p_from_tg_id text DEFAULT NULL,
  p_from_name text DEFAULT NULL,
  p_clan_id text DEFAULT NULL
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

  v_hp_before bigint;
  v_reward_claimed_before boolean;
  v_fight_started_at timestamptz;
  v_dmg_applied bigint;
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

  select f.hp, f.reward_claimed
    into v_hp_before, v_reward_claimed_before
  from public.player_boss_fights f
  where f.owner_tg_id = trim(p_owner_tg_id)
    and f.boss_id = p_boss_id;

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

  select f.fight_started_at
    into v_fight_started_at
  from public.player_boss_fights f
  where f.owner_tg_id = trim(p_owner_tg_id)
    and f.boss_id = p_boss_id;

  v_dmg_applied = case
    when coalesce(v_reward_claimed_before, false) then greatest(0, p_max_hp - v_hp)
    else greatest(0, coalesce(v_hp_before, p_max_hp) - v_hp)
  end;

  insert into public.boss_damage_events (
    to_tg_id,
    boss_id,
    dmg_applied,
    hp_before,
    hp_after,
    max_hp,
    source,
    from_tg_id,
    from_name,
    clan_id,
    fight_started_at
  ) values (
    trim(p_owner_tg_id),
    p_boss_id,
    v_dmg_applied,
    coalesce(v_hp_before, p_max_hp),
    v_hp,
    p_max_hp,
    coalesce(nullif(trim(coalesce(p_source, 'hit')), ''), 'hit'),
    nullif(trim(coalesce(p_from_tg_id, '')), ''),
    nullif(trim(coalesce(p_from_name, '')), ''),
    nullif(trim(coalesce(p_clan_id, '')), ''),
    coalesce(v_fight_started_at, now())
  );

  return query select v_owner_tg_id, v_boss_id, v_hp, v_max_hp, v_expires_at, v_reward_claimed;
end;
$$;

GRANT ALL ON FUNCTION public.apply_boss_damage(
  p_owner_tg_id text,
  p_boss_id integer,
  p_dmg bigint,
  p_max_hp bigint,
  p_expires_at timestamptz,
  p_source text,
  p_from_tg_id text,
  p_from_name text,
  p_clan_id text
) TO anon, authenticated, service_role;
