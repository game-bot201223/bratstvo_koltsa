-- Fix claim_boss_reward: return columns renamed to avoid ambiguity with
-- table column "reward_claimed" in UPDATE (PG 42702). Drop + Create needed.

DROP FUNCTION IF EXISTS public.claim_boss_reward(text, integer);

CREATE FUNCTION public.claim_boss_reward(
  p_owner_tg_id text,
  p_boss_id integer
)
RETURNS TABLE(r_ok boolean, r_hp bigint, r_reward_claimed boolean)
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
declare
  v_hp bigint;
  v_claimed boolean;
begin
  if p_owner_tg_id is null or length(trim(p_owner_tg_id)) = 0 then
    raise exception 'bad_owner';
  end if;
  if p_boss_id is null or p_boss_id <= 0 then
    raise exception 'bad_boss_id';
  end if;

  select f.hp, f.reward_claimed
  into v_hp, v_claimed
  from public.player_boss_fights f
  where f.owner_tg_id = trim(p_owner_tg_id)
    and f.boss_id = p_boss_id
  for update;

  if v_hp is null then
    return query select false, null::bigint, false;
    return;
  end if;

  if v_claimed then
    return query select false, v_hp, true;
    return;
  end if;

  if v_hp > 0 then
    return query select false, v_hp, false;
    return;
  end if;

  update public.player_boss_fights
  set reward_claimed = true, updated_at = now()
  where owner_tg_id = trim(p_owner_tg_id)
    and boss_id = p_boss_id;

  return query select true, v_hp, true;
end;
$$;

GRANT EXECUTE ON FUNCTION public.claim_boss_reward(text, integer) TO anon, authenticated, service_role;
