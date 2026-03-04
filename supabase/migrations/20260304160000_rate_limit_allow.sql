create table if not exists public.rate_limits (
  k text primary key,
  allow_at timestamptz not null
);

alter table public.rate_limits enable row level security;

grant select, insert, update on table public.rate_limits to service_role;

create or replace function public.rate_limit_allow(
  p_key text,
  p_window_ms integer
)
returns table(
  ok boolean,
  allowed boolean,
  next_allow_at timestamptz
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_window interval;
  v_next timestamptz;
  v_allowed boolean := false;
begin
  if p_key is null or length(trim(p_key)) = 0 then
    return query select true, true, v_now;
    return;
  end if;
  if p_window_ms is null or p_window_ms < 0 then
    return query select true, true, v_now;
    return;
  end if;

  v_window := make_interval(secs => (p_window_ms::double precision / 1000.0));

  -- If key exists and is expired: advance allow_at and allow.
  update public.rate_limits
    set allow_at = v_now + v_window
    where k = p_key and allow_at <= v_now
    returning allow_at into v_next;

  if found then
    return query select true, true, v_now;
    return;
  end if;

  -- If key does not exist: insert and allow.
  insert into public.rate_limits (k, allow_at)
    values (p_key, v_now + v_window)
    on conflict (k) do nothing
    returning allow_at into v_next;

  if found then
    return query select true, true, v_now;
    return;
  end if;

  -- Otherwise: key exists and not expired.
  select allow_at into v_next from public.rate_limits where k = p_key;
  return query select true, false, v_next;
end;
$$;

grant execute on function public.rate_limit_allow(text, integer) to service_role;
