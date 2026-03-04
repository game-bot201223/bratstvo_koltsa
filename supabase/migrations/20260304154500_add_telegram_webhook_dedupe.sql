-- Dedupe Telegram webhook updates by update_id to make webhook handling idempotent.

create table if not exists public.telegram_webhook_updates (
  update_id bigint primary key,
  received_at timestamptz not null default now(),
  body jsonb
);

alter table public.telegram_webhook_updates enable row level security;

-- Service role can insert/select; anon has no access.
grant select, insert on table public.telegram_webhook_updates to service_role;
