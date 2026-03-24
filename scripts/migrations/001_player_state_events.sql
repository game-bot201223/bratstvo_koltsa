-- Опционально: таблица также создаётся при старте game_backend (ensure_player_audit_schema).
CREATE TABLE IF NOT EXISTS public.player_state_events (
  id bigserial PRIMARY KEY,
  tg_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  action_type text NOT NULL DEFAULT 'save',
  endpoint text,
  request_id text,
  state_version_after int,
  client_reason text,
  summary jsonb,
  action_payload jsonb
);
CREATE INDEX IF NOT EXISTS idx_player_state_events_tg_created
  ON public.player_state_events (tg_id, created_at DESC);
