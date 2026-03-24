-- Авторитетное состояние группового боя на сервере (Python game_backend + player_game_action_v1).
-- Один ряд на игрока: tg_id PK. Эпоха battle_epoch — optimistic lock для gb_* действий.

CREATE TABLE IF NOT EXISTS public.player_gf_battle (
  tg_id text PRIMARY KEY,
  start_ts timestamptz NOT NULL,
  battle_epoch integer NOT NULL DEFAULT 1,
  battle jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS player_gf_battle_start_ts_idx ON public.player_gf_battle (start_ts);

COMMENT ON TABLE public.player_gf_battle IS 'Server-owned group fight battle JSON + epoch; enabled when GF_SERVER_BATTLE_PRIMARY=1';
