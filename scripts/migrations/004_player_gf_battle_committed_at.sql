-- Mirror for scripts/migrations (см. supabase/migrations/20260324120000_player_gf_battle_committed_at.sql)
ALTER TABLE public.player_gf_battle
  ADD COLUMN IF NOT EXISTS committed_at timestamptz;
