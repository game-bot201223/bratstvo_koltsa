-- Одноразовый gf_commit_v1: после успеха выставляется committed_at; новый бой — через init (upsert сбрасывает колонку).
ALTER TABLE public.player_gf_battle
  ADD COLUMN IF NOT EXISTS committed_at timestamptz;

COMMENT ON COLUMN public.player_gf_battle.committed_at IS 'NULL = активный бой; NOT NULL = gf_commit_v1 уже применён';
