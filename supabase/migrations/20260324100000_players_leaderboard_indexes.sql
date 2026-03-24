-- Leaderboard and admin panel indexes for players table.
-- These significantly speed up ORDER BY queries for top_players_list and admin_list_players.

CREATE INDEX CONCURRENTLY IF NOT EXISTS players_level_updated_idx
  ON public.players (level DESC, updated_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS players_stats_sum_updated_idx
  ON public.players (stats_sum DESC, updated_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS players_boss_wins_updated_idx
  ON public.players (boss_wins DESC, updated_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS players_updated_at_desc_idx
  ON public.players (updated_at DESC);
