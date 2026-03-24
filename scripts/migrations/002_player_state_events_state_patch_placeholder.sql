-- Future: deterministic replay from the journal.
-- Apply only when server code writes patches after each commit.
--
-- Option A: column on player_state_events
-- alter table public.player_state_events add column if not exists state_patch jsonb;
--
-- Option B: separate table player_state_patches (tg_id, state_version_after, patch jsonb, ...)

SELECT 1;
