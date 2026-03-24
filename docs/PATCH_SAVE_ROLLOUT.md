# Patch-save rollout (stages)

Status: `[ ]` todo · `[~]` partial · `[x]` done in repo

## Stage 1 — Whitelist `cloudStateSnapshot` `[x]`
Implemented in `index.html`: only allowed persistent fields; bosses/groupFight/gf1010 sanitized; no full fight blobs; `districtGateBossFight` forced null.

## Stage 2 — Server patch entrypoint `[~]`
`POST /functions/v1/player_patch_v1` in `game_backend.py` — `action: set_player_name` (rate limit + session gate + integrity + events).

## Stage 3 — Disallow routine full-state save `[ ]`
Env gate + `legacy_full` escape for admin/migration.

## Stage 4 — Rename via patch only `[x]`
No full state in rename body; apply server response to local `state`.

## Stage 5 — Cache/sync read-only source of truth `[ ]`
Periodic sync pulls server by `state_version`; no push of full local blob as authority.

## Stage 6–7 — Integrity + suspicious downgrade `[~]`
Server guards exist; client `applyServerStateGuarded` on pull/sync.

## Stage 8 — Mutation queue `[~]`
`enqueuePlayerMutation` in `index.html` — rename uses it; extend to further patch-actions and optionally `serverDoUpsertAsync`.

## Stage 9 — Event log + snapshots `[x]`
`player_state_events`, auto snapshots (existing backend).

**Prod order:** 1 → 4 → 2 (more actions) → 8 → 5 → 3.
