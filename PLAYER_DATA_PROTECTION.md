# Player progress protection

Порядок внедрения серверных RPC (GF, награды, энергия, …): **`docs/IMPLEMENTATION_SEQUENCE.md`**.

See `scripts/game_backend.py`: `evaluate_state_wipe_block`, `merge_progression_max`.

Env off-switches: `DISABLE_STATE_WIPE_GUARD`, `DISABLE_PROGRESSION_MAX_MERGE` in `scripts/backend.env.example`.

Backups and WAL are operational; no code prevents admin SQL or disk loss.
