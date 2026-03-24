# Run from repo root: python scripts/apply_gf_rpc_phase1_patch.py
# Patches scripts/game_backend.py: GF RPC aliases + gf_server_battle_logic import.
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GB = ROOT / "scripts" / "game_backend.py"

IMPORT_BLOCK = '''import psycopg2
from aiohttp import web

from gf_server_battle_logic import (
    apply_server_battle_init,
    battle_row_update,
    prepare_gb_action_body,
    server_gf_actions_enabled,
)

DB_DSN'''

NEW_SYNC_START = '''def player_game_action_sync(tg_id: str, user: dict, body: dict, client_ip: str | None) -> dict:
    client_action = str(body.get("action") or "").strip()
    _GF_NEW_RPC = frozenset({"create_gf_session_v1", "gf_action_v1", "gf_commit_v1"})
    if client_action in _GF_NEW_RPC and not server_gf_actions_enabled():
        return {"ok": False, "error": "gf_server_actions_disabled", "http_status": 403, "hint": "set_GF_SERVER_ACTIONS_ENABLED=1"}
    if client_action == "gf_commit_v1":
        return {"ok": False, "error": "not_implemented", "http_status": 501, "hint": "gf_commit_v1_wip"}

    mutate_action = client_action
    if client_action == "create_gf_session_v1":
        mutate_action = "gf_server_battle_init"
    elif client_action == "gf_action_v1":
        _op = str(body.get("op") or "").strip().lower()
        _gf_op_map = {
            "use_item": "gb_consumable_item_use",
            "med": "gb_arena_med_use",
            "nade": "gb_arena_nade_use",
        }
        if _op == "hit":
            return {"ok": False, "error": "gf_action_op_not_implemented", "http_status": 501, "hint": "hit_wip"}
        mutate_action = _gf_op_map.get(_op) or ""
        if not mutate_action:
            return {"ok": False, "error": "bad_gf_op", "http_status": 400, "hint": "use_item|med|nade"}

    allowed = {
        "lavka_buy",
        "arena_buy_med",
        "arena_buy_nade",
        "consumable_buff_use",
        "gf_server_battle_init",
        "create_gf_session_v1",
        "gf_action_v1",
        "gb_consumable_item_use",
        "gb_arena_med_use",
        "gb_arena_nade_use",
    }
    if client_action not in allowed:
        return {"ok": False, "error": "unknown_action", "http_status": 400}'''

MUTATE_BLOCK = '''            gf_cur_epoch = None
            work_body = body
            if mutate_action == "gf_server_battle_init":
                err_c, err_st = apply_server_battle_init(st, work_body, cur, tg_id, prow)
            elif mutate_action in {"gb_consumable_item_use", "gb_arena_med_use", "gb_arena_nade_use"}:
                prep = prepare_gb_action_body(work_body, tg_id, cur, st, cur_sv)
                if not prep.get("ok"):
                    conn.commit()
                    out_e = {
                        "ok": False,
                        "error": prep.get("error") or "gf_prep_failed",
                        "http_status": int(prep.get("http_status") or 400),
                    }
                    if prep.get("gf_battle_epoch") is not None:
                        out_e["gf_battle_epoch"] = prep["gf_battle_epoch"]
                    if prep.get("current_state_version") is not None:
                        out_e["current_state_version"] = prep["current_state_version"]
                    return out_e
                work_body = prep["body"]
                gf_cur_epoch = prep.get("gf_cur_epoch")
                err_c, err_st = _pga_mutate_state_for_action(st, mutate_action, work_body)
            else:
                err_c, err_st = _pga_mutate_state_for_action(st, mutate_action, work_body)
            if err_c:
                conn.commit()
                return {"ok": False, "error": err_c, "http_status": int(err_st or 400)}

            if gf_cur_epoch is not None:
                b_out = st.get("groupFight", {}).get("battle") if isinstance(st.get("groupFight"), dict) else None
                if isinstance(b_out, dict):
                    next_ep = int(gf_cur_epoch) + 1
                    b_out["_gfEpoch"] = next_ep
                    try:
                        battle_row_update(cur, tg_id, next_ep, b_out)
                    except Exception:
                        conn.rollback()
                        return {"ok": False, "error": "gf_battle_persist_failed", "http_status": 500}

            ok_i, er_i = validate_player_state_integrity(st)'''

OLD_MUTATE = '''            err_c, err_st = _pga_mutate_state_for_action(st, action, body)
            if err_c:
                conn.commit()
                return {"ok": False, "error": err_c, "http_status": int(err_st or 400)}

            ok_i, er_i = validate_player_state_integrity(st)'''


def main():
    text = GB.read_text(encoding="utf-8")
    if "from gf_server_battle_logic import" not in text:
        text = text.replace(
            "import psycopg2\nfrom aiohttp import web\n\n\nDB_DSN",
            IMPORT_BLOCK.replace("DB_DSN", "DB_DSN", 1),
            1,
        )
    # Replace function header + allowed (old style action =)
    text = re.sub(
        r"def player_game_action_sync\(tg_id: str, user: dict, body: dict, client_ip: str \| None\) -> dict:\n    action = str\(body\.get\(\"action\"\) or \"\"\)\.strip\(\)\n    allowed = \{[^}]+\}\n    if action not in allowed:\n        return \{\"ok\": False, \"error\": \"unknown_action\", \"http_status\": 400\}",
        NEW_SYNC_START,
        text,
        count=1,
    )
    text = text.replace('"action": action,', '"action": client_action,', 1)  # duplicate return — first occurrence in this function only risky
    if OLD_MUTATE not in text:
        print("OLD_MUTATE block not found; maybe already patched")
    else:
        text = text.replace(OLD_MUTATE, MUTATE_BLOCK, 1)

    text = text.replace(
        """        try:
            append_player_state_event(
                tg_id,
                str(action)[:64],
                "player_game_action_v1",
                rid or str(uuid.uuid4())[:64],
                out_sv,
                f"game_action {action}",
                _state_summary_for_event(st),
                {"action": action, "item_id": body.get("item_id")},
            )""",
        """        try:
            append_player_state_event(
                tg_id,
                str(client_action)[:64],
                "player_game_action_v1",
                rid or str(uuid.uuid4())[:64],
                out_sv,
                f"game_action {client_action}",
                _state_summary_for_event(st),
                {
                    "action": client_action,
                    "mutate_action": mutate_action,
                    "item_id": body.get("item_id"),
                    "op": body.get("op"),
                },
            )""",
        1,
    )
    text = text.replace(
        """        return {
            "ok": True,
            "player": _normalize_player_state_field(pl) if pl else None,
            "state_version": out_sv,
            "action": action,
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _db_put(conn)


def _normalize_player_state_field""",
        """        return {
            "ok": True,
            "player": _normalize_player_state_field(pl) if pl else None,
            "state_version": out_sv,
            "action": client_action,
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _db_put(conn)


def _normalize_player_state_field""",
        1,
    )

    GB.write_text(text, encoding="utf-8")
    print("patched", GB)


if __name__ == "__main__":
    main()
