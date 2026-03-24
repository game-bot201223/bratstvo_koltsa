"""One-off patcher: run from repo root: python scripts/_apply_gf_server_battle_patch.py"""
from pathlib import Path


def main():
    p = Path(__file__).resolve().parent / "game_backend.py"
    text = p.read_text(encoding="utf-8")

    old_allowed = """    allowed = {
        \"lavka_buy\",
        \"arena_buy_med\",
        \"arena_buy_nade\",
        \"consumable_buff_use\",
        \"gb_consumable_item_use\",
        \"gb_arena_med_use\",
        \"gb_arena_nade_use\",
    }"""
    new_allowed = """    allowed = {
        \"lavka_buy\",
        \"arena_buy_med\",
        \"arena_buy_nade\",
        \"consumable_buff_use\",
        \"gf_server_battle_init\",
        \"gb_consumable_item_use\",
        \"gb_arena_med_use\",
        \"gb_arena_nade_use\",
    }"""
    if old_allowed not in text:
        print("SKIP or FAIL: allowed block not found")
    else:
        text = text.replace(old_allowed, new_allowed, 1)
        print("patched allowed")

    old_mut = """            err_c, err_st = _pga_mutate_state_for_action(st, action, body)
            if err_c:
                conn.commit()
                return {\"ok\": False, \"error\": err_c, \"http_status\": int(err_st or 400)}

            ok_i, er_i = validate_player_state_integrity(st)"""

    new_mut = """            gf_cur_epoch = None
            if action == \"gf_server_battle_init\":
                err_c, err_st = apply_server_battle_init(st, body, cur, tg_id, prow)
            elif action in {\"gb_consumable_item_use\", \"gb_arena_med_use\", \"gb_arena_nade_use\"}:
                prep = prepare_gb_action_body(body, tg_id, cur, st, cur_sv)
                if not prep.get(\"ok\"):
                    conn.commit()
                    out_e = {
                        \"ok\": False,
                        \"error\": prep.get(\"error\") or \"gf_prep_failed\",
                        \"http_status\": int(prep.get(\"http_status\") or 400),
                    }
                    if prep.get(\"gf_battle_epoch\") is not None:
                        out_e[\"gf_battle_epoch\"] = prep[\"gf_battle_epoch\"]
                    if prep.get(\"current_state_version\") is not None:
                        out_e[\"current_state_version\"] = prep[\"current_state_version\"]
                    return out_e
                body = prep[\"body\"]
                gf_cur_epoch = prep.get(\"gf_cur_epoch\")
                err_c, err_st = _pga_mutate_state_for_action(st, action, body)
            else:
                err_c, err_st = _pga_mutate_state_for_action(st, action, body)
            if err_c:
                conn.commit()
                return {\"ok\": False, \"error\": err_c, \"http_status\": int(err_st or 400)}

            if gf_cur_epoch is not None:
                b_out = st.get(\"groupFight\", {}).get(\"battle\") if isinstance(st.get(\"groupFight\"), dict) else None
                if isinstance(b_out, dict):
                    next_ep = int(gf_cur_epoch) + 1
                    b_out[\"_gfEpoch\"] = next_ep
                    try:
                        battle_row_update(cur, tg_id, next_ep, b_out)
                    except Exception:
                        conn.rollback()
                        return {\"ok\": False, \"error\": \"gf_battle_persist_failed\", \"http_status\": 500}

            ok_i, er_i = validate_player_state_integrity(st)"""

    if old_mut not in text:
        print("SKIP or FAIL: mutate block not found")
    else:
        text = text.replace(old_mut, new_mut, 1)
        print("patched mutate block")

    imp = "from gf_server_battle_logic import apply_server_battle_init, battle_row_update, prepare_gb_action_body"
    if imp not in text:
        needle = "from aiohttp import web\n\n\nDB_DSN"
        rep = "from aiohttp import web\n\n" + imp + "\n\n\nDB_DSN"
        if needle not in text:
            print("FAIL: import anchor not found")
        else:
            text = text.replace(needle, rep, 1)
            print("patched import")

    p.write_text(text, encoding="utf-8")
    print("done ->", p)


if __name__ == "__main__":
    main()
