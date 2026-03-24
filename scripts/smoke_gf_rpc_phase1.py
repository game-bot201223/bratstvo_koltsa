#!/usr/bin/env python3
"""
Офлайн-смоук Phase 1 GF RPC: только ветки до открытия БД (без DSN).
Запуск из каталога scripts/:
  python smoke_gf_rpc_phase1.py
Полный HTTP-смоук (create_gf_session / gb_*): на запущенном game_backend с БД.
"""
from __future__ import annotations

import os
import sys


def main() -> int:
    try:
        import psycopg2  # noqa: F401
        import aiohttp  # noqa: F401
    except ImportError:
        print("smoke_gf_rpc_phase1: SKIP (need: pip install psycopg2-binary aiohttp)")
        return 0

    here = os.path.dirname(os.path.abspath(__file__))
    if here not in sys.path:
        sys.path.insert(0, here)

    # 1) Новые RPC выключены
    os.environ.pop("GF_SERVER_ACTIONS_ENABLED", None)
    if "game_backend" in sys.modules:
        del sys.modules["game_backend"]
    import game_backend as gb1

    r = gb1.player_game_action_sync("0", {}, {"action": "create_gf_session_v1"}, None)
    assert r.get("http_status") == 403 and r.get("error") == "gf_server_actions_disabled", r

    r = gb1.player_game_action_sync("0", {}, {"action": "gf_commit_v1"}, None)
    assert r.get("http_status") == 403 and r.get("error") == "gf_server_actions_disabled", r

    # 2) Включены: commit и hit — без БД
    os.environ["GF_SERVER_ACTIONS_ENABLED"] = "1"
    del sys.modules["game_backend"]
    import game_backend as gb2

    # gf_commit_v1 теперь идёт в БД (нет раннего 501)
    r = gb2.player_game_action_sync(
        "0",
        {},
        {"action": "gf_commit_v1", "expected_state_version": 0},
        None,
    )
    assert r.get("http_status") == 404 and r.get("error") == "player_not_found", r

    r = gb2.player_game_action_sync("0", {}, {"action": "gf_action_v1", "op": "hit"}, None)
    assert r.get("http_status") == 501 and r.get("error") == "gf_action_op_not_implemented", r

    r = gb2.player_game_action_sync("0", {}, {"action": "gf_action_v1", "op": "typo_op"}, None)
    assert r.get("http_status") == 400 and r.get("error") == "bad_gf_op", r

    print("smoke_gf_rpc_phase1: OK (offline branches)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
