#!/usr/bin/env python3
"""Юнит-тесты логики GF commit без БД: gf_server_battle_logic."""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)


def main() -> int:
    import gf_server_battle_logic as g

    b_win = {
        "startTs": 1,
        "cost": 2,
        "myHp": 10,
        "myMaxHp": 10,
        "targets": [{"id": "E1", "hp": 0, "maxHp": 10}],
        "myTeam": [{"id": "YOU", "hp": 10, "maxHp": 10}],
        "dmgBy": {"YOU": 100, "A1": 0},
    }
    assert g.gf_battle_terminal_outcome(b_win) == "win"

    b_ongoing = {
        "startTs": 1,
        "cost": 2,
        "myHp": 10,
        "targets": [{"id": "E1", "hp": 5, "maxHp": 10}],
        "myTeam": [{"id": "YOU", "hp": 10, "maxHp": 10}],
        "dmgBy": {"YOU": 1},
    }
    assert g.gf_battle_terminal_outcome(b_ongoing) is None

    b_loss = {
        "startTs": 1,
        "cost": 2,
        "myHp": 0,
        "targets": [{"id": "E1", "hp": 10, "maxHp": 10}],
        "myTeam": [{"id": "YOU", "hp": 0, "maxHp": 10}, {"id": "A1", "hp": 0, "maxHp": 10}],
        "dmgBy": {"YOU": 50},
    }
    assert g.gf_battle_terminal_outcome(b_loss) == "loss"

    g2 = g.gf_compute_group_fight_gold(b_win, True)
    assert g2 == 5, g2  # 3+1+1 cost2 win alive top

    for _ in range(30):
        x = g.gf_roll_consumable_drop_id()
        assert 1 <= x <= 10, x

    print("test_gf_commit_unit: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
