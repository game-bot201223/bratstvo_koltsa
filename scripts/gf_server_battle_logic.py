"""
Серверный state группового боя: таблица public.player_gf_battle, init и optimistic lock для gb_*.
Включается env GF_SERVER_BATTLE_PRIMARY=1. См. docs/GROUP_FIGHT_SERVER_STATE.md.
"""
from __future__ import annotations

import copy
import json
import math
import os
import secrets
from datetime import datetime, timezone


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def server_battle_primary() -> bool:
    return os.environ.get("GF_SERVER_BATTLE_PRIMARY", "").strip().lower() in {"1", "true", "yes", "on"}


def server_gf_actions_enabled() -> bool:
    """
    Включает имена RPC create_gf_session_v1 / gf_action_v1 / gf_commit_v1 в player_game_action_v1.
    Старые action (gf_server_battle_init, gb_*) работают без этого флага.
    """
    return os.environ.get("GF_SERVER_ACTIONS_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}


def server_battle_inject_bots() -> bool:
    return os.environ.get("GF_SERVER_BATTLE_INJECT_BOTS", "1").strip().lower() not in {"0", "false", "no", "off"}


# Как на клиенте CONSUMABLES_DROP_CHANCES (index.html)
GF_CONSUMABLE_DROP_WEIGHTS = [13, 13, 13, 13, 13, 13, 3, 8, 3, 8]


def ensure_battle_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.player_gf_battle (
          tg_id text PRIMARY KEY,
          start_ts timestamptz NOT NULL,
          battle_epoch integer NOT NULL DEFAULT 1,
          battle jsonb NOT NULL DEFAULT '{}'::jsonb,
          committed_at timestamptz,
          updated_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS player_gf_battle_start_ts_idx ON public.player_gf_battle (start_ts);")
    except Exception:
        pass


def cost_from_start_ts_ms(ms: int) -> int:
    try:
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return 20 if (dt.hour % 2 == 0) else 2
    except Exception:
        return 2


def _safe_pos(v) -> float:
    try:
        x = float(v)
    except Exception:
        return 0.0
    if not math.isfinite(x) or x < 0:
        return 0.0
    return x


def _max_hp(health: float) -> int:
    return max(1, int(round(_safe_pos(health) * 18)))


def _eff_stat_from_state(st: dict, k: str) -> float:
    gym = st.get("gym") if isinstance(st.get("gym"), dict) else {}
    try:
        base = max(1, int(gym.get(k) or 1) or 1)
    except Exception:
        base = 1
    if k == "charisma":
        return float(base)
    cb = st.get("consumablesBuffs") if isinstance(st.get("consumablesBuffs"), dict) else {}
    try:
        until = int(cb.get(k) or 0)
    except Exception:
        until = 0
    consumables_add = 0
    if until > _now_ms():
        consumables_add = max(0, round(base * 0.30))
    return float(base + consumables_add)


def _p_stats_from_state(st: dict) -> dict:
    return {
        "h": _safe_pos(_eff_stat_from_state(st, "health")),
        "s": _safe_pos(_eff_stat_from_state(st, "strength")),
        "a": _safe_pos(_eff_stat_from_state(st, "agility")),
        "i": _safe_pos(_eff_stat_from_state(st, "initiative")),
        "e": _safe_pos(_eff_stat_from_state(st, "endurance")),
        "m": _safe_pos(_eff_stat_from_state(st, "might")),
    }


def fetch_group_fight_entries(cur, start_ts_ms: int) -> list:
    try:
        ts = datetime.fromtimestamp(start_ts_ms / 1000.0, tz=timezone.utc)
    except Exception:
        return []
    cur.execute(
        """
        select tg_id, coalesce(name, 'Player'), coalesce(photo_url, ''), coalesce(stats_sum, 0)
        from public.group_fight_entries
        where start_ts = %s::timestamptz
        """,
        (ts,),
    )
    rows = cur.fetchall() or []
    out = []
    for r in rows:
        out.append(
            {
                "tg_id": str(r[0] or "").strip(),
                "name": str(r[1] or "Player"),
                "photo_url": str(r[2] or ""),
                "stats_sum": int(r[3] or 0),
            }
        )
    return out


def build_battle_for_player(st: dict, tg_id: str, prow, start_ts_ms: int, cur) -> dict | None:
    entries = fetch_group_fight_entries(cur, start_ts_ms)
    arr = list(entries)
    if server_battle_inject_bots() and len(arr) < 10:
        for i in range(10 - len(arr)):
            arr.append({"tg_id": f"BOT_GF_{i + 1}", "name": f"BOT GF {i + 1}", "photo_url": "", "stats_sum": 0})
    me_id = str(tg_id).strip()
    has_you = any(str(e.get("tg_id") or "").strip() == me_id for e in arr)
    if not has_you:
        arr.append(
            {
                "tg_id": me_id or "YOU",
                "name": str(prow[2] or "Player")[:64],
                "photo_url": str(prow[3] or ""),
                "stats_sum": 0,
            }
        )

    def _sort_key(e):
        return (str(e.get("tg_id") or "").strip(), str(e.get("name") or ""))

    arr.sort(key=_sort_key)
    team_a = []
    team_b = []
    for i, e in enumerate(arr):
        (team_a if i % 2 == 0 else team_b).append(e)
    you_in_a = any(str(e.get("tg_id") or "").strip() == me_id for e in team_a)
    my_team_src = team_a if you_in_a else team_b
    enemy_src = team_b if you_in_a else team_a
    if not my_team_src or not enemy_src:
        return None

    p_stats = _p_stats_from_state(st)
    e_stats = {k: p_stats[k] for k in p_stats}
    my_max_hp = _max_hp(p_stats["h"])
    enemy_max_hp = _max_hp(e_stats["h"])
    cost = cost_from_start_ts_ms(start_ts_ms)
    battle = {
        "startTs": int(start_ts_ms),
        "cost": int(cost),
        "createdTs": int(_now_ms()),
        "round": 1,
        "turnLeft": 30,
        "acted": False,
        "myMaxHp": int(my_max_hp),
        "myHp": int(my_max_hp),
        "log": [],
        "dmgBy": {},
        "pStats": p_stats,
        "eStats": e_stats,
        "enemyMaxHp": int(enemy_max_hp),
        "myTarget": "",
        "myTeam": [],
        "targets": [],
        "_gfEpoch": 1,
    }
    you_photo = str(prow[3] or "")
    for i, e in enumerate(my_team_src):
        etg = str(e.get("tg_id") or "").strip()
        is_you = etg == me_id
        battle["myTeam"].append(
            {
                "id": "YOU" if is_you else ("A" + str(i + 1)),
                "name": str(e.get("name") or ("ТЫ" if is_you else f"Союзник {i + 1}"))[:64],
                "photo": you_photo if is_you else str(e.get("photo_url") or ""),
                "hp": int(my_max_hp),
                "maxHp": int(my_max_hp),
            }
        )
    for i, e in enumerate(enemy_src):
        battle["targets"].append(
            {
                "id": "E" + str(i + 1),
                "name": str(e.get("name") or f"Враг {i + 1}")[:64],
                "photo": str(e.get("photo_url") or ""),
                "hp": int(enemy_max_hp),
                "maxHp": int(enemy_max_hp),
            }
        )
    dmg_by = {}
    for u in battle["myTeam"]:
        uid = str(u.get("id") or "")
        if uid:
            dmg_by[uid] = 0
    battle["dmgBy"] = dmg_by
    return battle


def battle_row_lock(cur, tg_id: str) -> tuple[datetime, int, dict, datetime | None] | None:
    cur.execute(
        """
        select start_ts, battle_epoch, battle, committed_at
        from public.player_gf_battle
        where tg_id = %s
        for update
        """,
        (str(tg_id),),
    )
    r = cur.fetchone()
    if not r:
        return None
    _ts, ep, raw_b = r[0], int(r[1] or 1), r[2]
    _committed = r[3] if len(r) > 3 else None
    try:
        if isinstance(raw_b, str):
            bd = json.loads(raw_b) if str(raw_b).strip() else {}
        elif isinstance(raw_b, dict):
            bd = dict(raw_b)
        else:
            bd = {}
    except Exception:
        bd = {}
    return _ts, ep, bd, _committed


def battle_row_upsert(cur, tg_id: str, start_ts_ms: int, epoch: int, battle: dict) -> None:
    ts = datetime.fromtimestamp(start_ts_ms / 1000.0, tz=timezone.utc)
    cur.execute(
        """
        insert into public.player_gf_battle (tg_id, start_ts, battle_epoch, battle, updated_at)
        values (%s, %s, %s, %s::jsonb, now())
        on conflict (tg_id) do update set
          start_ts = excluded.start_ts,
          battle_epoch = excluded.battle_epoch,
          battle = excluded.battle,
          committed_at = NULL,
          updated_at = now()
        """,
        (str(tg_id), ts, int(epoch), json.dumps(battle, ensure_ascii=False)),
    )


def battle_row_mark_committed(cur, tg_id: str) -> None:
    """После gf_commit_v1: строка помечена, JSON боя очищен (новый бой только через init)."""
    cur.execute(
        """
        update public.player_gf_battle set
          committed_at = now(),
          battle = '{}'::jsonb,
          battle_epoch = 1,
          updated_at = now()
        where tg_id = %s
        """,
        (str(tg_id),),
    )


def battle_row_update(cur, tg_id: str, epoch: int, battle: dict) -> None:
    cur.execute(
        """
        update public.player_gf_battle set
          battle_epoch = %s,
          battle = %s::jsonb,
          updated_at = now()
        where tg_id = %s
        """,
        (int(epoch), json.dumps(battle, ensure_ascii=False), str(tg_id)),
    )


def apply_server_battle_init(st: dict, body: dict, cur, tg_id: str, prow) -> tuple[str | None, int | None]:
    if not server_battle_primary():
        return "gf_server_battle_disabled", 400
    ensure_battle_table(cur)
    try:
        raw_st = body.get("start_ts_ms")
        if raw_st is None:
            raw_st = body.get("startTs")
        start_ts_ms = int(raw_st or 0)
    except Exception:
        start_ts_ms = 0
    if start_ts_ms <= 0:
        return "bad_start_ts", 400
    if int(_now_ms()) < int(start_ts_ms):
        return "gf_battle_not_started", 400
    gf = st.get("groupFight") if isinstance(st.get("groupFight"), dict) else {}
    if not gf.get("joined"):
        return "gf_not_joined", 400
    if int(gf.get("startTs") or 0) != int(start_ts_ms):
        return "gf_start_ts_mismatch", 400

    battle = build_battle_for_player(st, tg_id, prow, start_ts_ms, cur)
    if not battle:
        return "gf_battle_build_failed", 400

    if not isinstance(st.get("groupFight"), dict):
        st["groupFight"] = {}
    st["groupFight"]["battle"] = battle

    battle_row_upsert(cur, tg_id, int(start_ts_ms), 1, battle)
    return None, None


def gf_battle_terminal_outcome(battle: dict | None) -> str | None:
    """
    Совпадает с условиями победы/поражения на клиенте (все враги мёртвы / команда мертва).
    Возвращает 'win', 'loss' или None, если бой ещё идёт или данные неполные.
    """
    if not isinstance(battle, dict) or not battle:
        return None
    targets = battle.get("targets")
    if not isinstance(targets, list):
        return None
    any_alive_enemy = False
    for t in targets:
        if not isinstance(t, dict):
            continue
        if int(t.get("hp") or 0) > 0:
            any_alive_enemy = True
            break
    my_hp = int(battle.get("myHp") or 0)
    you_alive = my_hp > 0
    ally_alive = False
    mt = battle.get("myTeam")
    if isinstance(mt, list):
        for u in mt:
            if not isinstance(u, dict):
                continue
            if str(u.get("id") or "") == "YOU":
                continue
            if int(u.get("hp") or 0) > 0:
                ally_alive = True
                break
    if not any_alive_enemy:
        return "win"
    if not you_alive and not ally_alive:
        return "loss"
    return None


def gf_compute_group_fight_gold(battle: dict, win: bool) -> int:
    """Как gfBattleComputeReward(b, win) в index.html."""
    try:
        cost = int(battle.get("cost") or 0)
    except Exception:
        cost = 0
    my_hp = int(battle.get("myHp") or 0)
    you_alive = my_hp > 0
    dmg_by = battle.get("dmgBy") if isinstance(battle.get("dmgBy"), dict) else {}
    best_id, best = "", -1
    for k, v in dmg_by.items():
        try:
            vv = int(v or 0)
        except Exception:
            vv = 0
        if vv > best:
            best, best_id = vv, str(k)
    is_top = bool(best_id and best_id == "YOU")
    gold = 0
    if win:
        if cost == 2:
            gold += 3
            if you_alive:
                gold += 1
            if is_top:
                gold += 1
        elif cost == 20:
            gold += 30
            if you_alive:
                gold += 10
            if is_top:
                gold += 10
    else:
        if is_top:
            if cost == 2:
                gold += 2
            elif cost == 20:
                gold += 20
    return max(0, int(gold))


def gf_roll_consumable_drop_id() -> int:
    """Тот же вес, что CONSUMABLES_DROP_CHANCES на клиенте (id 1..10)."""
    weights = list(GF_CONSUMABLE_DROP_WEIGHTS)
    total = sum(weights)
    if total <= 0:
        return 1
    rng = secrets.SystemRandom()
    r = rng.randrange(total)
    acc = 0
    for i, w in enumerate(weights):
        acc += w
        if r < acc:
            return i + 1
    return 1


def prepare_gb_action_body(body: dict, tg_id: str, cur, st: dict, cur_sv: int) -> dict:
    """
    При server_battle_primary подставляет battle из БД и возвращает db_epoch для последующего инкремента.
    ok=False — вызывающий должен вернуть HTTP-ответ без мутации.
    """
    if not server_battle_primary():
        return {"ok": True, "body": body, "gf_cur_epoch": None}
    ensure_battle_table(cur)
    row = battle_row_lock(cur, tg_id)
    if not row:
        return {"ok": False, "error": "gf_server_battle_missing", "http_status": 400}
    _, db_epoch, srv_battle, committed_at = row
    if committed_at is not None:
        return {"ok": False, "error": "gf_battle_already_committed", "http_status": 409}
    be_in = body.get("battle_epoch")
    if be_in is None:
        be_in = body.get("battleEpoch")
    try:
        be_in = int(be_in) if be_in is not None else None
    except Exception:
        be_in = None
    gf_chk = st.get("groupFight") if isinstance(st.get("groupFight"), dict) else {}
    exp_ts = int(gf_chk.get("startTs") or 0)
    if int(srv_battle.get("startTs") or 0) != exp_ts:
        return {"ok": False, "error": "gf_battle_start_ts_mismatch", "http_status": 409}
    if be_in is None or int(be_in) != int(db_epoch):
        return {
            "ok": False,
            "error": "gf_battle_epoch_mismatch",
            "http_status": 409,
            "gf_battle_epoch": int(db_epoch),
            "current_state_version": int(cur_sv),
        }
    if not isinstance(srv_battle, dict) or not srv_battle:
        return {"ok": False, "error": "gf_server_battle_empty", "http_status": 400}
    new_body = {**body, "battle": copy.deepcopy(srv_battle)}
    return {"ok": True, "body": new_body, "gf_cur_epoch": int(db_epoch)}


__all__ = [
    "server_battle_primary",
    "server_gf_actions_enabled",
    "ensure_battle_table",
    "apply_server_battle_init",
    "prepare_gb_action_body",
    "battle_row_update",
    "battle_row_lock",
    "battle_row_mark_committed",
    "gf_battle_terminal_outcome",
    "gf_compute_group_fight_gold",
    "gf_roll_consumable_drop_id",
    "GF_CONSUMABLE_DROP_WEIGHTS",
]
