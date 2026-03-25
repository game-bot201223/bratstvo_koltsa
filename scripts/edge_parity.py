"""
Логика ранее в отдельном serverless-слое (исторически Deno): тот же Postgres, что и game_backend.
Используется VPS Python backend при nginx proxy на /functions/v1/ (совместимый путь API).
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone

import psycopg2

DB_DSN = os.environ.get("GAME_DB_DSN", "dbname=gamedb user=postgres host=/var/run/postgresql")

HAVCHIK_ENERGY_BY_TYPE = (5, 10, 15, 20, 25, 30)
HAVCHIK_LEVEL_FOR_TYPE = (1, 10, 30, 60, 120, 200)
HAVCHIK_CLAIM_DAILY_CAP = 200


def _conn():
    return psycopg2.connect(DB_DSN)


def _parse_state(raw):
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    s = str(raw).strip()
    if not s:
        return {}
    try:
        o = json.loads(s)
        return o if isinstance(o, dict) else {}
    except Exception:
        return {}


def _sanitize_clan_id(v) -> str:
    s = str(v or "").strip().upper()
    if not s or not re.match(r"^CLN\d{1,20}$", s):
        return ""
    return s


def _utc_day_start_iso() -> str:
    d = datetime.now(timezone.utc)
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc).isoformat()


def _havchik_game_day_start_iso() -> str:
    """Игровой день с 18:00 UTC — как халтурка и подписи в клиенте (не полночь UTC)."""
    d = datetime.now(timezone.utc)
    if d.hour < 18:
        d = d - timedelta(days=1)
    return datetime(d.year, d.month, d.day, 18, 0, 0, tzinfo=timezone.utc).isoformat()


def _rate_limit_allow(cur, key: str, window_ms: int) -> tuple[bool, str | None]:
    try:
        cur.execute("select * from public.rate_limit_allow(%s, %s)", (key, int(window_ms)))
        row = cur.fetchone()
        if not row or len(row) < 3:
            return True, None
        allowed = bool(row[1])
        nxt = row[2].isoformat() if row[2] else None
        return allowed, nxt
    except Exception:
        return True, None


def delete_clan_operation(actor_tg_id: str, clan_id_raw: str) -> dict:
    cid = _sanitize_clan_id(clan_id_raw)
    if not cid:
        return {"ok": False, "error": "bad_clan_id", "http": 400}
    conn = _conn()
    try:
        with conn.cursor() as cur:
            allowed, nxt = _rate_limit_allow(cur, f"delete_clan:{actor_tg_id}", 1500)
            if not allowed:
                return {"ok": False, "error": "rate_limited", "next_allow_at": nxt, "http": 429}
            cur.execute(
                "select id, owner_tg_id from public.clans where id = %s limit 1",
                (cid,),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return {"ok": True, "deleted": False, "http": 200}
            if str(row[1] or "") != str(actor_tg_id):
                conn.rollback()
                return {"ok": False, "error": "forbidden", "http": 403}
            cur.execute("delete from public.clans where id = %s", (cid,))
        conn.commit()
        return {"ok": True, "deleted": True, "http": 200}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_group_fight_entries_operation(start_ts_iso: str) -> dict:
    if not start_ts_iso or not str(start_ts_iso).strip():
        return {"ok": False, "error": "missing_start_ts", "http": 400}
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select tg_id, name, photo_url, stats_sum
                from public.group_fight_entries
                where start_ts = %s::timestamptz
                order by stats_sum desc nulls last
                limit 5000
                """,
                (str(start_ts_iso).strip(),),
            )
            rows = cur.fetchall()
        conn.commit()
        out = []
        for r in rows or []:
            out.append(
                {
                    "tg_id": str(r[0] or ""),
                    "name": str(r[1] or ""),
                    "photo_url": str(r[2] or ""),
                    "stats_sum": int(r[3] or 0),
                }
            )
        return {"ok": True, "entries": out, "http": 200}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def join_group_fight_operation(
    tg_id: str,
    start_ts_iso: str,
    name: str,
    photo_url: str,
    stats_sum: int,
) -> dict:
    if not start_ts_iso or not str(start_ts_iso).strip():
        return {"ok": False, "error": "missing_start_ts", "http": 400}
    conn = _conn()
    try:
        with conn.cursor() as cur:
            allowed, nxt = _rate_limit_allow(cur, f"join_group_fight:{tg_id}", 900)
            if not allowed:
                return {"ok": False, "error": "rate_limited", "next_allow_at": nxt, "http": 429}
            cur.execute(
                """
                insert into public.group_fight_entries (start_ts, tg_id, name, photo_url, stats_sum, updated_at)
                values (%s::timestamptz, %s, %s, %s, %s, now())
                on conflict (start_ts, tg_id) do update set
                  name = excluded.name,
                  photo_url = excluded.photo_url,
                  stats_sum = excluded.stats_sum,
                  updated_at = now()
                """,
                (
                    str(start_ts_iso).strip(),
                    str(tg_id),
                    str(name or "Player")[:64],
                    str(photo_url or "")[:512],
                    max(0, int(stats_sum or 0)),
                ),
            )
        conn.commit()
        return {"ok": True, "tg_id": str(tg_id), "start_ts": str(start_ts_iso).strip(), "http": 200}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def havchik_inbox_pull_operation(to_tg_id: str, limit: int) -> dict:
    lim = max(1, min(500, int(limit or 50)))
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, from_tg_id, from_name, type_id, energy, created_at
                from public.havchik_inbox
                where to_tg_id = %s and claimed = false
                order by id asc
                limit %s
                """,
                (str(to_tg_id), lim),
            )
            rows = cur.fetchall()
        conn.commit()
        items = []
        for r in rows or []:
            items.append(
                {
                    "id": int(r[0]),
                    "from_tg_id": str(r[1] or ""),
                    "from_name": str(r[2] or ""),
                    "type_id": int(r[3] or 0),
                    "energy": int(r[4] or 0),
                    "created_at": r[5].isoformat() if r[5] else None,
                }
            )
        return {"ok": True, "items": items, "http": 200}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def havchik_claim_operation(tg_id: str, inbox_id: int, expected_state_version: int | None) -> dict:
    if inbox_id <= 0:
        return {"ok": False, "error": "bad_id", "http": 400}
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select tg_id, state, state_version, name, photo_url, level, xp, gold, silver, tooth,"
                " district_fear_total, arena_power, arena_wins, arena_losses, stats_sum, boss_wins"
                " from public.players where tg_id = %s for update",
                (str(tg_id),),
            )
            prow = cur.fetchone()
            if not prow:
                conn.rollback()
                return {"ok": False, "error": "player_not_found", "http": 404}
            cur_sv = int(prow[2] or 0)
            if expected_state_version is not None and int(expected_state_version) != cur_sv:
                conn.rollback()
                return {
                    "ok": False,
                    "error": "state_regress_blocked",
                    "current_state_version": cur_sv,
                    "http": 409,
                }

            cur.execute(
                "select id, energy from public.havchik_inbox where id = %s and to_tg_id = %s and claimed = false",
                (int(inbox_id), str(tg_id)),
            )
            ir = cur.fetchone()
            if not ir:
                conn.rollback()
                return {"ok": False, "error": "not_found", "http": 404}
            energy_add = max(0, int(ir[1] or 0))

            day_start = _havchik_game_day_start_iso()
            cur.execute(
                """
                select coalesce(sum(energy), 0) from public.havchik_inbox
                where to_tg_id = %s and claimed = true and claimed_at >= %s::timestamptz
                """,
                (str(tg_id), day_start),
            )
            sum_today = int(cur.fetchone()[0] or 0)
            if sum_today + energy_add > HAVCHIK_CLAIM_DAILY_CAP:
                conn.rollback()
                return {"ok": False, "error": "daily_cap", "http": 429}

            cur.execute(
                """
                update public.havchik_inbox set claimed = true, claimed_at = now()
                where id = %s and to_tg_id = %s and claimed = false
                """,
                (int(inbox_id), str(tg_id)),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return {"ok": False, "error": "not_found", "http": 404}

            st = _parse_state(prow[1])
            emax = max(400, int(st.get("energyMax") or 400))
            en = min(emax, max(0, int(st.get("energy") or 0)) + energy_add)
            st["energy"] = en
            # Иначе srv_apply_energy_regen на следующем запросе считает пассив с древнего lastEnergyTs и «ломает» баланс.
            st["lastEnergyTs"] = int(datetime.now(timezone.utc).timestamp() * 1000)
            next_sv = cur_sv + 1
            cur.execute(
                "update public.players set state = %s::jsonb, state_version = %s, updated_at = now() "
                "where tg_id = %s and state_version = %s",
                (json.dumps(st, ensure_ascii=False), next_sv, str(tg_id), cur_sv),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return {
                    "ok": False,
                    "error": "state_regress_blocked",
                    "current_state_version": cur_sv,
                    "http": 409,
                }

            item = {"id": int(inbox_id), "energy": energy_add}
        conn.commit()
        return {"ok": True, "item": item, "state_version": next_sv, "http": 200}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _find_player_by_name_lower(cur, nm: str) -> str | None:
    cur.execute(
        "select tg_id from public.players where lower(trim(name)) = lower(trim(%s)) limit 1",
        (nm[:18],),
    )
    r = cur.fetchone()
    return str(r[0]) if r and r[0] else None


def _players_in_clan_by_state(cur, clan_id: str, sender_tg_id: str) -> set[str]:
    """tg_id игроков, у которых в state прописан тот же клан (без учёта регистра CLN…)."""
    out: set[str] = set()
    if not clan_id:
        return out
    cur.execute(
        """
        select tg_id from public.players
        where (
          upper(trim(coalesce(state->'clan'->>'id',''))) = %s
          or upper(trim(coalesce(state->>'clanId',''))) = %s
          or upper(trim(coalesce(state->>'clan_id',''))) = %s
        )
        and tg_id::text <> %s
        limit 500
        """,
        (clan_id, clan_id, clan_id, str(sender_tg_id)),
    )
    for r in cur.fetchall() or []:
        tid = str(r[0] or "").strip()
        if tid:
            out.add(tid)
    return out


def _clan_data_resolve_members(cur, data: object, sender_tg_id: str, into: set[str]) -> None:
    if isinstance(data, str):
        try:
            data = json.loads(data) if data.strip() else {}
        except Exception:
            data = {}
    if not isinstance(data, dict):
        return
    meta = data.get("memberMeta")
    if isinstance(meta, dict):
        for _nm, mv in meta.items():
            if isinstance(mv, dict):
                tid = str(mv.get("tg_id") or mv.get("tgId") or "").strip()
                if tid and tid != sender_tg_id:
                    into.add(tid)
    mem = data.get("members")
    if isinstance(mem, list):
        for m in mem:
            if isinstance(m, dict):
                tid = str(m.get("tg_id") or m.get("tgId") or "").strip()
                if tid and tid != sender_tg_id:
                    into.add(tid)
            else:
                nm = str(m or "").strip()[:18]
                if nm:
                    tid2 = _find_player_by_name_lower(cur, nm)
                    if tid2 and tid2 != sender_tg_id:
                        into.add(tid2)


def _havchik_recipients_from_sender_clan_state(cur, st: dict, clan_id: str, sender_tg_id: str) -> set[str]:
    """
    Ростер из state отправителя (часто есть только локально, без строки в public.clans).
    """
    out: set[str] = set()
    if not clan_id:
        return out
    c = st.get("clan")
    if not isinstance(c, dict):
        return out
    st_cid = _sanitize_clan_id(c.get("id"))
    if st_cid and st_cid != clan_id:
        return out
    _clan_data_resolve_members(cur, c, sender_tg_id, out)
    for key in ("leader", "deputy", "owner"):
        v = c.get(key)
        if isinstance(v, str) and v.strip():
            nm = v.strip()[:18]
            tid2 = _find_player_by_name_lower(cur, nm)
            if tid2 and tid2 != sender_tg_id:
                out.add(tid2)
    return out


def _list_clan_member_tg_ids(cur, clan_id: str, sender_tg_id: str) -> set[str]:
    out: set[str] = set()
    cur.execute(
        "select owner_tg_id, data from public.clans where id = %s limit 1",
        (clan_id,),
    )
    row = cur.fetchone()
    if row:
        owner = str(row[0] or "").strip()
        if owner and owner != str(sender_tg_id):
            out.add(owner)
        _clan_data_resolve_members(cur, row[1], str(sender_tg_id), out)
    # Нельзя return при отсутствии строки в public.clans — иначе теряем всех, кто только в players.state.
    out |= _players_in_clan_by_state(cur, clan_id, str(sender_tg_id))
    return out


def havchik_send_operation(sender_tg_id: str, type_id: int) -> dict:
    tid = max(0, min(5, int(type_id)))
    energy = HAVCHIK_ENERGY_BY_TYPE[tid] if tid < len(HAVCHIK_ENERGY_BY_TYPE) else 5
    min_lvl = HAVCHIK_LEVEL_FOR_TYPE[tid] if tid < len(HAVCHIK_LEVEL_FOR_TYPE) else 1

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select tg_id, name, level, state, state_version from public.players where tg_id = %s limit 1 for update",
                (str(sender_tg_id),),
            )
            srow = cur.fetchone()
            if not srow:
                conn.rollback()
                return {"ok": False, "error": "sender_not_found", "http": 404}
            sender_level = max(1, int(srow[2] or 1))
            cur_sv = int(srow[4] or 0)
            if sender_level < min_lvl:
                conn.rollback()
                return {
                    "ok": False,
                    "error": "level_required",
                    "required_level": min_lvl,
                    "current_level": sender_level,
                    "http": 403,
                }

            day_start = _havchik_game_day_start_iso()
            cur.execute(
                "select count(*) from public.havchik_inbox where from_tg_id = %s and created_at >= %s::timestamptz",
                (str(sender_tg_id), day_start),
            )
            if int(cur.fetchone()[0] or 0) >= 1:
                conn.rollback()
                return {"ok": False, "error": "one_send_per_day", "http": 429}

            st = _parse_state(srow[3])
            from_name = str(srow[1] or "Player").strip()[:18] or "Player"
            sender_lower = from_name.lower()
            recipients: set[str] = set()

            fr = st.get("friends")
            if isinstance(fr, list):
                resolved = 0
                for it in fr[:80]:
                    if isinstance(it, str):
                        nm = str(it).strip()[:18]
                        if not nm or nm.lower() == sender_lower or resolved >= 20:
                            continue
                        tid2 = _find_player_by_name_lower(cur, nm)
                        if tid2 and tid2 != sender_tg_id:
                            recipients.add(tid2)
                            resolved += 1
                    elif isinstance(it, dict):
                        tgx = str(it.get("tg_id") or it.get("tgId") or "").strip()
                        if tgx and tgx != sender_tg_id:
                            recipients.add(tgx)
                        else:
                            nm = str(it.get("name") or "").strip()[:18]
                            if nm and nm.lower() != sender_lower and resolved < 20:
                                tid2 = _find_player_by_name_lower(cur, nm)
                                if tid2 and tid2 != sender_tg_id:
                                    recipients.add(tid2)
                                    resolved += 1

            clan_id = _sanitize_clan_id(st.get("clan", {}).get("id") if isinstance(st.get("clan"), dict) else None)
            if not clan_id:
                clan_id = _sanitize_clan_id(st.get("clanId") or st.get("clan_id"))
            if clan_id:
                for t in _list_clan_member_tg_ids(cur, clan_id, str(sender_tg_id)):
                    recipients.add(t)
                for t in _havchik_recipients_from_sender_clan_state(cur, st, clan_id, str(sender_tg_id)):
                    recipients.add(t)

            rec_arr = [x for x in recipients if x]
            if not rec_arr:
                conn.rollback()
                return {"ok": False, "error": "no_recipients", "http": 400}

            for to_tg in rec_arr:
                cur.execute(
                    """
                    insert into public.havchik_inbox (to_tg_id, from_tg_id, from_name, type_id, energy, created_at)
                    values (%s, %s, %s, %s, %s, now())
                    """,
                    (to_tg, str(sender_tg_id), from_name, tid, energy),
                )
            st["havchikLastSendTs"] = int(datetime.now(timezone.utc).timestamp() * 1000)
            next_sv = cur_sv + 1
            cur.execute(
                "update public.players set state = %s::jsonb, state_version = %s, updated_at = now() "
                "where tg_id = %s and state_version = %s",
                (json.dumps(st, ensure_ascii=False), next_sv, str(sender_tg_id), cur_sv),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return {
                    "ok": False,
                    "error": "state_regress_blocked",
                    "current_state_version": cur_sv,
                    "http": 409,
                }
        conn.commit()
        return {"ok": True, "inserted": len(rec_arr), "http": 200}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def referral_accept_operation(referrer_tg_id: str, new_user_tg_id: str, new_user_name: str) -> dict:
    ref = str(referrer_tg_id or "").strip()
    new_id = str(new_user_tg_id or "").strip()
    nm = str(new_user_name or "").strip()[:18] or "Браток"
    if not ref or not new_id:
        return {"ok": False, "error": "missing_init_data_or_referrer", "http": 400}
    if ref == new_id:
        return {"ok": True, "added": False, "reason": "self", "http": 200}
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select state, state_version from public.players where tg_id = %s for update",
                (ref,),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return {"ok": False, "error": "referrer_not_found", "http": 404}
            st = _parse_state(row[0])
            cur_sv = int(row[1] or 0)
            friends = st.get("friends") if isinstance(st.get("friends"), list) else []
            nmn = nm.upper()
            for f in friends:
                if isinstance(f, dict):
                    if str(f.get("tg_id") or f.get("tgId") or "").strip() == new_id:
                        conn.commit()
                        return {"ok": True, "added": False, "reason": "already_friend", "http": 200}
                    if str(f.get("name") or "").strip().upper() == nmn:
                        conn.commit()
                        return {"ok": True, "added": False, "reason": "already_friend", "http": 200}
                elif isinstance(f, str) and str(f).strip().upper() == nmn:
                    conn.commit()
                    return {"ok": True, "added": False, "reason": "already_friend", "http": 200}
            friends.append({"tg_id": new_id, "name": nm})
            st["friends"] = friends[-250:]
            next_sv = cur_sv + 1
            cur.execute(
                "update public.players set state = %s::jsonb, state_version = %s, updated_at = now() "
                "where tg_id = %s and state_version = %s",
                (json.dumps(st, ensure_ascii=False), next_sv, ref, cur_sv),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return {
                    "ok": False,
                    "error": "state_regress_blocked",
                    "current_state_version": cur_sv,
                    "http": 409,
                }
        conn.commit()
        return {"ok": True, "added": True, "http": 200}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
