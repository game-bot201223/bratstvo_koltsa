#!/usr/bin/env python3
import asyncio
import copy
import hashlib
import math
import time
import hmac
import json
import os
import random
import re
import secrets
import socket
import uuid
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qsl

import psycopg2
from aiohttp import web

from gf_server_battle_logic import (
    apply_server_battle_init,
    battle_row_update,
    prepare_gb_action_body,
    server_gf_actions_enabled,
)

DB_DSN = os.environ.get("GAME_DB_DSN", "dbname=gamedb user=postgres host=/var/run/postgresql")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
BACKEND_HOST = os.environ.get("GAME_BACKEND_HOST", "127.0.0.1").strip() or "127.0.0.1"
BACKEND_PORT = int(os.environ.get("GAME_BACKEND_PORT", "8081") or 8081)
ADMIN_TG_IDS = set(
    x.strip() for x in os.environ.get("ADMIN_TG_IDS", os.environ.get("ADMIN_TG_ID", "")).replace(";", ",").split(",") if x.strip()
)
SUPER_ADMIN_TG_ID = str((os.environ.get("SUPER_ADMIN_TG_ID", "") or "").strip())
if not SUPER_ADMIN_TG_ID:
    try:
        SUPER_ADMIN_TG_ID = sorted(list(ADMIN_TG_IDS))[0] if ADMIN_TG_IDS else ""
    except Exception:
        SUPER_ADMIN_TG_ID = ""
ALLOWED_TG_IDS = set(
    x.strip() for x in os.environ.get("ALLOWED_TG_IDS", "").replace(";", ",").split(",") if x.strip()
)
ALLOW_ANON_WHITELIST = os.environ.get("ALLOW_ANON_WHITELIST", "1").strip().lower() in {"1", "true", "yes", "on"}
STRICT_AUTH = os.environ.get("STRICT_AUTH", "1").strip().lower() in {"1", "true", "yes", "on"}
INIT_DATA_MAX_AGE_SECONDS = int(os.environ.get("INIT_DATA_MAX_AGE_SECONDS", "900") or 900)
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1").strip() or "127.0.0.1"
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379") or 6379)
REDIS_PASS = os.environ.get("REDIS_PASS", "").strip()
WS_CLIENTS = defaultdict(set)  # tg_id -> set[WebSocketResponse]
WS_LOCK = asyncio.Lock()
ADMIN_DANGER_TOKENS = {}


def player_progress_log(event: str, tg_id: str, **extra):
    """Структурные логи в stdout (journald/docker). Отключить: DISABLE_PLAYER_PROGRESS_LOG=1"""
    try:
        if os.environ.get("DISABLE_PLAYER_PROGRESS_LOG", "").strip().lower() in {"1", "true", "yes", "on"}:
            return
        rec = {"ts": datetime.now(timezone.utc).isoformat(), "event": str(event), "tg_id": str(tg_id)}
        for k, v in extra.items():
            if v is not None:
                rec[str(k)] = v
        print("PLAYER_PROGRESS " + json.dumps(rec, ensure_ascii=False, default=str), flush=True)
    except Exception:
        pass


def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-telegram-init-data",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
    }


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def verify_init_data(init_data: str):
    if not init_data or not TELEGRAM_BOT_TOKEN:
        return False, None
    pairs = parse_qsl(init_data, keep_blank_values=True)
    data = dict(pairs)
    provided_hash = data.get("hash", "")
    if not provided_hash:
        return False, None
    check_pairs = [f"{k}={v}" for k, v in pairs if k != "hash"]
    check_pairs.sort()
    data_check_string = "\n".join(check_pairs)
    secret_key = hmac.new(b"WebAppData", TELEGRAM_BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
    computed = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed.lower(), provided_hash.lower()):
        return False, None
    user_raw = data.get("user")
    if not user_raw:
        return False, None
    try:
        auth_date = int(data.get("auth_date", "0") or 0)
    except Exception:
        return False, None
    now_ts = int(datetime.now(timezone.utc).timestamp())
    if auth_date <= 0 or abs(now_ts - auth_date) > max(60, INIT_DATA_MAX_AGE_SECONDS):
        return False, None
    try:
        user = json.loads(user_raw)
    except Exception:
        return False, None
    if not user or not user.get("id"):
        return False, None
    return True, user


def extract_user_from_init_data(init_data: str):
    if not init_data:
        return None
    pairs = parse_qsl(init_data, keep_blank_values=True)
    data = dict(pairs)
    user_raw = data.get("user")
    if not user_raw:
        return None
    try:
        user = json.loads(user_raw)
    except Exception:
        return None
    if not user or not user.get("id"):
        return None
    return user


def is_allowed_user(tg_id: str) -> bool:
    if not ALLOWED_TG_IDS:
        return True
    return str(tg_id).strip() in ALLOWED_TG_IDS


def authorize_user_from_init_data(init_data: str):
    ok, user = verify_init_data(init_data)
    if ok and user and user.get("id"):
        tg_id = str(user["id"])
        if not is_allowed_user(tg_id):
            return False, None, "access_denied_whitelist"
        return True, user, None

    # Optional temporary anonymous whitelist mode. Disabled in strict mode.
    if ALLOW_ANON_WHITELIST and not STRICT_AUTH:
        user2 = extract_user_from_init_data(init_data)
        if user2 and user2.get("id"):
            tg_id = str(user2["id"])
            if is_allowed_user(tg_id):
                return True, user2, None
            return False, None, "access_denied_whitelist"
    return False, None, "unauthorized"


def _db_conn():
    return psycopg2.connect(DB_DSN)


def _db_put(conn):
    try:
        conn.close()
    except Exception:
        pass


def _redis_call(*args):
    resp = _redis_call_resp(*args)
    return resp is not None


def _redis_call_resp(*args):
    parts = [str(x) for x in args]
    req = f"*{len(parts)}\r\n" + "".join([f"${len(p.encode('utf-8'))}\r\n{p}\r\n" for p in parts])
    try:
        with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=0.6) as s:
            s.settimeout(0.6)
            if REDIS_PASS:
                auth = f"*2\r\n$4\r\nAUTH\r\n${len(REDIS_PASS.encode('utf-8'))}\r\n{REDIS_PASS}\r\n"
                s.sendall(auth.encode("utf-8"))
                try:
                    s.recv(1024)
                except Exception:
                    pass
            s.sendall(req.encode("utf-8"))
            try:
                return s.recv(4096).decode("utf-8", errors="ignore")
            except Exception:
                return "+OK\r\n"
    except Exception:
        return None


def _redis_readline(sock):
    buf = bytearray()
    while True:
        ch = sock.recv(1)
        if not ch:
            break
        buf.extend(ch)
        if len(buf) >= 2 and buf[-2:] == b"\r\n":
            break
    return bytes(buf)


def _redis_readexact(sock, n: int):
    data = bytearray()
    need = max(0, int(n or 0))
    while len(data) < need:
        part = sock.recv(need - len(data))
        if not part:
            break
        data.extend(part)
    return bytes(data)


def _redis_parse(sock):
    head = _redis_readline(sock)
    if not head:
        return None
    pfx = head[:1]
    body = head[1:-2].decode("utf-8", errors="ignore") if len(head) >= 2 else ""
    if pfx == b"+":
        return body
    if pfx == b"-":
        return None
    if pfx == b":":
        try:
            return int(body or 0)
        except Exception:
            return 0
    if pfx == b"$":
        try:
            ln = int(body or -1)
        except Exception:
            ln = -1
        if ln < 0:
            return None
        data = _redis_readexact(sock, ln)
        _ = _redis_readexact(sock, 2)  # \r\n
        return data.decode("utf-8", errors="ignore")
    if pfx == b"*":
        try:
            cnt = int(body or 0)
        except Exception:
            cnt = 0
        arr = []
        for _i in range(max(0, cnt)):
            arr.append(_redis_parse(sock))
        return arr
    return None


def _redis_exec(*args):
    parts = [str(x) for x in args]
    req = f"*{len(parts)}\r\n" + "".join([f"${len(p.encode('utf-8'))}\r\n{p}\r\n" for p in parts])
    try:
        with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=0.8) as s:
            s.settimeout(0.8)
            if REDIS_PASS:
                auth = f"*2\r\n$4\r\nAUTH\r\n${len(REDIS_PASS.encode('utf-8'))}\r\n{REDIS_PASS}\r\n"
                s.sendall(auth.encode("utf-8"))
                _redis_parse(s)
            s.sendall(req.encode("utf-8"))
            return _redis_parse(s)
    except Exception:
        return None


def request_client_ip(request) -> str | None:
    try:
        xff = request.headers.get("X-Forwarded-For") or request.headers.get("x-forwarded-for")
        if xff:
            first = str(xff.split(",")[0]).strip()
            return first[:80] if first else None
        ra = request.remote
        return str(ra)[:80] if ra else None
    except Exception:
        return None


def record_security_sample(kind: str, tg_id: str | None = None):
    """Счётчики для алертов: пишем в realtime_perf_samples (value_ms=1). metric_kind <= 32."""
    kk = "sg_x"
    try:
        raw = re.sub(r"[^a-zA-Z0-9_]+", "_", str(kind or "x").strip())[:20].lower()
        kk = ("sg_" + (raw or "x"))[:32]
        record_rt_sample(kk, 1)
    except Exception:
        pass
    try:
        if tg_id:
            player_progress_log("security_metric", str(tg_id), metric=str(kk))
    except Exception:
        pass


def check_player_save_rate_limits(tg_id: str, client_ip: str | None) -> tuple[bool, str]:
    """
    Лимит сохранений в минуту (Redis INCR + окно 60 с).
    SAVE_RATE_LIMIT_PER_MINUTE_TG=120 (0 = выкл), SAVE_RATE_LIMIT_PER_MINUTE_IP=0 (опц.).
    При недоступном Redis — пропускаем (fail-open).
    """
    try:
        lim_tg = int(os.environ.get("SAVE_RATE_LIMIT_PER_MINUTE_TG", "180") or 180)
    except Exception:
        lim_tg = 180
    try:
        lim_ip = int(os.environ.get("SAVE_RATE_LIMIT_PER_MINUTE_IP", "0") or 0)
    except Exception:
        lim_ip = 0
    bucket = int(time.time() // 60)
    if lim_tg > 0:
        key = f"rl:sv:tg:{str(tg_id)}:{bucket}"
        n = _redis_exec("INCR", key)
        if n is not None:
            try:
                if int(n) == 1:
                    _redis_exec("EXPIRE", key, "75")
            except Exception:
                pass
            try:
                if int(n) > int(lim_tg):
                    return False, "save_rate_limited_tg"
            except Exception:
                pass
    if lim_ip > 0 and client_ip:
        safe = re.sub(r"[^0-9a-fA-F.:]", "_", str(client_ip)[:80])
        key2 = f"rl:sv:ip:{safe}:{bucket}"
        n2 = _redis_exec("INCR", key2)
        if n2 is not None:
            try:
                if int(n2) == 1:
                    _redis_exec("EXPIRE", key2, "75")
            except Exception:
                pass
            try:
                if int(n2) > int(lim_ip):
                    return False, "save_rate_limited_ip"
            except Exception:
                pass
    return True, ""


def evaluate_save_session_gate(cur_row: dict | None, body: dict, is_admin_override: bool) -> tuple[bool, str, int]:
    """
    REQUIRE_SESSION_MATCH_FOR_SAVE=1: client должен слать session_id = players.active_session_id.
    Пустой active_session_id в БД — пропуск (старые строки). Админ-override — пропуск.
    Возвращает (ok, error, http_status).
    """
    if is_admin_override:
        return True, "", 0
    if os.environ.get("REQUIRE_SESSION_MATCH_FOR_SAVE", "").strip().lower() not in {"1", "true", "yes", "on"}:
        return True, "", 0
    if not cur_row:
        return True, "", 0
    db_sid = str((cur_row or {}).get("active_session_id") or "").strip()
    if not db_sid:
        return True, "", 0
    cli_sid = str(body.get("session_id") or body.get("sessionId") or "").strip()
    if not cli_sid:
        return False, "session_id_required", 403
    if cli_sid != db_sid:
        return False, "session_mismatch", 409
    return True, "", 0


def redis_touch_player_rt(tg_id: str, state_version: int):
    key = f"rt:player:{str(tg_id)}"
    _redis_call("HSET", key, "state_version", str(int(state_version or 0)), "updated_at", now_iso())
    _redis_call("EXPIRE", key, "300")


def redis_publish_player_save(tg_id: str, state_version: int):
    ch = f"rt:player:save:{str(tg_id)}"
    payload = json.dumps({"tg_id": str(tg_id), "state_version": int(state_version or 0), "ts": now_iso()}, ensure_ascii=False)
    _redis_call("PUBLISH", ch, payload)


def redis_publish_boss_hit(tg_id: str, boss_id: int, hp: int, max_hp: int):
    ch = f"rt:boss:hit:{str(tg_id)}"
    payload = json.dumps(
        {"tg_id": str(tg_id), "boss_id": int(boss_id or 0), "hp": int(hp or 0), "max_hp": int(max_hp or 0), "ts": now_iso()},
        ensure_ascii=False,
    )
    _redis_call("PUBLISH", ch, payload)


def redis_next_boss_seq(owner_tg_id: str, boss_id: int) -> int:
    key = f"rt:boss:seq:{str(owner_tg_id)}:{int(boss_id)}"
    raw = _redis_call_resp("INCR", key)
    _redis_call("EXPIRE", key, "28800")
    if not raw:
        return int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        s = str(raw).strip()
        if s.startswith(":"):
            return int(s[1:].split("\r\n")[0])
        return int(s.split("\r\n")[0])
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1000)


def redis_claim_boss_event(owner_tg_id: str, boss_id: int, event_id: str, ttl_sec: int = 28800) -> bool:
    try:
        eid = str(event_id or "").strip()[:64]
        if not eid:
            return False
        key = f"rt:boss:event:{str(owner_tg_id)}:{int(boss_id)}:{eid}"
        r = _redis_exec("SET", key, "1", "NX", "EX", str(max(60, int(ttl_sec or 28800))))
        if isinstance(r, str):
            return str(r).strip().upper() == "OK"
        return False
    except Exception:
        return False


def redis_boss_log_push(owner_tg_id: str, boss_id: int, who: str, dmg: int, event_id: str | None = None):
    key = f"dmglog:{str(owner_tg_id)}:{int(boss_id)}"
    eid = str(event_id or "").strip()[:64]
    if not eid:
        eid = uuid.uuid4().hex
    entry = json.dumps(
        {
            "event_id": eid,
            "who": str(who or "PLAYER")[:18],
            "dmg": int(max(0, int(dmg or 0))),
            "ts": int(datetime.now(timezone.utc).timestamp() * 1000),
        },
        ensure_ascii=False,
    )
    _redis_call("LPUSH", key, entry)
    _redis_call("LTRIM", key, "0", "99")
    _redis_call("EXPIRE", key, "28800")


def redis_boss_top_add(owner_tg_id: str, boss_id: int, who: str, dmg: int):
    key = f"topdmg:{str(owner_tg_id)}:{int(boss_id)}"
    _redis_call("ZINCRBY", key, str(int(max(0, int(dmg or 0)))), str(who or "PLAYER")[:18])
    _redis_call("EXPIRE", key, "28800")


def redis_boss_log_get(owner_tg_id: str, boss_id: int):
    try:
        rows = _redis_exec("LRANGE", f"dmglog:{str(owner_tg_id)}:{int(boss_id)}", "0", "49")
        out = []
        for ln in (rows if isinstance(rows, list) else []):
            s = str(ln or "").strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                out.append(
                    {
                        "event_id": str(obj.get("event_id", "") or "").strip()[:64],
                        "who": str(obj.get("who", "—"))[:18],
                        "dmg": int(obj.get("dmg", 0) or 0),
                        "ts": int(obj.get("ts", 0) or 0),
                    }
                )
            except Exception:
                continue
        return out
    except Exception:
        return []


def redis_boss_rt_clear(owner_tg_id: str, boss_id: int):
    """Новая попытка боя — старый лог/топ/seq/stream не должны смешиваться с текущим боем."""
    tg = str(owner_tg_id)
    bid = int(boss_id)
    try:
        _redis_call("DEL", f"dmglog:{tg}:{bid}")
        _redis_call("DEL", f"topdmg:{tg}:{bid}")
        _redis_call("DEL", f"rt:boss:seq:{tg}:{bid}")
        _redis_call("DEL", f"rt:boss:stream:{tg}:{bid}")
    except Exception:
        pass


def redis_boss_top_get(owner_tg_id: str, boss_id: int):
    try:
        rows = _redis_exec("ZREVRANGE", f"topdmg:{str(owner_tg_id)}:{int(boss_id)}", "0", "19", "WITHSCORES")
        lines = [str(x or "").strip() for x in (rows if isinstance(rows, list) else []) if str(x or "").strip()]
        out = []
        i = 0
        while i + 1 < len(lines):
            who = lines[i][:18]
            dmg = int(float(lines[i + 1] or 0))
            out.append({"who": who, "dmg": dmg})
            i += 2
        return out
    except Exception:
        return []


def redis_boss_event_append(owner_tg_id: str, boss_id: int, seq: int, payload: dict):
    key = f"rt:boss:stream:{str(owner_tg_id)}:{int(boss_id)}"
    try:
        seq_i = max(1, int(seq or 1))
    except Exception:
        seq_i = 1
    event = json.dumps(payload or {}, ensure_ascii=False)
    _redis_call("ZADD", key, str(seq_i), event)
    # Keep newest 300 events per boss stream.
    _redis_call("ZREMRANGEBYRANK", key, "0", "-301")
    _redis_call("EXPIRE", key, "28800")


def redis_boss_events_after_seq(owner_tg_id: str, boss_id: int, since_seq: int, limit: int = 120):
    start_score = max(0, int(since_seq or 0)) + 1
    lim = max(1, min(300, int(limit or 120)))
    key = f"rt:boss:stream:{str(owner_tg_id)}:{int(boss_id)}"
    try:
        rows = _redis_exec("ZRANGEBYSCORE", key, str(start_score), "+inf", "LIMIT", "0", str(lim))
        out = []
        for ln in (rows if isinstance(rows, list) else []):
            s = str(ln or "").strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                continue
        return out
    except Exception:
        return []


async def _ws_register(tg_id: str, ws: web.WebSocketResponse):
    async with WS_LOCK:
        WS_CLIENTS[str(tg_id)].add(ws)


async def _ws_unregister(tg_id: str, ws: web.WebSocketResponse):
    async with WS_LOCK:
        s = WS_CLIENTS.get(str(tg_id))
        if s and ws in s:
            s.remove(ws)
        if s is not None and not s:
            WS_CLIENTS.pop(str(tg_id), None)


async def _ws_send_to_tg(tg_id: str, payload: dict):
    async with WS_LOCK:
        clients = list(WS_CLIENTS.get(str(tg_id), set()))
    dead = []
    for ws in clients:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    if dead:
        async with WS_LOCK:
            s = WS_CLIENTS.get(str(tg_id))
            if s:
                for ws in dead:
                    s.discard(ws)
                if not s:
                    WS_CLIENTS.pop(str(tg_id), None)


def _resp_pack(*parts):
    vals = [str(x) for x in parts]
    return ("*" + str(len(vals)) + "\r\n" + "".join(["$" + str(len(v.encode("utf-8"))) + "\r\n" + v + "\r\n" for v in vals])).encode("utf-8")


async def _redis_async_readline(reader):
    return await reader.readuntil(b"\r\n")


async def _redis_async_readexact(reader, n: int):
    return await reader.readexactly(max(0, int(n or 0)))


async def _redis_async_parse(reader):
    head = await _redis_async_readline(reader)
    if not head:
        return None
    pfx = head[:1]
    body = head[1:-2].decode("utf-8", errors="ignore") if len(head) >= 2 else ""
    if pfx == b"+":
        return body
    if pfx == b"-":
        return None
    if pfx == b":":
        try:
            return int(body or 0)
        except Exception:
            return 0
    if pfx == b"$":
        try:
            ln = int(body or -1)
        except Exception:
            ln = -1
        if ln < 0:
            return None
        data = await _redis_async_readexact(reader, ln)
        await _redis_async_readexact(reader, 2)  # CRLF
        return data.decode("utf-8", errors="ignore")
    if pfx == b"*":
        try:
            cnt = int(body or 0)
        except Exception:
            cnt = 0
        arr = []
        for _i in range(max(0, cnt)):
            arr.append(await _redis_async_parse(reader))
        return arr
    return None


async def redis_pubsub_loop(app: web.Application):
    # Cross-instance realtime fanout via Redis Pub/Sub (no redis-cli dependency).
    while True:
        writer = None
        try:
            reader, writer = await asyncio.open_connection(REDIS_HOST, REDIS_PORT)
            if REDIS_PASS:
                writer.write(_resp_pack("AUTH", REDIS_PASS))
                await writer.drain()
                await _redis_async_parse(reader)
            writer.write(_resp_pack("PSUBSCRIBE", "rt:player:save:*", "rt:boss:hit:*", "rt:boss:help:*"))
            await writer.drain()
            while True:
                msg = await _redis_async_parse(reader)
                if not isinstance(msg, list) or len(msg) < 1:
                    continue
                msg_type = str(msg[0] or "")
                if msg_type in {"psubscribe", "subscribe", "pong"}:
                    continue
                if msg_type != "pmessage" or len(msg) < 4:
                    continue
                channel = str(msg[2] or "")
                payload_raw = msg[3]
                try:
                    data = json.loads(payload_raw) if payload_raw else {}
                except Exception:
                    data = {}
                if channel.startswith("rt:player:save:"):
                    tg_id = channel.split(":")[-1]
                    await _ws_send_to_tg(tg_id, {"type": "save_ack", "data": data})
                elif channel.startswith("rt:boss:hit:"):
                    tg_id = channel.split(":")[-1]
                    await _ws_send_to_tg(tg_id, {"type": "boss_update", "data": data})
                elif channel.startswith("rt:boss:help:"):
                    tg_id = channel.split(":")[-1]
                    await _ws_send_to_tg(tg_id, {"type": "boss_help_event", "data": data})
        except asyncio.CancelledError:
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
            break
        except Exception:
            pass
        finally:
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
        await asyncio.sleep(1.0)


def fetch_player(tg_id: str):
    sql = """
    select tg_id,name,photo_url,arena_power,level,stats_sum,boss_wins,state,
           active_session_id,active_session_updated_at,active_device_id,updated_at,state_version
    from public.players
    where tg_id = %s
    limit 1
    """
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (str(tg_id),))
            row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return {
            "tg_id": row[0],
            "name": row[1],
            "photo_url": row[2],
            "arena_power": row[3],
            "level": row[4],
            "stats_sum": row[5],
            "boss_wins": row[6],
            "state": row[7],
            "active_session_id": row[8],
            "active_session_updated_at": row[9].isoformat() if row[9] else None,
            "active_device_id": row[10],
            "updated_at": row[11].isoformat() if row[11] else None,
            "state_version": int(row[12] or 0),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def fetch_player_admin_full(tg_id: str):
    sql = """
    select tg_id,name,photo_url,level,xp,gold,silver,tooth,district_fear_total,
           arena_power,arena_wins,arena_losses,stats_sum,boss_wins,state,updated_at,
           state_version,active_session_id,active_session_updated_at,active_device_id
    from public.players
    where tg_id = %s
    limit 1
    """
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (str(tg_id),))
            row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return {
            "tg_id": row[0],
            "name": row[1],
            "photo_url": row[2],
            "level": int(row[3] or 1),
            "xp": int(row[4] or 0),
            "gold": int(row[5] or 0),
            "silver": int(row[6] or 0),
            "tooth": int(row[7] or 0),
            "district_fear_total": int(row[8] or 0),
            "arena_power": int(row[9] or 0),
            "arena_wins": int(row[10] or 0),
            "arena_losses": int(row[11] or 0),
            "stats_sum": int(row[12] or 0),
            "boss_wins": int(row[13] or 0),
            "state": row[14] if isinstance(row[14], dict) else {},
            "updated_at": row[15].isoformat() if row[15] else None,
            "state_version": int(row[16] or 0),
            "active_session_id": row[17],
            "active_session_updated_at": row[18].isoformat() if row[18] else None,
            "active_device_id": row[19],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def fetch_player_by_name(name: str):
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            q = str(name or "").strip()
            if not q:
                conn.commit()
                return None
            cur.execute(
                """
                select tg_id
                from public.players
                where lower(name) = lower(%s)
                order by updated_at desc
                limit 1
                """,
                (q,),
            )
            row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return fetch_player_admin_full(str(row[0]))
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def admin_list_players(query: str = "", limit: int = 50, offset: int = 0, filters: dict | None = None):
    conn = _db_conn()
    try:
        lim = max(1, min(200, int(limit or 50)))
        off = max(0, int(offset or 0))
        q = str(query or "").strip()
        f = filters if isinstance(filters, dict) else {}
        clan_id = str(f.get("clan_id", "") or "").strip().upper()
        min_level = f.get("min_level")
        max_level = f.get("max_level")
        active_within_hours = f.get("active_within_hours")
        with conn.cursor() as cur:
            where_parts = []
            params = []
            if q:
                like = f"%{q}%"
                where_parts.append("(tg_id ilike %s or name ilike %s)")
                params.extend([like, like])
            if clan_id:
                where_parts.append("(coalesce(state->'clan'->>'id','') = %s)")
                params.append(clan_id)
            if min_level is not None:
                where_parts.append("(coalesce(level,1) >= %s)")
                params.append(max(1, int(min_level or 1)))
            if max_level is not None:
                where_parts.append("(coalesce(level,1) <= %s)")
                params.append(max(1, int(max_level or 1)))
            if active_within_hours is not None:
                hours = max(1, int(active_within_hours or 1))
                where_parts.append("(updated_at >= now() - (%s * interval '1 hour'))")
                params.append(hours)
            where_sql = (" where " + " and ".join(where_parts)) if where_parts else ""
            sql = f"""
                select tg_id, name, level, xp, gold, silver, tooth, boss_wins, state_version, updated_at
                from public.players
                {where_sql}
                order by updated_at desc
                limit %s offset %s
            """
            params.extend([lim, off])
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        conn.commit()
        out = []
        for r in rows:
            out.append(
                {
                    "tg_id": str(r[0] or ""),
                    "name": str(r[1] or ""),
                    "level": int(r[2] or 1),
                    "xp": int(r[3] or 0),
                    "gold": int(r[4] or 0),
                    "silver": int(r[5] or 0),
                    "tooth": int(r[6] or 0),
                    "boss_wins": int(r[7] or 0),
                    "state_version": int(r[8] or 0),
                    "updated_at": r[9].isoformat() if r[9] else None,
                }
            )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def admin_write_player_state(actor_tg_id: str, target_tg_id: str, state_obj: dict, note_action: str):
    row = fetch_player_admin_full(target_tg_id)
    if not row:
        return {"ok": False, "error": "player_not_found"}
    s = state_obj if isinstance(state_obj, dict) else {}
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("select coalesce(state_version,0) from public.players where tg_id=%s limit 1", (str(target_tg_id),))
            sv_row = cur.fetchone()
            cur_sv = int(sv_row[0] or 0) if sv_row else 0
            next_sv = cur_sv + 1
            cur.execute(
                """
                update public.players
                set
                  name=%s,
                  photo_url=%s,
                  level=%s,
                  xp=%s,
                  gold=%s,
                  silver=%s,
                  tooth=%s,
                  district_fear_total=%s,
                  arena_power=%s,
                  arena_wins=%s,
                  arena_losses=%s,
                  stats_sum=%s,
                  boss_wins=%s,
                  state=%s::jsonb,
                  state_version=%s,
                  updated_at=now()
                where tg_id=%s
                """,
                (
                    str(s.get("playerName", row.get("name", "Player")) or "Player")[:18],
                    str(row.get("photo_url", "") or ""),
                    int(s.get("level", row.get("level", 1)) or 1),
                    int(s.get("totalXp", s.get("xp", row.get("xp", 0))) or 0),
                    int(s.get("gold", row.get("gold", 0)) or 0),
                    int(s.get("silver", row.get("silver", 0)) or 0),
                    int(s.get("tooth", row.get("tooth", 0)) or 0),
                    int(sum(int(v or 0) for v in ((s.get("districtFear", {}) or {}).values())) if isinstance(s.get("districtFear"), dict) else row.get("district_fear_total", 0)),
                    int(((s.get("arena", {}) or {}).get("power", row.get("arena_power", 0)) or 0)),
                    int(((s.get("arena", {}) or {}).get("wins", row.get("arena_wins", 0)) or 0)),
                    int(((s.get("arena", {}) or {}).get("losses", row.get("arena_losses", 0)) or 0)),
                    int(row.get("stats_sum", 0) or 0),
                    int(((s.get("bosses", {}) or {}).get("wins", row.get("boss_wins", 0)) or 0)),
                    json.dumps(s, ensure_ascii=False),
                    int(next_sv),
                    str(target_tg_id),
                ),
            )
        conn.commit()
        admin_audit_log(actor_tg_id, note_action, target_tg_id, {"next_state_version": int(next_sv)})
        return {"ok": True, "target_tg_id": str(target_tg_id), "state_version": int(next_sv)}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def admin_make_player_god(actor_tg_id: str, target_tg_id: str):
    row = fetch_player_admin_full(target_tg_id)
    if not row:
        return {"ok": False, "error": "player_not_found"}
    s = row.get("state", {}) if isinstance(row.get("state", {}), dict) else {}
    s["level"] = max(999, int(s.get("level", 1) or 1))
    s["totalXp"] = max(999999999, int(s.get("totalXp", s.get("xp", 0)) or 0))
    s["xp"] = int(s.get("totalXp", 0) or 0)
    s["gold"] = max(999999999, int(s.get("gold", 0) or 0))
    s["silver"] = max(999999999, int(s.get("silver", 0) or 0))
    s["tooth"] = max(999999999, int(s.get("tooth", 0) or 0))
    s["rings"] = max(999999999, int(s.get("rings", 0) or 0))
    gym = s.get("gym", {}) if isinstance(s.get("gym", {}), dict) else {}
    for k in ["health", "strength", "agility", "initiative", "endurance", "might", "charisma"]:
        gym[k] = max(5000, int(gym.get(k, 1) or 1))
    s["gym"] = gym
    bosses = s.get("bosses", {}) if isinstance(s.get("bosses", {}), dict) else {}
    hs = bosses.get("hitStocks", {}) if isinstance(bosses.get("hitStocks", {}), dict) else {}
    hs["kick"] = max(9999, int(hs.get("kick", 0) or 0))
    hs["knuckle"] = max(9999, int(hs.get("knuckle", 0) or 0))
    hs["enema"] = max(9999, int(hs.get("enema", 0) or 0))
    bosses["hitStocks"] = hs
    bosses["wins"] = max(9999, int(bosses.get("wins", 0) or 0))
    s["bosses"] = bosses
    return admin_write_player_state(actor_tg_id, target_tg_id, s, "player_godmode")


def admin_reset_player_state(actor_tg_id: str, target_tg_id: str):
    row = fetch_player_admin_full(target_tg_id)
    if not row:
        return {"ok": False, "error": "player_not_found"}
    try:
        player_progress_log("admin_reset_state", str(target_tg_id), actor_tg_id=str(actor_tg_id))
    except Exception:
        pass
    s = {}
    return admin_write_player_state(actor_tg_id, target_tg_id, s, "player_reset")


def admin_bulk_grant(actor_tg_id: str, payload: dict):
    q = str((payload or {}).get("query", "") or "").strip()
    limit = int((payload or {}).get("limit", 5000) or 5000)
    min_level = (payload or {}).get("min_level")
    max_level = (payload or {}).get("max_level")
    clan_id = str((payload or {}).get("clan_id", "") or "").strip().upper()
    active_within_hours = (payload or {}).get("active_within_hours")
    dry_run = bool((payload or {}).get("dry_run", False))
    confirm_token = str((payload or {}).get("confirm_token", "") or "").strip().upper()
    filters = {
        "min_level": min_level,
        "max_level": max_level,
        "clan_id": clan_id,
        "active_within_hours": active_within_hours,
    }
    players = admin_list_players(q, limit, 0, filters)
    preset = str((payload or {}).get("preset", "") or "").strip().upper()
    gold_add = int((payload or {}).get("gold_add", 0) or 0)
    silver_add = int((payload or {}).get("silver_add", 0) or 0)
    tooth_add = int((payload or {}).get("tooth_add", 0) or 0)
    level_set = (payload or {}).get("level_set")
    if preset == "ECONOMY_BOOST":
        gold_add = max(gold_add, 50000)
        silver_add = max(silver_add, 50000)
        tooth_add = max(tooth_add, 5000)
    elif preset == "PVP_MAX":
        level_set = max(120, int(level_set or 120))
        gold_add = max(gold_add, 20000)
        silver_add = max(silver_add, 20000)
    elif preset == "BOSS_MAX":
        level_set = max(200, int(level_set or 200))
        tooth_add = max(tooth_add, 20000)
        gold_add = max(gold_add, 10000)
    sample = [{"tg_id": str(p.get("tg_id", "")), "name": str(p.get("name", "")), "level": int(p.get("level", 1) or 1)} for p in players[:50]]
    if dry_run:
        return {"ok": True, "dry_run": True, "matched": int(len(players)), "sample": sample}
    if confirm_token != "APPLY":
        return {"ok": False, "error": "confirm_token_required", "hint": "set confirm_token=APPLY"}
    updated = 0
    for p in players:
        tid = str(p.get("tg_id", ""))
        row = fetch_player_admin_full(tid)
        if not row:
            continue
        s = row.get("state", {}) if isinstance(row.get("state", {}), dict) else {}
        s["gold"] = max(0, int(s.get("gold", row.get("gold", 0)) or 0) + gold_add)
        s["silver"] = max(0, int(s.get("silver", row.get("silver", 0)) or 0) + silver_add)
        s["tooth"] = max(0, int(s.get("tooth", row.get("tooth", 0)) or 0) + tooth_add)
        if level_set is not None:
            s["level"] = max(1, int(level_set or 1))
        res = admin_write_player_state(actor_tg_id, tid, s, "bulk_grant")
        if res and res.get("ok"):
            updated += 1
    admin_audit_log(
        actor_tg_id,
        "bulk_grant_summary",
        "",
        {
            "updated": int(updated),
            "query": q,
            "clan_id": clan_id,
            "min_level": min_level,
            "max_level": max_level,
            "active_within_hours": active_within_hours,
            "gold_add": gold_add,
            "silver_add": silver_add,
            "tooth_add": tooth_add,
            "level_set": level_set,
            "preset": preset,
        },
    )
    return {"ok": True, "updated": int(updated), "matched": int(len(players))}


def promo_norm(code: str) -> str:
    return "".join(str(code or "").strip().upper().split())


def _promo_category_norm(v: str) -> str:
    x = str(v or "all").strip().lower()
    return x if x in {"all", "newbie", "vip", "clan"} else "all"


def _promo_target_mode_norm(v: str) -> str:
    x = str(v or "all").strip().lower()
    return x if x in {"all", "private"} else "all"


def _player_flags(level: int, state_obj: dict):
    st = state_obj if isinstance(state_obj, dict) else {}
    lvl = max(1, int(level or 1))
    is_newbie = lvl <= 10
    vip = False
    try:
        if bool(st.get("vip", False)):
            vip = True
        elif bool(st.get("vipActive", False)):
            vip = True
        else:
            vu = int(st.get("vipUntil", 0) or 0)
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            vip = vu > now_ms
    except Exception:
        vip = False
    has_clan = False
    try:
        c = st.get("clan", {})
        if isinstance(c, dict) and str(c.get("id", "")).strip():
            has_clan = True
    except Exception:
        has_clan = False
    return {"newbie": is_newbie, "vip": vip, "clan": has_clan}


def promo_create_or_update(actor_tg_id: str, payload: dict):
    code = promo_norm((payload or {}).get("code", ""))
    if not code:
        return {"ok": False, "error": "missing_code"}
    rewards = (payload or {}).get("rewards", {})
    if not isinstance(rewards, dict):
        rewards = {}
    def _i(v):
        try:
            return max(0, int(v or 0))
        except Exception:
            return 0
    rewards_clean = {
        "gold": _i(rewards.get("gold", 0)),
        "silver": _i(rewards.get("silver", 0)),
        "tooth": _i(rewards.get("tooth", 0)),
        "rings": _i(rewards.get("rings", 0)),
    }
    max_total_uses = _i((payload or {}).get("max_total_uses", 0))
    max_per_user = max(1, _i((payload or {}).get("max_per_user", 1)) or 1)
    starts_at = (payload or {}).get("starts_at")
    ends_at = (payload or {}).get("ends_at")
    title = str((payload or {}).get("title", "") or "")[:120]
    note = str((payload or {}).get("note", "") or "")[:300]
    active = bool((payload or {}).get("active", True))
    category = _promo_category_norm((payload or {}).get("category", "all"))
    target_mode = _promo_target_mode_norm((payload or {}).get("target_mode", "all"))
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.promo_codes(
                  code, title, note, rewards, max_total_uses, max_per_user, starts_at, ends_at, active, category, target_mode, created_by_tg_id, updated_at
                )
                values (%s,%s,%s,%s::jsonb,%s,%s,%s::timestamptz,%s::timestamptz,%s,%s,%s,%s,now())
                on conflict (code) do update set
                  title=excluded.title,
                  note=excluded.note,
                  rewards=excluded.rewards,
                  max_total_uses=excluded.max_total_uses,
                  max_per_user=excluded.max_per_user,
                  starts_at=excluded.starts_at,
                  ends_at=excluded.ends_at,
                  active=excluded.active,
                  category=excluded.category,
                  target_mode=excluded.target_mode,
                  updated_at=now()
                returning code, active, used_total, max_total_uses, max_per_user, starts_at, ends_at, rewards, category, target_mode
                """,
                (
                    code,
                    title,
                    note,
                    json.dumps(rewards_clean, ensure_ascii=False),
                    int(max_total_uses),
                    int(max_per_user),
                    starts_at,
                    ends_at,
                    bool(active),
                    str(category),
                    str(target_mode),
                    str(actor_tg_id or ""),
                ),
            )
            row = cur.fetchone()
        conn.commit()
        admin_audit_log(
            actor_tg_id,
            "promo_upsert",
            "",
            {
                "code": code,
                "active": bool(active),
                "category": category,
                "target_mode": target_mode,
                "max_total_uses": int(max_total_uses),
                "max_per_user": int(max_per_user),
            },
        )
        return {
            "ok": True,
            "promo": {
                "code": str(row[0] or code),
                "active": bool(row[1]),
                "used_total": int(row[2] or 0),
                "max_total_uses": int(row[3] or 0),
                "max_per_user": int(row[4] or 1),
                "starts_at": row[5].isoformat() if row[5] else None,
                "ends_at": row[6].isoformat() if row[6] else None,
                "rewards": row[7] if isinstance(row[7], dict) else rewards_clean,
                "category": str(row[8] or category),
                "target_mode": str(row[9] or target_mode),
            },
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_list(limit: int = 100):
    conn = _db_conn()
    try:
        lim = max(1, min(300, int(limit or 100)))
        with conn.cursor() as cur:
            cur.execute(
                """
                select code, title, note, rewards, active, used_total, max_total_uses, max_per_user, starts_at, ends_at, created_at, updated_at, category, target_mode
                from public.promo_codes
                order by updated_at desc nulls last, created_at desc
                limit %s
                """,
                (lim,),
            )
            rows = cur.fetchall() or []
        conn.commit()
        out = []
        for r in rows:
            out.append(
                {
                    "code": str(r[0] or ""),
                    "title": str(r[1] or ""),
                    "note": str(r[2] or ""),
                    "rewards": r[3] if isinstance(r[3], dict) else {},
                    "active": bool(r[4]),
                    "used_total": int(r[5] or 0),
                    "max_total_uses": int(r[6] or 0),
                    "max_per_user": int(r[7] or 1),
                    "starts_at": r[8].isoformat() if r[8] else None,
                    "ends_at": r[9].isoformat() if r[9] else None,
                    "created_at": r[10].isoformat() if r[10] else None,
                    "updated_at": r[11].isoformat() if r[11] else None,
                    "category": str(r[12] or "all"),
                    "target_mode": str(r[13] or "all"),
                }
            )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_apply_to_player(tg_id: str, code_raw: str, source: str = "manual", event_type: str = "", event_key: str = ""):
    tg = str(tg_id or "").strip()
    code = promo_norm(code_raw)
    if not tg:
        return {"ok": False, "error": "missing_tg_id"}
    if not code:
        return {"ok": False, "error": "missing_code"}
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select code, rewards, active, used_total, max_total_uses, max_per_user, starts_at, ends_at, category, target_mode
                from public.promo_codes
                where code=%s
                for update
                """,
                (code,),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return {"ok": False, "error": "promo_not_found"}
            rewards = row[1] if isinstance(row[1], dict) else {}
            is_active = bool(row[2])
            used_total = int(row[3] or 0)
            max_total = int(row[4] or 0)
            max_per_user = max(1, int(row[5] or 1))
            starts_at = row[6]
            ends_at = row[7]
            category = _promo_category_norm(row[8] if len(row) > 8 else "all")
            target_mode = _promo_target_mode_norm(row[9] if len(row) > 9 else "all")
            cur.execute("select now()")
            now_row = cur.fetchone()
            now_ts = now_row[0] if now_row else None
            if not is_active:
                conn.rollback()
                return {"ok": False, "error": "promo_inactive"}
            if starts_at and now_ts and now_ts < starts_at:
                conn.rollback()
                return {"ok": False, "error": "promo_not_started"}
            if ends_at and now_ts and now_ts > ends_at:
                conn.rollback()
                return {"ok": False, "error": "promo_expired"}
            if max_total > 0 and used_total >= max_total:
                conn.rollback()
                return {"ok": False, "error": "promo_limit_reached"}
            cur.execute(
                """
                select count(1) from public.promo_code_redemptions
                where code=%s and tg_id=%s
                """,
                (code, tg),
            )
            per_user_used = int((cur.fetchone() or [0])[0] or 0)
            if per_user_used >= max_per_user:
                conn.rollback()
                return {"ok": False, "error": "promo_already_used"}
            cur.execute(
                """
                select coalesce(level,1), coalesce(xp,0), coalesce(gold,0), coalesce(silver,0), coalesce(tooth,0), coalesce(state,'{}'::jsonb), coalesce(state_version,0)
                from public.players
                where tg_id=%s
                for update
                """,
                (tg,),
            )
            p = cur.fetchone()
            if not p:
                conn.rollback()
                return {"ok": False, "error": "player_not_found"}
            level, xp, gold, silver, tooth, state_json, state_version = p
            st = state_json if isinstance(state_json, dict) else {}
            flags = _player_flags(int(level or 1), st)
            if category != "all" and not bool(flags.get(category, False)):
                conn.rollback()
                return {"ok": False, "error": "promo_category_mismatch"}
            if target_mode == "private":
                cur.execute(
                    "select 1 from public.promo_code_targets where code=%s and tg_id=%s limit 1",
                    (code, tg),
                )
                if not cur.fetchone():
                    conn.rollback()
                    return {"ok": False, "error": "promo_private_forbidden"}
            def _i(v):
                try:
                    return max(0, int(v or 0))
                except Exception:
                    return 0
            rg = _i(rewards.get("gold", 0))
            rs = _i(rewards.get("silver", 0))
            rt = _i(rewards.get("tooth", 0))
            rr = _i(rewards.get("rings", 0))
            gold_new = max(0, int(gold or 0) + rg)
            silver_new = max(0, int(silver or 0) + rs)
            tooth_new = max(0, int(tooth or 0) + rt)
            rings_old = 0
            try:
                rings_old = max(0, int(st.get("rings", 0) or 0))
            except Exception:
                rings_old = 0
            rings_new = rings_old + rr
            st["gold"] = int(gold_new)
            st["silver"] = int(silver_new)
            st["tooth"] = int(tooth_new)
            st["rings"] = int(rings_new)
            pc = st.get("promoUsedCodes", {}) if isinstance(st.get("promoUsedCodes", {}), dict) else {}
            pc[code] = now_iso()
            st["promoUsedCodes"] = pc
            next_sv = int(state_version or 0) + 1
            cur.execute(
                """
                update public.players
                set gold=%s, silver=%s, tooth=%s, state=%s::jsonb, state_version=%s, updated_at=now()
                where tg_id=%s
                """,
                (gold_new, silver_new, tooth_new, json.dumps(st, ensure_ascii=False), next_sv, tg),
            )
            cur.execute(
                """
                insert into public.promo_code_redemptions(code, tg_id, rewards, source, event_type, event_key)
                values (%s,%s,%s::jsonb,%s,%s,%s)
                """,
                (
                    code,
                    tg,
                    json.dumps({"gold": rg, "silver": rs, "tooth": rt, "rings": rr}, ensure_ascii=False),
                    str(source or "manual")[:24],
                    (str(event_type or "")[:32] or None),
                    (str(event_key or "")[:80] or None),
                ),
            )
            cur.execute(
                "update public.promo_codes set used_total = coalesce(used_total,0) + 1, updated_at=now() where code=%s",
                (code,),
            )
        conn.commit()
        return {
            "ok": True,
            "code": code,
            "rewards": {"gold": rg, "silver": rs, "tooth": rt, "rings": rr},
            "balances": {"gold": int(gold_new), "silver": int(silver_new), "tooth": int(tooth_new), "rings": int(rings_new)},
            "state_version": int(next_sv),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_targets_set(actor_tg_id: str, code_raw: str, tg_ids: list[str]):
    code = promo_norm(code_raw)
    if not code:
        return {"ok": False, "error": "missing_code"}
    norm_ids = []
    seen = set()
    for x in (tg_ids or []):
        t = str(x or "").strip()
        if not t or t in seen:
            continue
        seen.add(t)
        norm_ids.append(t)
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("select 1 from public.promo_codes where code=%s limit 1", (code,))
            if not cur.fetchone():
                conn.rollback()
                return {"ok": False, "error": "promo_not_found"}
            cur.execute("delete from public.promo_code_targets where code=%s", (code,))
            for tid in norm_ids:
                cur.execute(
                    "insert into public.promo_code_targets(code, tg_id) values (%s,%s) on conflict do nothing",
                    (code, tid),
                )
        conn.commit()
        admin_audit_log(actor_tg_id, "promo_targets_set", "", {"code": code, "count": int(len(norm_ids))})
        return {"ok": True, "code": code, "count": int(len(norm_ids))}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_targets_get(code_raw: str, limit: int = 5000):
    code = promo_norm(code_raw)
    if not code:
        return []
    conn = _db_conn()
    try:
        lim = max(1, min(20000, int(limit or 5000)))
        with conn.cursor() as cur:
            cur.execute(
                "select tg_id from public.promo_code_targets where code=%s order by tg_id asc limit %s",
                (code, lim),
            )
            rows = cur.fetchall() or []
        conn.commit()
        return [str(r[0] or "") for r in rows]
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_auto_rule_upsert(actor_tg_id: str, payload: dict):
    event_type = str((payload or {}).get("event_type", "") or "").strip().lower()
    if event_type not in {"first_login", "boss_win", "holiday"}:
        return {"ok": False, "error": "bad_event_type"}
    event_key = str((payload or {}).get("event_key", "") or "").strip()[:80]
    promo_code = promo_norm((payload or {}).get("promo_code", ""))
    if not promo_code:
        return {"ok": False, "error": "missing_promo_code"}
    active = bool((payload or {}).get("active", True))
    note = str((payload or {}).get("note", "") or "")[:300]
    rid = (payload or {}).get("id")
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("select 1 from public.promo_codes where code=%s limit 1", (promo_code,))
            if not cur.fetchone():
                conn.rollback()
                return {"ok": False, "error": "promo_not_found"}
            if rid is None:
                cur.execute(
                    """
                    insert into public.promo_auto_rules(event_type, event_key, promo_code, active, note, created_by_tg_id, updated_at)
                    values (%s,%s,%s,%s,%s,%s,now())
                    returning id
                    """,
                    (event_type, event_key, promo_code, active, note, str(actor_tg_id or "")),
                )
                new_id = int((cur.fetchone() or [0])[0] or 0)
            else:
                cur.execute(
                    """
                    update public.promo_auto_rules
                    set event_type=%s, event_key=%s, promo_code=%s, active=%s, note=%s, updated_at=now()
                    where id=%s
                    returning id
                    """,
                    (event_type, event_key, promo_code, active, note, int(rid)),
                )
                rr = cur.fetchone()
                if not rr:
                    conn.rollback()
                    return {"ok": False, "error": "rule_not_found"}
                new_id = int(rr[0] or 0)
        conn.commit()
        admin_audit_log(actor_tg_id, "promo_auto_rule_upsert", "", {"id": int(new_id), "event_type": event_type, "event_key": event_key, "promo_code": promo_code, "active": bool(active)})
        return {"ok": True, "id": int(new_id)}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_auto_rule_list(limit: int = 200):
    conn = _db_conn()
    try:
        lim = max(1, min(1000, int(limit or 200)))
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, event_type, event_key, promo_code, active, note, created_by_tg_id, created_at, updated_at
                from public.promo_auto_rules
                order by updated_at desc, id desc
                limit %s
                """,
                (lim,),
            )
            rows = cur.fetchall() or []
        conn.commit()
        out = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0] or 0),
                    "event_type": str(r[1] or ""),
                    "event_key": str(r[2] or ""),
                    "promo_code": str(r[3] or ""),
                    "active": bool(r[4]),
                    "note": str(r[5] or ""),
                    "created_by_tg_id": str(r[6] or ""),
                    "created_at": r[7].isoformat() if r[7] else None,
                    "updated_at": r[8].isoformat() if r[8] else None,
                }
            )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_auto_rule_delete(actor_tg_id: str, rid: int):
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("delete from public.promo_auto_rules where id=%s returning id", (int(rid),))
            row = cur.fetchone()
        conn.commit()
        if not row:
            return {"ok": False, "error": "rule_not_found"}
        admin_audit_log(actor_tg_id, "promo_auto_rule_delete", "", {"id": int(rid)})
        return {"ok": True, "id": int(rid)}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_run_auto_event(tg_id: str, event_type: str, event_key: str = ""):
    tg = str(tg_id or "").strip()
    ev = str(event_type or "").strip().lower()
    ek = str(event_key or "").strip()[:80]
    if not tg or ev not in {"first_login", "boss_win", "holiday"}:
        return {"ok": True, "applied": []}
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, promo_code
                from public.promo_auto_rules
                where active=true
                  and event_type=%s
                  and (coalesce(event_key,'')='' or event_key=%s)
                order by id asc
                """,
                (ev, ek),
            )
            rules = cur.fetchall() or []
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)
    applied = []
    for rr in rules:
        rid = int(rr[0] or 0)
        code = str(rr[1] or "")
        conn2 = _db_conn()
        try:
            with conn2.cursor() as cur2:
                cur2.execute(
                    """
                    insert into public.promo_auto_grants(tg_id, promo_code, event_type, event_key)
                    values (%s,%s,%s,%s)
                    on conflict do nothing
                    returning id
                    """,
                    (tg, code, ev, ek),
                )
                ins = cur2.fetchone()
            conn2.commit()
            if not ins:
                continue
        except Exception:
            conn2.rollback()
            continue
        finally:
            _db_put(conn2)
        res = promo_apply_to_player(tg, code, source="auto", event_type=ev, event_key=ek)
        if res and res.get("ok"):
            applied.append({"rule_id": rid, "code": code, "rewards": res.get("rewards", {})})
    return {"ok": True, "applied": applied}


def promo_report(code_raw: str = "", limit: int = 200):
    code = promo_norm(code_raw)
    conn = _db_conn()
    try:
        lim = max(1, min(1000, int(limit or 200)))
        with conn.cursor() as cur:
            if code:
                cur.execute(
                    """
                    select code, tg_id, rewards, source, event_type, event_key, created_at
                    from public.promo_code_redemptions
                    where code=%s
                    order by created_at desc
                    limit %s
                    """,
                    (code, lim),
                )
                rows = cur.fetchall() or []
                cur.execute(
                    """
                    select count(1),
                      coalesce(sum((rewards->>'gold')::bigint),0),
                      coalesce(sum((rewards->>'silver')::bigint),0),
                      coalesce(sum((rewards->>'tooth')::bigint),0),
                      coalesce(sum((rewards->>'rings')::bigint),0)
                    from public.promo_code_redemptions
                    where code=%s
                    """,
                    (code,),
                )
                s = cur.fetchone() or [0, 0, 0, 0, 0]
                summary = {
                    "code": code,
                    "redeemed_total": int(s[0] or 0),
                    "issued": {"gold": int(s[1] or 0), "silver": int(s[2] or 0), "tooth": int(s[3] or 0), "rings": int(s[4] or 0)},
                }
            else:
                cur.execute(
                    """
                    select code, tg_id, rewards, source, event_type, event_key, created_at
                    from public.promo_code_redemptions
                    order by created_at desc
                    limit %s
                    """,
                    (lim,),
                )
                rows = cur.fetchall() or []
                cur.execute(
                    """
                    select
                      coalesce(sum((rewards->>'gold')::bigint),0),
                      coalesce(sum((rewards->>'silver')::bigint),0),
                      coalesce(sum((rewards->>'tooth')::bigint),0),
                      coalesce(sum((rewards->>'rings')::bigint),0),
                      count(1)
                    from public.promo_code_redemptions
                    """
                )
                s = cur.fetchone() or [0, 0, 0, 0, 0]
                summary = {
                    "code": "",
                    "redeemed_total": int(s[4] or 0),
                    "issued": {"gold": int(s[0] or 0), "silver": int(s[1] or 0), "tooth": int(s[2] or 0), "rings": int(s[3] or 0)},
                }
        conn.commit()
        events = []
        for r in rows:
            events.append(
                {
                    "code": str(r[0] or ""),
                    "tg_id": str(r[1] or ""),
                    "rewards": r[2] if isinstance(r[2], dict) else {},
                    "source": str(r[3] or ""),
                    "event_type": str(r[4] or ""),
                    "event_key": str(r[5] or ""),
                    "created_at": r[6].isoformat() if r[6] else None,
                }
            )
        return {"ok": True, "summary": summary, "events": events}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_campaign_upsert(actor_tg_id: str, payload: dict):
    cid = (payload or {}).get("id")
    title = str((payload or {}).get("title", "") or "")[:120]
    promo_code = promo_norm((payload or {}).get("promo_code", ""))
    starts_at = (payload or {}).get("starts_at")
    ends_at = (payload or {}).get("ends_at")
    force_active = bool((payload or {}).get("force_active", False))
    active = bool((payload or {}).get("active", True))
    note = str((payload or {}).get("note", "") or "")[:300]
    if not promo_code:
        return {"ok": False, "error": "missing_promo_code"}
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("select 1 from public.promo_codes where code=%s limit 1", (promo_code,))
            if not cur.fetchone():
                conn.rollback()
                return {"ok": False, "error": "promo_not_found"}
            if cid is None:
                cur.execute(
                    """
                    insert into public.promo_campaigns(title,promo_code,starts_at,ends_at,force_active,active,last_state,note,created_by_tg_id,updated_at)
                    values (%s,%s,nullif(%s,'')::timestamptz,nullif(%s,'')::timestamptz,%s,%s,'unknown',%s,%s,now())
                    returning id
                    """,
                    (title, promo_code, starts_at, ends_at, force_active, active, note, str(actor_tg_id or "")),
                )
                rid = int((cur.fetchone() or [0])[0] or 0)
            else:
                cur.execute(
                    """
                    update public.promo_campaigns
                    set title=%s,promo_code=%s,starts_at=nullif(%s,'')::timestamptz,ends_at=nullif(%s,'')::timestamptz,force_active=%s,active=%s,note=%s,updated_at=now()
                    where id=%s
                    returning id
                    """,
                    (title, promo_code, starts_at, ends_at, force_active, active, note, int(cid)),
                )
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    return {"ok": False, "error": "campaign_not_found"}
                rid = int(row[0] or 0)
        conn.commit()
        admin_audit_log(actor_tg_id, "promo_campaign_upsert", "", {"id": int(rid), "promo_code": promo_code, "active": bool(active), "force_active": bool(force_active)})
        return {"ok": True, "id": int(rid)}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_campaign_list(limit: int = 200):
    conn = _db_conn()
    try:
        lim = max(1, min(1000, int(limit or 200)))
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, title, promo_code, starts_at, ends_at, force_active, active, last_state, note, created_by_tg_id, created_at, updated_at
                from public.promo_campaigns
                order by updated_at desc, id desc
                limit %s
                """,
                (lim,),
            )
            rows = cur.fetchall() or []
        conn.commit()
        out = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0] or 0),
                    "title": str(r[1] or ""),
                    "promo_code": str(r[2] or ""),
                    "starts_at": r[3].isoformat() if r[3] else None,
                    "ends_at": r[4].isoformat() if r[4] else None,
                    "force_active": bool(r[5]),
                    "active": bool(r[6]),
                    "last_state": str(r[7] or "unknown"),
                    "note": str(r[8] or ""),
                    "created_by_tg_id": str(r[9] or ""),
                    "created_at": r[10].isoformat() if r[10] else None,
                    "updated_at": r[11].isoformat() if r[11] else None,
                }
            )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_campaign_delete(actor_tg_id: str, cid: int):
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("delete from public.promo_campaigns where id=%s returning id", (int(cid),))
            row = cur.fetchone()
        conn.commit()
        if not row:
            return {"ok": False, "error": "campaign_not_found"}
        admin_audit_log(actor_tg_id, "promo_campaign_delete", "", {"id": int(cid)})
        return {"ok": True, "id": int(cid)}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def promo_campaign_tick():
    conn = _db_conn()
    try:
        changed = 0
        with conn.cursor() as cur:
            cur.execute("select now()")
            now_ts = (cur.fetchone() or [None])[0]
            cur.execute(
                """
                select id, promo_code, starts_at, ends_at, force_active, active, last_state
                from public.promo_campaigns
                where active=true
                """
            )
            rows = cur.fetchall() or []
            for r in rows:
                cid, promo_code, starts_at, ends_at, force_active, active, last_state = r
                should = bool(force_active)
                if not should:
                    ok_start = (starts_at is None) or (now_ts is not None and now_ts >= starts_at)
                    ok_end = (ends_at is None) or (now_ts is not None and now_ts <= ends_at)
                    should = bool(ok_start and ok_end)
                next_state = "on" if should else "off"
                if str(last_state or "") != next_state:
                    cur.execute(
                        "update public.promo_codes set active=%s, updated_at=now() where code=%s",
                        (bool(should), str(promo_code)),
                    )
                    cur.execute(
                        "update public.promo_campaigns set last_state=%s, updated_at=now() where id=%s",
                        (next_state, int(cid)),
                    )
                    changed += 1
        conn.commit()
        return {"ok": True, "changed": int(changed)}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)

def _is_admin_tg(tg_id: str) -> bool:
    return str(tg_id or "").strip() in ADMIN_TG_IDS


def _is_super_admin_tg(tg_id: str) -> bool:
    return bool(SUPER_ADMIN_TG_ID) and str(tg_id or "").strip() == str(SUPER_ADMIN_TG_ID)


def _issue_admin_danger_token(actor_tg_id: str, action: str, ttl_sec: int = 90):
    token = secrets.token_urlsafe(18)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    ADMIN_DANGER_TOKENS[str(token)] = {
        "actor_tg_id": str(actor_tg_id or ""),
        "action": str(action or ""),
        "expires_at": now_ts + max(30, min(300, int(ttl_sec or 90))),
    }
    return {"token": token, "expires_at": int(ADMIN_DANGER_TOKENS[str(token)]["expires_at"])}


def _consume_admin_danger_token(actor_tg_id: str, action: str, token: str) -> bool:
    t = str(token or "").strip()
    if not t:
        return False
    row = ADMIN_DANGER_TOKENS.get(t)
    if not row:
        return False
    try:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        if int(row.get("expires_at", 0) or 0) < now_ts:
            ADMIN_DANGER_TOKENS.pop(t, None)
            return False
        if str(row.get("actor_tg_id", "")) != str(actor_tg_id or ""):
            return False
        if str(row.get("action", "")) != str(action or ""):
            return False
        ADMIN_DANGER_TOKENS.pop(t, None)
        return True
    except Exception:
        return False


def admin_audit_log(actor_tg_id: str, action: str, target_tg_id: str | None = None, details: dict | None = None):
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.admin_audit_log(actor_tg_id, target_tg_id, action, details)
                values (%s, %s, %s, %s::jsonb)
                """,
                (
                    str(actor_tg_id or ""),
                    str(target_tg_id or "") or None,
                    str(action or "")[:64],
                    json.dumps(details or {}, ensure_ascii=False),
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        _db_put(conn)


def maybe_auto_player_snapshot(tg_id: str, state_version: int):
    """
    Опционально пишет снимок в admin_player_snapshots каждые N версий state (точечное восстановление).
    Включение: AUTO_PLAYER_SNAPSHOT_EVERY_SV=25 (0 = выкл). actor_tg_id=0 — служебная метка.
    После этого вызывается снимок по времени (AUTO_PLAYER_SNAPSHOT_MINUTES), если включён.
    """
    try:
        step = int(os.environ.get("AUTO_PLAYER_SNAPSHOT_EVERY_SV", "0") or 0)
        sv = int(state_version or 0)
        if step > 0 and sv >= step and (sv % step) == 0:
            player = fetch_player_admin_full(tg_id)
            if player:
                conn = _db_conn()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            insert into public.admin_player_snapshots(actor_tg_id, target_tg_id, state_version, snapshot, note)
                            values (%s, %s, %s, %s::jsonb, %s)
                            """,
                            (
                                "0",
                                str(tg_id),
                                int(player.get("state_version") or 0),
                                json.dumps(player, ensure_ascii=False),
                                "AUTO_CHECKPOINT",
                            ),
                        )
                    conn.commit()
                    player_progress_log("auto_snapshot", tg_id, state_version=sv)
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                finally:
                    _db_put(conn)
    except Exception:
        pass
    try:
        maybe_auto_player_snapshot_by_minutes(tg_id)
    except Exception:
        pass


def _redis_incr_player_event_counter(tg_id: str) -> int:
    try:
        key = f"rt:player:evt_n:{str(tg_id)}"
        raw = _redis_call_resp("INCR", key)
        if not raw:
            return 0
        s = str(raw).strip()
        if s.startswith(":"):
            return int(s[1:].split("\r\n")[0])
        return int(s.split("\r\n")[0])
    except Exception:
        return 0


def _insert_auto_player_snapshot_row(tg_id: str, note: str):
    player = fetch_player_admin_full(tg_id)
    if not player:
        return False
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.admin_player_snapshots(actor_tg_id, target_tg_id, state_version, snapshot, note)
                values (%s, %s, %s, %s::jsonb, %s)
                """,
                (
                    "0",
                    str(tg_id),
                    int(player.get("state_version") or 0),
                    json.dumps(player, ensure_ascii=False),
                    str(note or "")[:220],
                ),
            )
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _db_put(conn)


def maybe_auto_player_snapshot_every_n_events(tg_id: str, state_version_after: int = 0):
    """
    Снимок после каждых N записей в журнал (Redis INCR). Если Redis недоступен — fallback: каждые N state_version.
    Вкл: AUTO_PLAYER_SNAPSHOT_EVERY_N_EVENTS=50 (0 = выкл).
    """
    try:
        n = int(os.environ.get("AUTO_PLAYER_SNAPSHOT_EVERY_N_EVENTS", "0") or 0)
        if n <= 0 or not str(tg_id or "").strip():
            return
        cnt = _redis_incr_player_event_counter(tg_id)
        redis_ok = cnt > 0
        if redis_ok and (cnt % n) == 0:
            if _insert_auto_player_snapshot_row(tg_id, f"AUTO_CHECKPOINT_E{n}_c{cnt}"):
                player_progress_log("auto_snapshot_events", tg_id, event_count=cnt, every_n=n, via="redis")
            return
        if not redis_ok:
            sv = int(state_version_after or 0)
            if sv > 0 and (sv % n) == 0:
                if _insert_auto_player_snapshot_row(tg_id, f"AUTO_CHECKPOINT_E{n}_sv{sv}_redis_fallback"):
                    player_progress_log(
                        "auto_snapshot_events_fallback_sv",
                        tg_id,
                        state_version=sv,
                        every_n=n,
                        via="state_version_fallback",
                    )
    except Exception:
        pass


def maybe_auto_player_snapshot_by_minutes(tg_id: str):
    """
    Периодический снимок по времени (независимо от state_version).
    AUTO_PLAYER_SNAPSHOT_MINUTES=120 (0 = выкл). note=AUTO_CHECKPOINT_T
    """
    try:
        mins = int(os.environ.get("AUTO_PLAYER_SNAPSHOT_MINUTES", "0") or 0)
        if mins <= 0:
            return
        conn = _db_conn()
        last = None
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select max(created_at) from public.admin_player_snapshots
                    where target_tg_id=%s and note like 'AUTO%%'
                    """,
                    (str(tg_id),),
                )
                row = cur.fetchone()
                last = row[0] if row else None
            conn.commit()
        finally:
            _db_put(conn)
        now = datetime.now(timezone.utc)
        if last is not None:
            if getattr(last, "tzinfo", None) is None:
                last = last.replace(tzinfo=timezone.utc)
            if (now - last) < timedelta(minutes=mins):
                return
        player = fetch_player_admin_full(tg_id)
        if not player:
            return
        conn = _db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into public.admin_player_snapshots(actor_tg_id, target_tg_id, state_version, snapshot, note)
                    values (%s, %s, %s, %s::jsonb, %s)
                    """,
                    (
                        "0",
                        str(tg_id),
                        int(player.get("state_version") or 0),
                        json.dumps(player, ensure_ascii=False),
                        "AUTO_CHECKPOINT_T",
                    ),
                )
            conn.commit()
            player_progress_log("auto_snapshot_time", tg_id, state_version=int(player.get("state_version") or 0))
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            _db_put(conn)
    except Exception:
        pass


def admin_snapshot_create(actor_tg_id: str, target_tg_id: str, note: str = ""):
    player = fetch_player_admin_full(target_tg_id)
    if not player:
        return None
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.admin_player_snapshots(actor_tg_id, target_tg_id, state_version, snapshot, note)
                values (%s, %s, %s, %s::jsonb, %s)
                returning id, created_at
                """,
                (
                    str(actor_tg_id),
                    str(target_tg_id),
                    int(player.get("state_version", 0) or 0),
                    json.dumps(player, ensure_ascii=False),
                    str(note or "")[:300],
                ),
            )
            r = cur.fetchone()
        conn.commit()
        return {
            "id": int(r[0]),
            "created_at": r[1].isoformat() if r and r[1] else now_iso(),
            "target_tg_id": str(target_tg_id),
            "state_version": int(player.get("state_version", 0) or 0),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def admin_snapshot_list(target_tg_id: str, limit: int = 30):
    conn = _db_conn()
    try:
        lim = max(1, min(200, int(limit or 30)))
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, actor_tg_id, target_tg_id, state_version, created_at, note
                from public.admin_player_snapshots
                where target_tg_id = %s
                order by id desc
                limit %s
                """,
                (str(target_tg_id), lim),
            )
            rows = cur.fetchall() or []
        conn.commit()
        out = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "actor_tg_id": str(r[1] or ""),
                    "target_tg_id": str(r[2] or ""),
                    "state_version": int(r[3] or 0),
                    "created_at": r[4].isoformat() if r[4] else None,
                    "note": str(r[5] or ""),
                }
            )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def admin_snapshot_restore(actor_tg_id: str, target_tg_id: str, snapshot_id: int):
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select snapshot
                from public.admin_player_snapshots
                where id = %s and target_tg_id = %s
                limit 1
                """,
                (int(snapshot_id), str(target_tg_id)),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return {"ok": False, "error": "snapshot_not_found"}
            snap = row[0] if isinstance(row[0], dict) else {}
            st = snap.get("state", {}) if isinstance(snap.get("state", {}), dict) else {}
            cur.execute("select coalesce(state_version,0) from public.players where tg_id=%s limit 1", (str(target_tg_id),))
            cur_sv_row = cur.fetchone()
            cur_sv = int(cur_sv_row[0] or 0) if cur_sv_row else 0
            next_sv = cur_sv + 1
            cur.execute(
                """
                update public.players set
                  name=%s,
                  photo_url=%s,
                  level=%s,
                  xp=%s,
                  gold=%s,
                  silver=%s,
                  tooth=%s,
                  district_fear_total=%s,
                  arena_power=%s,
                  arena_wins=%s,
                  arena_losses=%s,
                  stats_sum=%s,
                  boss_wins=%s,
                  state=%s::jsonb,
                  state_version=%s,
                  updated_at=now()
                where tg_id=%s
                """,
                (
                    str(snap.get("name", "Player"))[:18],
                    str(snap.get("photo_url", "") or ""),
                    int(snap.get("level", 1) or 1),
                    int(snap.get("xp", 0) or 0),
                    int(snap.get("gold", 0) or 0),
                    int(snap.get("silver", 0) or 0),
                    int(snap.get("tooth", 0) or 0),
                    int(snap.get("district_fear_total", 0) or 0),
                    int(snap.get("arena_power", 0) or 0),
                    int(snap.get("arena_wins", 0) or 0),
                    int(snap.get("arena_losses", 0) or 0),
                    int(snap.get("stats_sum", 0) or 0),
                    int(snap.get("boss_wins", 0) or 0),
                    json.dumps(st, ensure_ascii=False),
                    int(next_sv),
                    str(target_tg_id),
                ),
            )
        conn.commit()
        admin_audit_log(actor_tg_id, "snapshot_restore", target_tg_id, {"snapshot_id": int(snapshot_id), "next_state_version": int(next_sv)})
        return {"ok": True, "target_tg_id": str(target_tg_id), "state_version": int(next_sv), "snapshot_id": int(snapshot_id)}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def admin_force_logout_player(actor_tg_id: str, target_tg_id: str):
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.players
                set active_session_id=null,
                    active_session_updated_at=null,
                    active_device_id=null,
                    updated_at=now()
                where tg_id=%s
                returning tg_id
                """,
                (str(target_tg_id),),
            )
            row = cur.fetchone()
        conn.commit()
        if not row:
            return {"ok": False, "error": "player_not_found"}
        admin_audit_log(actor_tg_id, "force_logout", target_tg_id, {})
        return {"ok": True, "target_tg_id": str(target_tg_id)}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def admin_audit_list(limit: int = 100, target_tg_id: str | None = None):
    conn = _db_conn()
    try:
        lim = max(1, min(500, int(limit or 100)))
        with conn.cursor() as cur:
            if target_tg_id:
                cur.execute(
                    """
                    select id, actor_tg_id, target_tg_id, action, details, created_at
                    from public.admin_audit_log
                    where target_tg_id = %s
                    order by id desc
                    limit %s
                    """,
                    (str(target_tg_id), lim),
                )
            else:
                cur.execute(
                    """
                    select id, actor_tg_id, target_tg_id, action, details, created_at
                    from public.admin_audit_log
                    order by id desc
                    limit %s
                    """,
                    (lim,),
                )
            rows = cur.fetchall() or []
        conn.commit()
        out = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "actor_tg_id": str(r[1] or ""),
                    "target_tg_id": str(r[2] or ""),
                    "action": str(r[3] or ""),
                    "details": r[4] if isinstance(r[4], dict) else {},
                    "created_at": r[5].isoformat() if r[5] else None,
                }
            )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def upsert_player(payload: dict, conn=None):
    sql = """
    insert into public.players
      (tg_id,name,photo_url,level,xp,gold,silver,tooth,district_fear_total,arena_power,arena_wins,arena_losses,stats_sum,boss_wins,state,updated_at,state_version)
    values
      (%(tg_id)s,%(name)s,%(photo_url)s,%(level)s,%(xp)s,%(gold)s,%(silver)s,%(tooth)s,%(district_fear_total)s,%(arena_power)s,%(arena_wins)s,%(arena_losses)s,%(stats_sum)s,%(boss_wins)s,%(state)s::jsonb,%(updated_at)s::timestamptz,%(next_state_version)s)
    on conflict (tg_id) do update set
      name=excluded.name,
      photo_url=case
        when length(trim(coalesce(excluded.photo_url, ''))) > 0 then excluded.photo_url
        else public.players.photo_url
      end,
      level=excluded.level,
      xp=excluded.xp,
      gold=excluded.gold,
      silver=excluded.silver,
      tooth=excluded.tooth,
      district_fear_total=excluded.district_fear_total,
      arena_power=excluded.arena_power,
      arena_wins=excluded.arena_wins,
      arena_losses=excluded.arena_losses,
      stats_sum=excluded.stats_sum,
      boss_wins=excluded.boss_wins,
      state=excluded.state,
      state_version=public.players.state_version + 1,
      updated_at=now()
    where (
      (%(expected_state_version)s is not null and public.players.state_version = %(expected_state_version)s)
      or
      (
        %(expected_state_version)s is null
        and public.players.state_version = 0
        and (
          public.players.updated_at is null
          or excluded.updated_at >= (public.players.updated_at - interval '2 minutes')
        )
      )
    )
    returning tg_id, state_version
    """
    own_conn = conn is None
    if own_conn:
        conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, payload)
            row = cur.fetchone()
        if own_conn:
            conn.commit()
        if not row:
            return False, None
        return True, int(row[1] or 0)
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            _db_put(conn)


def fetch_player_state_version(tg_id: str):
    sql = "select state_version from public.players where tg_id=%s limit 1"
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (str(tg_id),))
            row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return int(row[0] or 0)
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def record_rt_sample(kind: str, value_ms: int):
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.realtime_perf_samples(metric_kind, value_ms, created_at)
                values (%s, %s, now())
                """,
                (str(kind)[:32], max(0, int(value_ms or 0))),
            )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        _db_put(conn)


def record_boss_metric(kind: str, value: int):
    allowed = {
        "boss_update_apply_live",
        "boss_update_apply_replay",
        "duplicate_event_dropped",
        "seq_gap_detected",
        "replay_empty",
        "replay_catchup_depth",
        "replay_requested_total",
        "replay_served_events_total",
    }
    kk = str(kind or "").strip()
    if kk not in allowed:
        return
    vv = max(0, int(value or 0))
    if vv <= 0:
        return
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.realtime_boss_metrics(metric_kind, metric_value, created_at)
                values (%s, %s, now())
                """,
                (kk, vv),
            )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        _db_put(conn)


def get_write_op_result(conn, tg_id: str, request_id: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            select state_version
            from public.player_write_ops
            where tg_id=%s and request_id=%s
            limit 1
            """,
            (str(tg_id), str(request_id)),
        )
        row = cur.fetchone()
        if not row:
            return None
        return int(row[0] or 0)


def save_write_op_result(conn, tg_id: str, request_id: str, state_version: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.player_write_ops (tg_id, request_id, state_version, created_at)
            values (%s,%s,%s,now())
            on conflict (tg_id, request_id) do nothing
            """,
            (str(tg_id), str(request_id), int(state_version or 0)),
        )


def track_write_event(conn, tg_id: str, request_id: str, event_type: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.player_write_events (tg_id, request_id, event_type, created_at)
            values (%s,%s,%s,now())
            """,
            (str(tg_id), str(request_id), str(event_type)),
        )


def list_district_leaders(limit: int = 100):
    sql = """
    select district_key, tg_id, name, fear, photo_url, updated_at
    from public.district_leaders
    order by fear desc nulls last, updated_at desc
    limit %s
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (max(1, min(int(limit or 100), 500)),))
            rows = cur.fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "district_key": r[0],
                        "tg_id": r[1],
                        "name": r[2],
                        "fear": r[3],
                        "photo_url": r[4],
                        "updated_at": r[5].isoformat() if r[5] else None,
                    }
                )
            return out


def list_district_daily_leaders(limit: int = 100):
    sql = """
    select day, district_key, tg_id, name, fear, photo_url, updated_at
    from public.district_daily_leaders
    order by day desc, fear desc nulls last, updated_at desc
    limit %s
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (max(1, min(int(limit or 100), 500)),))
            rows = cur.fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "day": r[0],
                        "district_key": r[1],
                        "tg_id": r[2],
                        "name": r[3],
                        "fear": r[4],
                        "photo_url": r[5],
                        "updated_at": r[6].isoformat() if r[6] else None,
                    }
                )
            return out


def list_district_daily_leaders_for_day(day: str, limit: int = 200):
    """Страшилы за конкретные сутки (UTC day key), а не случайные 100 строк из всех дней."""
    d = str(day or "").strip()
    if not d:
        return []
    sql = """
    select day, district_key, tg_id, name, fear, photo_url, updated_at
    from public.district_daily_leaders
    where day = %s
    order by fear desc nulls last, updated_at desc
    limit %s
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (d, max(1, min(int(limit or 200), 500))))
            rows = cur.fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "day": r[0],
                        "district_key": r[1],
                        "tg_id": r[2],
                        "name": r[3],
                        "fear": r[4],
                        "photo_url": r[5],
                        "updated_at": r[6].isoformat() if r[6] else None,
                    }
                )
            return out


def upsert_district_daily_leader(
    day: str,
    district_key: str,
    tg_id: str,
    name: str,
    fear: int,
    photo_url: str,
):
    """Одна строка на (day, district_key): лидер дня по страху в районе."""
    d = str(day or "").strip()
    dk = str(district_key or "").strip()
    tid = str(tg_id or "").strip()
    if not d or not dk or not tid:
        return False
    try:
        fv = max(0, int(fear or 0))
    except Exception:
        fv = 0
    nm = str(name or "").strip()[:80] or "—"
    ph = str(photo_url or "").strip()[:500] or None
    sql = """
    insert into public.district_daily_leaders (day, district_key, tg_id, name, fear, photo_url, updated_at)
    values (%s, %s, %s, %s, %s, %s, now())
    on conflict (day, district_key) do update set
      tg_id = excluded.tg_id,
      name = excluded.name,
      fear = excluded.fear,
      photo_url = excluded.photo_url,
      updated_at = now()
    where excluded.fear > public.district_daily_leaders.fear
       or (excluded.fear = public.district_daily_leaders.fear and excluded.tg_id = public.district_daily_leaders.tg_id)
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (d, dk, tid, nm, fv, ph))
        conn.commit()
    return True


def list_boss_last_winners(limit: int = 50):
    sql = """
    select boss_id, tg_id, name, photo_url, updated_at
    from public.boss_last_winners
    order by boss_id asc
    limit %s
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (max(1, min(int(limit or 50), 200)),))
            rows = cur.fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "boss_id": r[0],
                        "tg_id": r[1],
                        "name": r[2],
                        "photo_url": r[3],
                        "updated_at": r[4].isoformat() if r[4] else None,
                    }
                )
            return out


def list_top_players(kind: str = "LVL", limit: int = 100):
    k = str(kind or "LVL").upper()
    if k == "WEALTH":
        order_field = "stats_sum"
    elif k == "BOSS":
        order_field = "boss_wins"
    else:
        order_field = "level"
    sql = f"""
    select name, photo_url, level, stats_sum, boss_wins
    from public.players
    order by {order_field} desc nulls last, updated_at desc
    limit %s
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (max(1, min(int(limit or 100), 200)),))
            rows = cur.fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "name": r[0],
                        "photo_url": r[1],
                        "level": r[2],
                        "stats_sum": r[3],
                        "boss_wins": r[4],
                        "vip": False,
                    }
                )
            return out


def list_boss_fights(owner_tg_id: str):
    sql = """
    select boss_id, hp, max_hp, expires_at, reward_claimed, fight_started_at, updated_at
    from public.player_boss_fights
    where owner_tg_id = %s
    order by boss_id asc
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (str(owner_tg_id),))
            rows = cur.fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "boss_id": r[0],
                        "hp": r[1],
                        "max_hp": r[2],
                        "expires_at": r[3].isoformat() if r[3] else None,
                        "reward_claimed": bool(r[4]),
                        "fight_started_at": r[5].isoformat() if r[5] else None,
                        "updated_at": r[6].isoformat() if r[6] else None,
                    }
                )
            return out


def start_boss_fight(owner_tg_id: str, boss_id: int, max_hp: int, expires_at: str | None):
    prev = get_boss_fight(str(owner_tg_id), int(boss_id))
    # Клиент часто не шлёт expires_at → coalesce(now()+8h). Нельзя каждый start сбрасывать fight_started_at и
    # тянуть greatest(expires): иначе таймер «плывёт» и новая попытка не получает своё 8ч окно.
    sql = """
    insert into public.player_boss_fights
      (owner_tg_id, boss_id, hp, max_hp, expires_at, reward_claimed, fight_started_at, updated_at)
    values
      (%s, %s, %s, %s, coalesce(%s::timestamptz, now() + interval '8 hours'), false, now(), now())
    on conflict (owner_tg_id, boss_id) do update set
      max_hp = greatest(public.player_boss_fights.max_hp, excluded.max_hp),
      hp = case
        when public.player_boss_fights.reward_claimed
          or (
            public.player_boss_fights.hp > 0
            and public.player_boss_fights.expires_at is not null
            and public.player_boss_fights.expires_at < now()
          )
          then excluded.max_hp
        else public.player_boss_fights.hp
      end,
      expires_at = case
        when public.player_boss_fights.reward_claimed
          or (
            public.player_boss_fights.hp > 0
            and public.player_boss_fights.expires_at is not null
            and public.player_boss_fights.expires_at < now()
          )
          then coalesce(excluded.expires_at, now() + interval '8 hours')
        else public.player_boss_fights.expires_at
      end,
      fight_started_at = case
        when public.player_boss_fights.reward_claimed
          or (
            public.player_boss_fights.hp > 0
            and public.player_boss_fights.expires_at is not null
            and public.player_boss_fights.expires_at < now()
          )
          then now()
        else public.player_boss_fights.fight_started_at
      end,
      reward_claimed = case
        when public.player_boss_fights.reward_claimed
          or (
            public.player_boss_fights.hp > 0
            and public.player_boss_fights.expires_at is not null
            and public.player_boss_fights.expires_at < now()
          )
          then false
        else public.player_boss_fights.reward_claimed
      end,
      updated_at = now()
    returning owner_tg_id, boss_id, hp, max_hp, expires_at, reward_claimed, fight_started_at, updated_at
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (str(owner_tg_id), int(boss_id), int(max_hp), int(max_hp), expires_at))
            r = cur.fetchone()
        conn.commit()
    need_clear = False
    if prev is None:
        need_clear = True
    else:
        try:
            if bool(prev.get("reward_claimed")):
                need_clear = True
            elif int(prev.get("hp") or 0) <= 0:
                need_clear = True
            elif int(prev.get("hp") or 0) > 0:
                exp_s = prev.get("expires_at")
                if exp_s:
                    raw = str(exp_s).replace("Z", "+00:00")
                    exp_dt = datetime.fromisoformat(raw)
                    if exp_dt.tzinfo is None:
                        exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                    if exp_dt < datetime.now(timezone.utc):
                        need_clear = True
        except Exception:
            pass
    if need_clear:
        try:
            redis_boss_rt_clear(str(owner_tg_id), int(boss_id))
        except Exception:
            pass
    return {
        "owner_tg_id": r[0],
        "boss_id": r[1],
        "hp": r[2],
        "max_hp": r[3],
        "expires_at": r[4].isoformat() if r[4] else None,
        "reward_claimed": bool(r[5]),
        "fight_started_at": r[6].isoformat() if r[6] else None,
        "updated_at": r[7].isoformat() if r[7] else None,
    }


def pull_boss_help_events(to_tg_id: str, limit: int = 500):
    claim = """
    with picked as (
      select id
      from public.boss_help_events
      where to_tg_id = %s and consumed = false
      order by id asc
      for update skip locked
      limit %s
    )
    update public.boss_help_events e
    set consumed = true, consumed_at = now()
    from picked
    where e.id = picked.id
    returning e.id, e.to_tg_id, e.from_tg_id, e.from_name, e.boss_id, e.dmg, e.clan_id, e.created_at
    """
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(claim, (str(to_tg_id), max(1, min(int(limit or 500), 1000))))
            rows = cur.fetchall()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)
    events = []
    for r in rows:
        events.append(
            {
                "id": r[0],
                "to_tg_id": r[1],
                "from_tg_id": r[2],
                "from_name": r[3],
                "boss_id": r[4],
                "dmg": r[5],
                "clan_id": r[6],
                "created_at": r[7].isoformat() if r[7] else None,
            }
        )
    return events


def list_clans(limit: int = 200):
    sql = """
    select id, name, owner_tg_id, data, updated_at
    from public.clans
    order by updated_at desc
    limit %s
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (max(1, min(int(limit or 200), 500)),))
            rows = cur.fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "name": r[1],
                "owner_tg_id": r[2],
                "data": r[3] if isinstance(r[3], dict) else {},
                "updated_at": r[4].isoformat() if r[4] else None,
            }
        )
    return out


def upsert_clan(clan_id: str, name: str, owner_tg_id: str, data_obj: dict):
    sql = """
    insert into public.clans (id, name, owner_tg_id, data, updated_at)
    values (%s, %s, %s, %s::jsonb, now())
    on conflict (id) do update set
      name = excluded.name,
      data = excluded.data,
      updated_at = now()
    returning id, name, owner_tg_id, data, updated_at
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (str(clan_id), str(name), str(owner_tg_id), json.dumps(data_obj, ensure_ascii=False)))
            r = cur.fetchone()
        conn.commit()
    return {
        "id": r[0],
        "name": r[1],
        "owner_tg_id": r[2],
        "data": r[3] if isinstance(r[3], dict) else {},
        "updated_at": r[4].isoformat() if r[4] else None,
    }


def clan_apply_append(clan_id: str, applicant_name: str):
    """Добавить имя в data.apps клана (как edge clan_apply)."""
    cid = str(clan_id or "").strip()
    app_nm = str(applicant_name or "").strip()[:18]
    if not cid or not app_nm:
        return "bad_args"
    if not re.match(r"^CLN\d{1,20}$", cid):
        return "bad_clan_id"
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select data from public.clans where id = %s limit 1", (cid,))
            r = cur.fetchone()
            if not r:
                return "not_found"
            raw = r[0]
            data = raw if isinstance(raw, dict) else {}
            apps = data.get("apps") if isinstance(data.get("apps"), list) else []
            if app_nm not in apps:
                apps = list(apps) + [app_nm]
            nd = dict(data)
            nd["apps"] = apps
            cur.execute(
                "update public.clans set data = %s::jsonb, updated_at = now() where id = %s",
                (json.dumps(nd, ensure_ascii=False), cid),
            )
        conn.commit()
    return None


def clan_accept_member(clan_id: str, owner_tg_id: str, applicant_name: str):
    cid = str(clan_id or "").strip()
    app_nm = str(applicant_name or "").strip()[:18]
    owner = str(owner_tg_id or "").strip()
    if not cid or not app_nm or not owner:
        return "bad_args"
    if not re.match(r"^CLN\d{1,20}$", cid):
        return "bad_clan_id"
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select owner_tg_id, data from public.clans where id = %s limit 1", (cid,))
            r = cur.fetchone()
            if not r:
                return "not_found"
            if str(r[0] or "").strip() != owner:
                return "forbidden"
            data = r[1] if isinstance(r[1], dict) else {}
            apps = list(data.get("apps") or []) if isinstance(data.get("apps"), list) else []
            members = list(data.get("members") or []) if isinstance(data.get("members"), list) else []
            apps = [x for x in apps if str(x).strip() != app_nm]
            if app_nm not in members:
                members.append(app_nm)
            nd = dict(data)
            nd["apps"] = apps
            nd["members"] = members
            cur.execute(
                "update public.clans set data = %s::jsonb, updated_at = now() where id = %s",
                (json.dumps(nd, ensure_ascii=False), cid),
            )
        conn.commit()
    return None


def clan_reject_applicant(clan_id: str, owner_tg_id: str, applicant_name: str):
    cid = str(clan_id or "").strip()
    app_nm = str(applicant_name or "").strip()[:18]
    owner = str(owner_tg_id or "").strip()
    if not cid or not app_nm or not owner:
        return "bad_args"
    if not re.match(r"^CLN\d{1,20}$", cid):
        return "bad_clan_id"
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select owner_tg_id, data from public.clans where id = %s limit 1", (cid,))
            r = cur.fetchone()
            if not r:
                return "not_found"
            if str(r[0] or "").strip() != owner:
                return "forbidden"
            data = r[1] if isinstance(r[1], dict) else {}
            apps = list(data.get("apps") or []) if isinstance(data.get("apps"), list) else []
            apps = [x for x in apps if str(x).strip() != app_nm]
            nd = dict(data)
            nd["apps"] = apps
            cur.execute(
                "update public.clans set data = %s::jsonb, updated_at = now() where id = %s",
                (json.dumps(nd, ensure_ascii=False), cid),
            )
        conn.commit()
    return None


def clan_cancel_apply(clan_id: str, applicant_name: str):
    cid = str(clan_id or "").strip()
    app_nm = str(applicant_name or "").strip()[:18]
    if not cid or not app_nm:
        return "bad_args"
    if not re.match(r"^CLN\d{1,20}$", cid):
        return "bad_clan_id"
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select data from public.clans where id = %s limit 1", (cid,))
            r = cur.fetchone()
            if not r:
                return "not_found"
            data = r[0] if isinstance(r[0], dict) else {}
            apps = list(data.get("apps") or []) if isinstance(data.get("apps"), list) else []
            apps = [x for x in apps if str(x).strip() != app_nm]
            nd = dict(data)
            nd["apps"] = apps
            cur.execute(
                "update public.clans set data = %s::jsonb, updated_at = now() where id = %s",
                (json.dumps(nd, ensure_ascii=False), cid),
            )
        conn.commit()
    return None


def _norm_clan_name(n) -> str:
    return str(n or "").strip().upper()


def clan_leave_member(clan_id: str, member_name: str):
    cid = str(clan_id or "").strip()
    mem = str(member_name or "").strip()[:18]
    mn = _norm_clan_name(mem)
    if not cid or not mn:
        return "bad_args", None
    if not re.match(r"^CLN\d{1,20}$", cid):
        return "bad_clan_id", None
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("select data from public.clans where id = %s limit 1", (cid,))
            r = cur.fetchone()
            if not r:
                conn.rollback()
                return "not_found", None
            data = r[0] if isinstance(r[0], dict) else {}
            members = [str(x or "").strip() for x in (data.get("members") or [])] if isinstance(data.get("members"), list) else []
            next_members = [x for x in members if _norm_clan_name(x) != mn]
            leader = _norm_clan_name(data.get("leader") or "")
            deputy = str(data.get("deputy") or "").strip()
            if deputy and _norm_clan_name(deputy) == mn:
                deputy = ""
            if leader and leader == mn and len(next_members) > 0:
                conn.rollback()
                return "leader_must_transfer", None
            nd = dict(data)
            nd["members"] = next_members
            nd["deputy"] = deputy
            if not next_members:
                cur.execute("delete from public.clans where id = %s", (cid,))
            else:
                cur.execute(
                    "update public.clans set data = %s::jsonb, updated_at = now() where id = %s",
                    (json.dumps(nd, ensure_ascii=False), cid),
                )
        conn.commit()
        return None, (not next_members)
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def get_boss_fight(owner_tg_id: str, boss_id: int):
    sql = """
    select owner_tg_id, boss_id, hp, max_hp, expires_at, reward_claimed, fight_started_at, updated_at
    from public.player_boss_fights
    where owner_tg_id = %s and boss_id = %s
    limit 1
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (str(owner_tg_id), int(boss_id)))
            r = cur.fetchone()
    if not r:
        return None
    return {
        "owner_tg_id": r[0],
        "boss_id": r[1],
        "hp": r[2],
        "max_hp": r[3],
        "expires_at": r[4].isoformat() if r[4] else None,
        "reward_claimed": bool(r[5]),
        "fight_started_at": r[6].isoformat() if r[6] else None,
        "updated_at": r[7].isoformat() if r[7] else None,
    }


def hit_boss_fight(owner_tg_id: str, boss_id: int, dmg: int, max_hp: int, expires_at: str | None):
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.player_boss_fights (owner_tg_id, boss_id, hp, max_hp, expires_at, reward_claimed, fight_started_at, updated_at)
                values (%s, %s, %s, %s, %s::timestamptz, false, now(), now())
                on conflict (owner_tg_id, boss_id) do nothing
                """,
                (str(owner_tg_id), int(boss_id), int(max_hp), int(max_hp), expires_at),
            )
            cur.execute(
                """
                update public.player_boss_fights
                set
                  hp = greatest(hp - %s, 0),
                  max_hp = greatest(max_hp, %s),
                  expires_at = case
                    when %s::timestamptz is null then expires_at
                    when expires_at is null then %s::timestamptz
                    else greatest(expires_at, %s::timestamptz)
                  end,
                  updated_at = now()
                where owner_tg_id = %s and boss_id = %s and hp > 0
                returning owner_tg_id, boss_id, hp, max_hp, expires_at, reward_claimed, fight_started_at, updated_at
                """,
                (max(0, int(dmg or 0)), max(1, int(max_hp or 1)), expires_at, expires_at, expires_at, str(owner_tg_id), int(boss_id)),
            )
            r = cur.fetchone()
        conn.commit()
    if not r:
        prev = get_boss_fight(str(owner_tg_id), int(boss_id))
        if prev:
            return prev
        return {
            "owner_tg_id": str(owner_tg_id),
            "boss_id": int(boss_id),
            "hp": 0,
            "max_hp": max(1, int(max_hp or 1)),
            "expires_at": None,
            "reward_claimed": False,
            "fight_started_at": None,
            "updated_at": None,
        }
    return {
        "owner_tg_id": r[0],
        "boss_id": r[1],
        "hp": r[2],
        "max_hp": r[3],
        "expires_at": r[4].isoformat() if r[4] else None,
        "reward_claimed": bool(r[5]),
        "fight_started_at": r[6].isoformat() if r[6] else None,
        "updated_at": r[7].isoformat() if r[7] else None,
    }


def _xp_need_for_level(level: int) -> int:
    """Кривая XP как на клиенте (xpNeed)."""
    lvl = max(1, int(level or 1))
    base = 1000
    target_lvl = 201
    target_need = 20000000
    k = math.pow((target_need / base), (1 / max(1, (target_lvl - 1))))
    return max(1, round(base * math.pow(k, (lvl - 1))))


def _apply_xp_award_to_state(st: dict, add_xp: int) -> None:
    add = max(0, int(add_xp or 0))
    if add <= 0:
        return
    st["totalXp"] = max(0, int(st.get("totalXp") or 0) + add)
    st["xp"] = max(0, int(st.get("xp") or 0) + add)
    while True:
        lv = max(1, int(st.get("level") or 1))
        need = _xp_need_for_level(lv)
        if int(st["xp"]) < need:
            break
        st["xp"] = int(st["xp"]) - need
        st["level"] = lv + 1


def _boss_reward_dict(boss_id: int) -> dict:
    bid = max(1, int(boss_id or 1))
    return {
        "xp": max(5, bid * 5),
        "gold": max(2, bid * 2),
        "tooth": 1,
    }


def _apply_boss_reward_to_state(st: dict, boss_id: int, reward: dict) -> None:
    cap = int(os.environ.get("PLAYER_CURRENCY_CAP", "999999999999") or 999999999999)
    rg = max(0, int(reward.get("gold") or 0))
    rt = max(0, int(reward.get("tooth") or 0))
    rx = max(0, int(reward.get("xp") or 0))
    st["gold"] = min(cap, max(0, int(st.get("gold") or 0)) + rg)
    st["tooth"] = min(cap, max(0, int(st.get("tooth") or 0)) + rt)
    st["silver"] = max(0, int(st.get("silver") or 0))
    _apply_xp_award_to_state(st, rx)
    if not isinstance(st.get("bosses"), dict):
        st["bosses"] = {}
    bs = st["bosses"]
    bs["wins"] = int(bs.get("wins") or 0) + 1
    wpb = bs.get("winsPerBoss")
    if not isinstance(wpb, dict):
        wpb = {}
    bs["winsPerBoss"] = wpb
    k = str(int(boss_id))
    wpb[k] = int(wpb.get(k) or 0) + 1
    nxt = int(boss_id) + 1
    if nxt > 1:
        if not isinstance(bs.get("keys"), dict):
            bs["keys"] = {}
        keys = bs["keys"]
        rk = f"ring{nxt}"
        keys[rk] = int(keys.get(rk) or 0) + 1
        pk = f"pass{nxt}"
        if pk in keys:
            try:
                del keys[pk]
            except Exception:
                pass


def boss_fight_commit_rewards_sync(
    tg_id: str,
    boss_id: int,
    user: dict,
    body: dict,
    client_ip: str | None,
) -> dict:
    """
    Атомарно: отметить награду в player_boss_fights и применить награду к players.state (+ denorms).
    Клиент не должен сам начислять валюту/xp за победу — только подставить ответ `player`.
    """
    bid = int(boss_id)
    if bid <= 0:
        return {"ok": False, "error": "bad_boss_id", "http_status": 400}
    try:
        rl_ok, rl_reason = check_player_save_rate_limits(tg_id, client_ip)
        if not rl_ok:
            return {"ok": False, "error": rl_reason or "rate_limited", "http_status": 429, "hint": "slow_down"}
    except Exception:
        pass
    reward = _boss_reward_dict(bid)
    rid = str(body.get("request_id") or "").strip()[:64]

    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select state, state_version, name, photo_url, level, xp, gold, silver, tooth,
                       district_fear_total, arena_power, arena_wins, arena_losses, stats_sum, boss_wins,
                       active_session_id
                from public.players
                where tg_id = %s
                for update
                """,
                (str(tg_id),),
            )
            prow = cur.fetchone()
            if not prow:
                conn.commit()
                return {"ok": False, "error": "player_not_found", "http_status": 404}

            raw_st, cur_sv = prow[0], int(prow[1] or 0)
            st = _parse_state_field(raw_st)
            st_before = copy.deepcopy(st)
            fake_row = {
                "tg_id": tg_id,
                "state": st_before,
                "level": int(prow[4] or 1),
                "stats_sum": int(prow[13] or 0),
                "boss_wins": int(prow[14] or 0),
                "arena_power": int(prow[10] or 0),
                "active_session_id": prow[15],
            }
            sess_ok, sess_err, sess_st = evaluate_save_session_gate(fake_row, body, False)
            if not sess_ok:
                conn.commit()
                return {"ok": False, "error": sess_err, "http_status": int(sess_st or 403)}

            ev_raw = body.get("expected_state_version")
            ev = None
            if ev_raw is not None:
                try:
                    ev = int(ev_raw)
                except (TypeError, ValueError):
                    ev = None
            if cur_sv >= 1 and ev is None:
                conn.commit()
                return {
                    "ok": False,
                    "error": "expected_state_version_required",
                    "current_state_version": cur_sv,
                    "http_status": 400,
                }
            if ev is not None and int(ev) != cur_sv:
                conn.commit()
                return {
                    "ok": False,
                    "error": "state_regress_blocked",
                    "current_state_version": cur_sv,
                    "http_status": 409,
                }

            if rid:
                cur.execute(
                    """
                    select state_version from public.player_write_ops
                    where tg_id=%s and request_id=%s
                    limit 1
                    """,
                    (str(tg_id), rid),
                )
                dup = cur.fetchone()
                if dup:
                    conn.commit()
                    pl = fetch_player(tg_id)
                    return {
                        "ok": True,
                        "can_claim": False,
                        "duplicate": True,
                        "reward_claimed": True,
                        "reward": reward,
                        "player": _normalize_player_state_field(pl) if pl else None,
                        "state_version": int(dup[0] or 0),
                    }

            cur.execute(
                """
                select hp, reward_claimed
                from public.player_boss_fights
                where owner_tg_id = %s and boss_id = %s
                for update
                """,
                (str(tg_id), int(bid)),
            )
            fr = cur.fetchone()
            if not fr:
                conn.commit()
                return {"ok": True, "can_claim": False, "hp": None, "reward_claimed": False, "hint": "no_fight_row"}
            hp_v, rcl = int(fr[0] or 0), bool(fr[1])
            if hp_v > 0:
                conn.commit()
                return {"ok": True, "can_claim": False, "hp": hp_v, "reward_claimed": rcl}
            if rcl:
                conn.commit()
                pl = fetch_player(tg_id)
                return {
                    "ok": True,
                    "can_claim": False,
                    "hp": 0,
                    "reward_claimed": True,
                    "already_claimed": True,
                    "player": _normalize_player_state_field(pl) if pl else None,
                    "state_version": cur_sv,
                }

            cur.execute(
                """
                update public.player_boss_fights
                set reward_claimed = true, updated_at = now()
                where owner_tg_id = %s and boss_id = %s and reward_claimed = false and hp <= 0
                returning hp
                """,
                (str(tg_id), int(bid)),
            )
            up = cur.fetchone()
            if not up:
                conn.rollback()
                return {"ok": False, "error": "claim_race", "http_status": 409}

            _apply_boss_reward_to_state(st, bid, reward)
            ok_i, err_i = validate_player_state_integrity(st)
            if not ok_i:
                conn.rollback()
                try:
                    player_progress_log("boss_commit_integrity_fail", tg_id, reason=err_i, boss_id=bid)
                except Exception:
                    pass
                return {"ok": False, "error": "state_integrity_failed", "reason": err_i, "http_status": 400}
            dg, dg_r = evaluate_patch_merge_downgrade(st_before, st)
            if dg:
                conn.rollback()
                return {"ok": False, "error": dg_r or "patch_downgrade_blocked", "http_status": 409}
            sharp, sr = evaluate_sharp_degradation_block(fake_row, st)
            if sharp:
                conn.rollback()
                return {"ok": False, "error": sr or "sharp_degradation_blocked", "http_status": 409}

            next_sv = cur_sv + 1
            payload = {
                "tg_id": str(tg_id),
                "name": str(prow[2] or "Player")[:18],
                "photo_url": str(prow[3] or ""),
                "level": int(prow[4] or 1),
                "xp": int(prow[5] or 0),
                "gold": int(prow[6] or 0),
                "silver": int(prow[7] or 0),
                "tooth": int(prow[8] or 0),
                "district_fear_total": int(prow[9] or 0),
                "arena_power": int(prow[10] or 0),
                "arena_wins": int(prow[11] or 0),
                "arena_losses": int(prow[12] or 0),
                "stats_sum": int(prow[13] or 0),
                "boss_wins": int(prow[14] or 0),
                "state": json.dumps(st, ensure_ascii=False),
                "updated_at": now_iso(),
                "expected_state_version": cur_sv,
                "next_state_version": next_sv,
            }
            _payload_refresh_denorms_from_state(payload, st)

            cur.execute(
                """
                update public.players set
                  name=%s,
                  level=%s,
                  xp=%s,
                  gold=%s,
                  silver=%s,
                  tooth=%s,
                  district_fear_total=%s,
                  arena_power=%s,
                  arena_wins=%s,
                  arena_losses=%s,
                  stats_sum=%s,
                  boss_wins=%s,
                  state=%s::jsonb,
                  state_version=%s,
                  updated_at=now()
                where tg_id=%s and state_version=%s
                returning state_version
                """,
                (
                    str(payload["name"])[:18],
                    int(payload["level"]),
                    int(payload["xp"]),
                    int(payload["gold"]),
                    int(payload["silver"]),
                    int(payload["tooth"]),
                    int(payload["district_fear_total"]),
                    int(payload["arena_power"]),
                    int(payload["arena_wins"]),
                    int(payload["arena_losses"]),
                    int(payload["stats_sum"]),
                    int(payload["boss_wins"]),
                    payload["state"],
                    next_sv,
                    str(tg_id),
                    cur_sv,
                ),
            )
            r2 = cur.fetchone()
            if not r2:
                conn.rollback()
                return {
                    "ok": False,
                    "error": "state_regress_blocked",
                    "current_state_version": cur_sv,
                    "http_status": 409,
                }
            out_sv = int(r2[0] or next_sv)
            if rid:
                save_write_op_result(conn, tg_id, rid, out_sv)
            conn.commit()

        try:
            append_player_state_event(
                tg_id,
                "boss_fight_commit_rewards",
                "boss_fight_claim",
                rid or str(uuid.uuid4())[:64],
                out_sv,
                f"boss_commit boss_id={bid}",
                _state_summary_for_event(st),
                {"boss_id": int(bid), "reward": reward},
            )
        except Exception:
            pass
        try:
            maybe_auto_player_snapshot(tg_id, out_sv)
        except Exception:
            pass
        try:
            redis_touch_player_rt(tg_id, out_sv)
            redis_publish_player_save(tg_id, out_sv)
        except Exception:
            pass
        pl = fetch_player(tg_id)
        return {
            "ok": True,
            "can_claim": True,
            "hp": 0,
            "reward_claimed": True,
            "reward": reward,
            "player": _normalize_player_state_field(pl) if pl else None,
            "state_version": out_sv,
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _db_put(conn)


def claim_boss_fight(owner_tg_id: str, boss_id: int):
    """Тесты/legacy: подставляет актуальный expected_state_version из БД."""
    csv = fetch_player_state_version(str(owner_tg_id))
    body: dict = {}
    if csv is not None:
        body["expected_state_version"] = int(csv)
    return boss_fight_commit_rewards_sync(str(owner_tg_id), int(boss_id), {}, body, None)


def find_arena_opponent(owner_tg_id: str, min_sum: int, max_sum: int, min_power: int, max_power: int):
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select tg_id, name, photo_url, stats_sum, arena_power, level
                from public.players
                where tg_id <> %s
                  and stats_sum between %s and %s
                  and (
                    %s <= 0 or %s <= 0
                    or arena_power between %s and %s
                  )
                order by random()
                limit 1
                """,
                (
                    str(owner_tg_id),
                    max(0, int(min_sum or 0)),
                    max(max(0, int(min_sum or 0)), int(max_sum or 0)),
                    int(min_power or 0),
                    int(max_power or 0),
                    max(0, int(min_power or 0)),
                    max(max(0, int(min_power or 0)), int(max_power or 0)),
                ),
            )
            r = cur.fetchone()
    if not r:
        return None
    return {
        "tg_id": r[0],
        "name": r[1],
        "photo_url": r[2],
        "stats_sum": r[3],
        "arena_power": r[4],
        "level": r[5],
    }


def get_players_by_names(names: list[str]):
    clean = []
    seen = set()
    for x in names or []:
        n = str(x or "").strip()
        if not n:
            continue
        k = n.lower()
        if k in seen:
            continue
        seen.add(k)
        clean.append(n)
    if not clean:
        return []
    sql = """
    select distinct on (lower(name)) name, photo_url, level, stats_sum
    from public.players
    where lower(name) = any(%s)
    order by lower(name), updated_at desc
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, ([x.lower() for x in clean],))
            rows = cur.fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "name": r[0],
                "photo_url": r[1] or "",
                "level": int(r[2] or 1),
                "stats_sum": int(r[3] or 0),
            }
        )
    return out


def _friend_names_from_player_state(st):
    """Имена братков из json state.friends (только для доставки помощи по боссу)."""
    names = []
    if not isinstance(st, dict):
        return names
    friends = st.get("friends")
    if not isinstance(friends, list):
        return names
    for fr in friends:
        n = ""
        if isinstance(fr, dict):
            n = str(fr.get("name") or "").strip().lower()
        elif fr is not None:
            n = str(fr).strip().lower()
        if n and n not in names:
            names.append(n)
    return names


def boss_help_send(owner_tg_id: str, boss_id: int, dmg: int, clan_id: str, from_name: str):
    recipients = []
    with _db_conn() as conn:
        with conn.cursor() as cur:
            # Try clan-targeted recipients first (by clan member names -> players tg_id).
            if clan_id:
                cur.execute("select data from public.clans where id = %s limit 1", (str(clan_id),))
                row = cur.fetchone()
                data = row[0] if row and isinstance(row[0], dict) else {}
                members = data.get("members", []) if isinstance(data, dict) else []
                member_names = [str(x or "").strip().lower() for x in members if str(x or "").strip()]
                if member_names:
                    cur.execute(
                        """
                        select tg_id
                        from public.players
                        where lower(name) = any(%s) and tg_id <> %s
                        """,
                        (member_names, str(owner_tg_id)),
                    )
                    recipients = [str(r[0]) for r in cur.fetchall() if r and r[0]]

            # Братки из state отправителя (без клана / клан не сматчился).
            if not recipients:
                cur.execute("select state from public.players where tg_id = %s limit 1", (str(owner_tg_id),))
                row = cur.fetchone()
                st = row[0] if row and isinstance(row[0], dict) else {}
                friend_names = _friend_names_from_player_state(st)
                if friend_names:
                    cur.execute(
                        """
                        select tg_id
                        from public.players
                        where lower(name) = any(%s) and tg_id <> %s
                        """,
                        (friend_names, str(owner_tg_id)),
                    )
                    recipients = [str(r[0]) for r in cur.fetchall() if r and r[0]]

            # Никакой рассылки «всем подряд» — иначе чужой урон в бою у случайных игроков.

            rec_unique = []
            seen = set()
            for tg in recipients:
                if tg in seen:
                    continue
                seen.add(tg)
                rec_unique.append(tg)
            recipients = rec_unique

            inserted = 0
            for to_tg in recipients:
                cur.execute(
                    """
                    insert into public.boss_help_events (to_tg_id, from_tg_id, from_name, boss_id, dmg, clan_id)
                    values (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(to_tg),
                        str(owner_tg_id),
                        str(from_name or "BRAT")[:18],
                        int(boss_id),
                        max(1, int(dmg or 0)),
                        str(clan_id or "") or None,
                    ),
                )
                inserted += 1
        conn.commit()
    return {"ok": True, "inserted": inserted, "debug": {"recipients": len(recipients), "recipient_ids": recipients}}


async def options_ok(_request):
    return web.Response(text="ok", headers=cors_headers())


async def health(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    return web.json_response({"ok": True, "service": "game-backend"}, headers=cors_headers())


async def get_player_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    tg_id = str(user["id"])
    target_tg = str(body.get("target_tg_id", "")).strip()
    target_name = str(body.get("target_name", "")).strip()
    if target_tg and tg_id in ADMIN_TG_IDS:
        tg_id = target_tg
    if (not target_tg) and target_name and tg_id in ADMIN_TG_IDS:
        player = await asyncio.to_thread(fetch_player_by_name, target_name)
    else:
        player = await asyncio.to_thread(fetch_player_admin_full if tg_id in ADMIN_TG_IDS else fetch_player, tg_id)
    if player is not None:
        try:
            raw_st = player.get("state")
            if raw_st is None:
                player["state"] = {}
            elif isinstance(raw_st, str):
                try:
                    player["state"] = json.loads(raw_st) if raw_st.strip() else {}
                except Exception:
                    player["state"] = {}
            elif not isinstance(raw_st, dict):
                player["state"] = {}
            sk = len(player["state"]) if isinstance(player.get("state"), dict) else 0
            player_progress_log(
                "load_ok",
                tg_id,
                state_keys=sk,
                state_version=int(player.get("state_version") or 0),
                level=int(player.get("level") or 1),
                boss_wins=int(player.get("boss_wins") or 0),
            )
        except Exception:
            pass
    else:
        player_progress_log("load_no_row", tg_id)
    return web.json_response({"ok": True, "player": player}, headers=cors_headers())


async def sync_pull_after_conflict_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    tg_id = str(user["id"])
    player = await asyncio.to_thread(fetch_player, tg_id)
    try:
        player_progress_log("sync_pull", tg_id, state_version=int((player or {}).get("state_version") or 0) if player else None)
    except Exception:
        pass
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            tg_id,
            "sync_pull_after_conflict",
            "sync_pull",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"state_version": int((player or {}).get("state_version") or 0) if player else 0},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True, "player": player}, headers=cors_headers())


def _build_upsert_payload(body: dict, user: dict, tg_id: str):
    state_obj = body.get("state", {}) if isinstance(body.get("state", {}), dict) else {}
    s = state_obj
    district_fear_total = 0
    if isinstance(s.get("districtFear"), dict):
        district_fear_total = sum(int(v or 0) for v in s.get("districtFear", {}).values())
    payload = {
        "tg_id": tg_id,
        "name": str(body.get("name", s.get("playerName", user.get("first_name", "Player"))) or "Player")[:18],
        "photo_url": str(body.get("photo_url", "") or ""),
        "level": int(body.get("level", s.get("level", 1)) or 1),
        "xp": int(body.get("xp", s.get("totalXp", s.get("xp", 0))) or 0),
        "gold": int(body.get("gold", s.get("gold", 0)) or 0),
        "silver": int(body.get("silver", s.get("silver", 0)) or 0),
        "tooth": int(body.get("tooth", s.get("tooth", 0)) or 0),
        "district_fear_total": int(body.get("district_fear_total", district_fear_total) or 0),
        "arena_power": int(body.get("arena_power", (s.get("arena", {}) or {}).get("power", 0)) or 0),
        "arena_wins": int(body.get("arena_wins", (s.get("arena", {}) or {}).get("wins", 0)) or 0),
        "arena_losses": int(body.get("arena_losses", (s.get("arena", {}) or {}).get("losses", 0)) or 0),
        "stats_sum": int(body.get("stats_sum", 0) or 0),
        "boss_wins": int(body.get("boss_wins", (s.get("bosses", {}) or {}).get("wins", 0)) or 0),
        "state": json.dumps(state_obj, ensure_ascii=False),
        "updated_at": now_iso(),
        "expected_state_version": None,
        "next_state_version": 1,
    }
    expected_sv_raw = body.get("expected_state_version")
    if expected_sv_raw is not None:
        try:
            ev = int(expected_sv_raw)
            if ev >= 0:
                payload["expected_state_version"] = ev
                payload["next_state_version"] = ev + 1
        except (TypeError, ValueError):
            pass
    return payload


def client_full_state_writes_forbidden() -> bool:
    """
    Запрет полного state от клиента (upsert / realtime_save_fast / state в boss_hit_and_save).
    Снятие только для миграций: ALLOW_CLIENT_FULL_STATE_SAVE=1
    """
    return os.environ.get("ALLOW_CLIENT_FULL_STATE_SAVE", "").strip().lower() not in {"1", "true", "yes", "on"}


def build_server_seeded_player_state(user: dict, seed_name: str | None) -> dict:
    """Шаблон нового профиля на сервере — без доверия к JSON state с клиента."""
    nn = str(seed_name or user.get("first_name") or "Player").strip()[:18] or "Player"
    ci = {str(i): 0 for i in range(1, 11)}
    return {
        "playerName": nn,
        "level": 1,
        "xp": 0,
        "totalXp": 0,
        "gold": 0,
        "silver": 0,
        "tooth": 0,
        "rings": 0,
        "inventory": [],
        "friends": [],
        "currencyLog": [],
        "bosses": {"wins": 0},
        "arena": {"power": 0, "wins": 0, "losses": 0},
        "gym": {},
        "districtFear": {},
        "districtBizLvls": {},
        "armorOwned": {},
        "petsOwned": {},
        "consumables": {"med": 0, "nade": 0},
        "consumablesItems": ci,
        "consumablesBuffs": {
            "health": 0,
            "strength": 0,
            "agility": 0,
            "initiative": 0,
            "endurance": 0,
            "might": 0,
            "charisma": 0,
        },
        "settings": {"sound": True, "haptics": True, "reduced": False},
    }


def _deep_merge_dict(base: dict, patch: dict):
    out = dict(base or {})
    for k, v in (patch or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge_dict(out.get(k) or {}, v)
        else:
            out[k] = v
    return out


def _parse_state_field(raw):
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            v = json.loads(raw)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def _core_progress_floor(s: dict) -> int:
    """Прогресс без золота/серебра — чтобы трата валюты не считалась «вайпом»."""
    try:
        if not isinstance(s, dict):
            return 0
        lvl = max(0, int(s.get("level") or 0))
        xp = max(int(s.get("totalXp") or 0), int(s.get("xp") or 0))
        bosses = s.get("bosses") if isinstance(s.get("bosses"), dict) else {}
        bw = max(0, int(bosses.get("wins") or 0))
        arena = s.get("arena") if isinstance(s.get("arena"), dict) else {}
        aw = max(0, int(arena.get("wins") or 0))
        df_sum = 0
        df = s.get("districtFear")
        if isinstance(df, dict):
            df_sum = sum(int(v or 0) for v in df.values())
        return lvl + (xp // 200) + (bw * 5) + (aw * 3) + (df_sum // 80)
    except Exception:
        return 0


def _progress_score_for_guard(s: dict) -> int:
    try:
        if not isinstance(s, dict):
            return 0
        lvl = max(0, int(s.get("level") or 0))
        xp = max(int(s.get("totalXp") or 0), int(s.get("xp") or 0))
        g = max(0, int(s.get("gold") or 0))
        t = max(0, int(s.get("tooth") or 0))
        bosses = s.get("bosses") if isinstance(s.get("bosses"), dict) else {}
        bw = max(0, int(bosses.get("wins") or 0))
        arena = s.get("arena") if isinstance(s.get("arena"), dict) else {}
        aw = max(0, int(arena.get("wins") or 0))
        rings = max(0, int(s.get("rings") or 0))
        df_sum = 0
        df = s.get("districtFear")
        if isinstance(df, dict):
            df_sum = sum(int(v or 0) for v in df.values())
        return (
            lvl
            + (xp // 100)
            + (g // 100)
            + (t // 10)
            + (rings // 5)
            + (bw * 5)
            + (aw * 3)
            + (df_sum // 50)
        )
    except Exception:
        return 0


def _has_meaningful_progress_guard(s: dict) -> bool:
    try:
        if not isinstance(s, dict) or len(s) == 0:
            return False
        if (int(s.get("level") or 0)) > 1:
            return True
        if (int(s.get("xp") or 0)) > 0 or (int(s.get("totalXp") or 0)) > 0:
            return True
        if (int(s.get("gold") or 0)) > 0:
            return True
        if (int(s.get("silver") or 0)) > 0:
            return True
        if (int(s.get("tooth") or 0)) > 0:
            return True
        if (int(s.get("rings") or 0)) > 0:
            return True
        gym = s.get("gym")
        if isinstance(gym, dict) and any((int(gym.get(k) or 0) > 10) for k in gym):
            return True
        ao = s.get("armorOwned")
        if isinstance(ao, dict) and len(ao) > 0:
            return True
        if s.get("selectedArmorKey"):
            return True
        if s.get("mainCharacterImg") and str(s.get("mainCharacterImg") or "").strip():
            return True
        po = s.get("petsOwned")
        if isinstance(po, dict) and len(po) > 0:
            return True
        if (int(s.get("activePetId") or 0)) > 0:
            return True
        dbl = s.get("districtBizLvls")
        if isinstance(dbl, dict) and any((int(dbl.get(k) or 0) > 0) for k in dbl):
            return True
        if (int(s.get("districtBizFirstPurchaseTs") or 0)) > 0:
            return True
        inv = s.get("inventory")
        if isinstance(inv, list) and any(str(x or "").strip() for x in inv):
            return True
        bosses = s.get("bosses")
        if isinstance(bosses, dict):
            if (int(bosses.get("wins") or 0)) > 0:
                return True
            keys = bosses.get("keys")
            if isinstance(keys, dict) and len(keys) > 0:
                return True
        df = s.get("districtFear")
        if isinstance(df, dict) and any((int(df.get(k) or 0) > 0) for k in df):
            return True
        return False
    except Exception:
        return False


def evaluate_state_wipe_block(existing_player_record: dict | None, incoming_state: dict) -> tuple[bool, str]:
    """
    Не даём клиенту/багу перезаписать богатый state пустым или «дефолтным».
    Новая строка игрока — не блокируем. Админ — отдельные handler'ы (не через этот путь).
    Отключение: DISABLE_STATE_WIPE_GUARD=1 в окружении.
    """
    if os.environ.get("DISABLE_STATE_WIPE_GUARD", "").strip().lower() in {"1", "true", "yes", "on"}:
        return False, ""
    if not existing_player_record:
        return False, ""
    ex = _parse_state_field(existing_player_record.get("state"))
    inc = incoming_state if isinstance(incoming_state, dict) else {}
    ex_score = _progress_score_for_guard(ex)
    inc_score = _progress_score_for_guard(inc)
    ex_mean = _has_meaningful_progress_guard(ex) or ex_score >= 12
    if not ex_mean:
        try:
            if int(existing_player_record.get("level") or 1) > 1:
                ex_mean = True
            elif int(existing_player_record.get("boss_wins") or 0) > 0:
                ex_mean = True
            elif int(existing_player_record.get("stats_sum") or 0) > 8:
                ex_mean = True
            elif int(existing_player_record.get("arena_power") or 0) > 5:
                ex_mean = True
        except Exception:
            pass

    if not ex_mean:
        return False, ""

    if len(inc) == 0:
        return True, "state_wipe_blocked_empty_incoming"

    ex_core = _core_progress_floor(ex)
    inc_core = _core_progress_floor(inc)
    # Сильный аккаунт, а по «ядру» (уровень/xp/победы) пришёл явный откат — не затирать.
    if ex_core > 14 and (inc_core + 10) < ex_core:
        return True, "state_wipe_blocked_core_regress"

    # Почти пустой payload при богатом сохранённом state (часто баг клиента / defaults).
    if ex_score >= 18 and inc_score <= 2 and len(inc) < 12:
        return True, "state_wipe_blocked_weak_incoming"

    return False, ""


def ensure_player_audit_schema():
    """Таблица player_state_events — журнал действий, меняющих state (ленивое создание при старте)."""
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists public.player_state_events (
                  id bigserial primary key,
                  tg_id text not null,
                  created_at timestamptz not null default now(),
                  action_type text not null default 'save',
                  endpoint text,
                  request_id text,
                  state_version_after int,
                  client_reason text,
                  summary jsonb,
                  action_payload jsonb
                );
                create index if not exists idx_player_state_events_tg_created
                  on public.player_state_events (tg_id, created_at desc);
                """
            )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _db_put(conn)


def validate_player_state_integrity(s: dict) -> tuple[bool, str]:
    """
    Структурная проверка state перед записью (null/неверный тип известных полей).
    Отключение: DISABLE_STATE_INTEGRITY_CHECK=1
    """
    if os.environ.get("DISABLE_STATE_INTEGRITY_CHECK", "").strip().lower() in {"1", "true", "yes", "on"}:
        return True, ""
    if not isinstance(s, dict):
        return False, "state_not_object"
    type_checks = [
        ("inventory", list),
        ("friends", list),
        ("currencyLog", list),
        ("bosses", dict),
        ("arena", dict),
        ("gym", dict),
        ("districtFear", dict),
        ("districtBizLvls", dict),
        ("armorOwned", dict),
        ("petsOwned", dict),
        ("consumables", dict),
        ("consumablesItems", dict),
        ("consumablesBuffs", dict),
    ]
    for key, typ in type_checks:
        if key not in s:
            continue
        v = s.get(key)
        if v is None:
            return False, f"state_null_{key}"
        if not isinstance(v, typ):
            return False, f"state_bad_type_{key}"
    try:
        if "level" in s and s.get("level") is not None:
            lv = int(s.get("level"))
            if lv < 1 or lv > 2_000_000:
                return False, "state_bad_level"
    except Exception:
        return False, "state_bad_level"
    for cur_key in ("gold", "silver", "tooth", "rings", "totalXp", "xp"):
        if cur_key not in s or s.get(cur_key) is None:
            continue
        try:
            if int(s.get(cur_key)) < 0:
                return False, f"state_negative_{cur_key}"
        except Exception:
            return False, f"state_bad_{cur_key}"
    c0 = s.get("consumables")
    if "consumables" in s and c0 is not None:
        if not isinstance(c0, dict):
            return False, "state_bad_type_consumables"
        for ck in ("med", "nade"):
            if ck in c0 and c0.get(ck) is not None:
                try:
                    if int(c0.get(ck)) < 0:
                        return False, f"state_negative_consumables_{ck}"
                except Exception:
                    return False, f"state_bad_consumables_{ck}"
    # Экипировка / кольца боссов: если ключ есть — не null и корректный тип
    if "selectedArmorKey" in s and s.get("selectedArmorKey") is None:
        return False, "state_null_selectedArmorKey"
    bs = s.get("bosses")
    if isinstance(bs, dict) and "keys" in bs and bs.get("keys") is not None and not isinstance(bs.get("keys"), dict):
        return False, "state_bad_type_bosses_keys"
    return True, ""


def _inventory_non_empty_slots(inv) -> int:
    if not isinstance(inv, list):
        return 0
    n = 0
    for x in inv:
        try:
            if x is None:
                continue
            s = str(x).strip()
            if s:
                n += 1
        except Exception:
            pass
    return n


def _consumables_bundle_score(st: dict) -> int:
    try:
        sc = 0
        c = st.get("consumables")
        if isinstance(c, dict):
            sc += int(c.get("med") or 0) + int(c.get("nade") or 0)
        ci = st.get("consumablesItems")
        if isinstance(ci, dict):
            for _k, v in ci.items():
                sc += int(v or 0)
        return max(0, sc)
    except Exception:
        return 0


def _armor_owned_positive_count(st: dict) -> int:
    try:
        ao = st.get("armorOwned")
        if not isinstance(ao, dict):
            return 0
        return sum(1 for _k, v in ao.items() if int(v or 0) > 0)
    except Exception:
        return 0


def evaluate_patch_merge_downgrade(before: dict, after: dict) -> tuple[bool, str]:
    """
    Блокируем резкое ухудшение после merge patch (инвентарь, расходники, броня).
    True, reason = запретить запись.
    Отключение: DISABLE_PATCH_DOWNGRADE_GUARD=1
    """
    if os.environ.get("DISABLE_PATCH_DOWNGRADE_GUARD", "").strip().lower() in {"1", "true", "yes", "on"}:
        return False, ""
    if not isinstance(before, dict) or not isinstance(after, dict):
        return True, "patch_downgrade_bad_types"
    bi = _inventory_non_empty_slots(before.get("inventory"))
    ai = _inventory_non_empty_slots(after.get("inventory"))
    if bi >= 8 and ai + 3 < int(bi * 0.35):
        return True, "patch_inventory_crash"
    if bi >= 5 and ai <= max(0, bi // 4) and (bi - ai) >= 4:
        return True, "patch_inventory_wipe"
    bc = _consumables_bundle_score(before)
    ac = _consumables_bundle_score(after)
    if bc >= 20 and ac < bc * 0.2 and (bc - ac) >= 15:
        return True, "patch_consumables_crash"
    ba = _armor_owned_positive_count(before)
    aa = _armor_owned_positive_count(after)
    if ba >= 2 and aa == 0:
        return True, "patch_armor_wipe"
    return False, ""


def server_row_looks_established_row(row: dict | None) -> bool:
    """Зеркало client serverRowLooksEstablished — значимая строка players без полного JSON state."""
    try:
        if not row or str(row.get("tg_id") or "").strip() == "":
            return False
        sv = int(row.get("state_version") or 0)
        if sv >= 1:
            return True
        if int(row.get("level") or 0) > 1:
            return True
        if int(row.get("boss_wins") or 0) > 0:
            return True
        if int(row.get("stats_sum") or 0) > 8:
            return True
        if int(row.get("arena_power") or 0) > 5:
            return True
        return False
    except Exception:
        return False


def player_row_allows_bootstrap_write(cur_row: dict | None, incoming_state: dict) -> tuple[bool, str]:
    """
    Полная запись state (bootstrap) только для пустого stub: пустой JSON state и строка ещё не «установлена».
    Иначе — только patch / отдельные RPC, чтобы клиент не перетирал облако старым blob.
    """
    if not cur_row:
        return True, ""
    ex = _parse_state_field(cur_row.get("state"))
    if len(ex) > 0:
        return False, "bootstrap_blocked_nonempty_state"
    if server_row_looks_established_row(cur_row):
        return False, "bootstrap_blocked_established_row"
    return True, ""


def evaluate_sharp_degradation_block(existing_player_record: dict | None, final_state: dict) -> tuple[bool, str]:
    """
    Блокируем резкое падение «оценки прогресса» без явного вайпа (дополнение к state_wipe_guard).
    Отключение: DISABLE_SHARP_DEGRADATION_GUARD=1
    """
    if os.environ.get("DISABLE_SHARP_DEGRADATION_GUARD", "").strip().lower() in {"1", "true", "yes", "on"}:
        return False, ""
    if not existing_player_record:
        return False, ""
    ex = _enrich_state_from_db_row(_parse_state_field(existing_player_record.get("state")), existing_player_record)
    inc = final_state if isinstance(final_state, dict) else {}
    if len(inc) == 0:
        return False, ""
    if not _has_meaningful_progress_guard(ex):
        return False, ""
    ex_sc = _progress_score_for_guard(ex)
    inc_sc = _progress_score_for_guard(inc)
    ex_core = _core_progress_floor(ex)
    inc_core = _core_progress_floor(inc)
    try:
        min_ex = int(os.environ.get("SHARP_GUARD_MIN_EXISTING_SCORE", "55") or 55)
    except Exception:
        min_ex = 55
    if ex_sc < min_ex:
        return False, ""
    try:
        ratio = float(os.environ.get("SHARP_GUARD_SCORE_RATIO", "0.42") or 0.42)
    except Exception:
        ratio = 0.42
    if inc_sc < ex_sc * ratio and (ex_sc - inc_sc) > 25:
        return True, "sharp_degradation_score"
    try:
        core_drop = int(os.environ.get("SHARP_GUARD_MAX_CORE_DROP", "18") or 18)
    except Exception:
        core_drop = 18
    if ex_core > 20 and inc_core + core_drop < ex_core:
        return True, "sharp_degradation_core"
    return False, ""


def _state_summary_for_event(s: dict) -> dict:
    try:
        if not isinstance(s, dict):
            return {}
        bosses = s.get("bosses") if isinstance(s.get("bosses"), dict) else {}
        arena = s.get("arena") if isinstance(s.get("arena"), dict) else {}
        inv = s.get("inventory") if isinstance(s.get("inventory"), list) else []
        return {
            "level": int(s.get("level") or 0),
            "gold": int(s.get("gold") or 0),
            "totalXp": max(int(s.get("totalXp") or 0), int(s.get("xp") or 0)),
            "boss_wins": int(bosses.get("wins") or 0),
            "arena_wins": int(arena.get("wins") or 0),
            "inv_len": len(inv),
        }
    except Exception:
        return {}


def append_player_state_event(
    tg_id: str,
    action_type: str,
    endpoint: str,
    request_id: str | None,
    state_version_after: int,
    client_reason: str,
    summary: dict,
    action_payload: dict | None,
):
    if os.environ.get("DISABLE_PLAYER_STATE_EVENTS", "").strip().lower() in {"1", "true", "yes", "on"}:
        return
    try:
        ap = json.dumps(action_payload or {}, ensure_ascii=False)[:4096]
        sm = json.dumps(summary or {}, ensure_ascii=False)[:4096]
    except Exception:
        ap = "{}"
        sm = "{}"
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.player_state_events
                (tg_id, action_type, endpoint, request_id, state_version_after, client_reason, summary, action_payload)
                values (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                """,
                (
                    str(tg_id),
                    str(action_type or "save")[:64],
                    str(endpoint or "")[:64],
                    str(request_id or "")[:64],
                    int(state_version_after or 0),
                    str(client_reason or "")[:500],
                    sm,
                    ap,
                ),
            )
        conn.commit()
        try:
            maybe_auto_player_snapshot_every_n_events(str(tg_id), int(state_version_after or 0))
        except Exception:
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _db_put(conn)


def append_player_activity_event(
    tg_id: str,
    endpoint: str,
    action_type: str,
    request_id: str | None,
    summary: dict | None,
    action_payload: dict | None,
):
    """Журнал действий игрока (без bump state_version)."""
    if os.environ.get("DISABLE_PLAYER_ACTIVITY_LOG", "").strip().lower() in {"1", "true", "yes", "on"}:
        return
    append_player_state_event(
        str(tg_id),
        str(action_type or "activity")[:64],
        str(endpoint or "")[:64],
        str(request_id or "")[:64] if request_id else str(uuid.uuid4())[:64],
        0,
        "activity_log",
        summary if isinstance(summary, dict) else {},
        action_payload if isinstance(action_payload, dict) else {},
    )


def admin_player_state_events_list(target_tg_id: str, limit: int = 80):
    conn = _db_conn()
    try:
        lim = max(1, min(200, int(limit or 80)))
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, created_at, action_type, endpoint, request_id, state_version_after, client_reason, summary, action_payload
                from public.player_state_events
                where tg_id = %s
                order by id desc
                limit %s
                """,
                (str(target_tg_id), lim),
            )
            rows = cur.fetchall() or []
        conn.commit()
        out = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "created_at": r[1].isoformat() if r[1] else None,
                    "action_type": str(r[2] or ""),
                    "endpoint": str(r[3] or ""),
                    "request_id": str(r[4] or ""),
                    "state_version_after": int(r[5] or 0),
                    "client_reason": str(r[6] or ""),
                    "summary": r[7] if isinstance(r[7], dict) else {},
                    "action_payload": r[8] if isinstance(r[8], dict) else {},
                }
            )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def pre_write_state_validation(cur_row: dict | None, payload: dict, skip_guards: bool):
    """None = ок; иначе (dict для JSON, http_status)."""
    if skip_guards:
        return None
    try:
        final_st = json.loads(str(payload.get("state") or "{}"))
    except Exception:
        final_st = {}
    if len(final_st) > 0:
        ok_i, err_i = validate_player_state_integrity(final_st)
        if not ok_i:
            return ({"ok": False, "error": "state_integrity_failed", "reason": err_i}, 400)
    if cur_row:
        sharp, sr = evaluate_sharp_degradation_block(cur_row, final_st)
        if sharp:
            return (
                {
                    "ok": False,
                    "error": sr or "sharp_degradation_blocked",
                    "hint": "sync_pull_recommended",
                },
                409,
            )
    return None


def admin_snapshot_restore_latest_auto(actor_tg_id: str, target_tg_id: str):
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id from public.admin_player_snapshots
                where target_tg_id=%s and note like 'AUTO%%'
                order by id desc
                limit 1
                """,
                (str(target_tg_id),),
            )
            row = cur.fetchone()
        conn.commit()
        if not row:
            return {"ok": False, "error": "no_auto_snapshot"}
    finally:
        _db_put(conn)
    return admin_snapshot_restore(actor_tg_id, target_tg_id, int(row[0]))


def _enrich_state_from_db_row(st: dict, row: dict | None) -> dict:
    """Если JSON state пустой, подтянуть уровень/победы из колонок players (рассинхрон не затирает прогресс)."""
    out = dict(st if isinstance(st, dict) else {})
    if not row:
        return out
    try:
        lv = int(row.get("level") or 1)
        if lv > 0:
            out["level"] = max(int(out.get("level") or 1), lv)
        bw = int(row.get("boss_wins") or 0)
        if bw > 0:
            if not isinstance(out.get("bosses"), dict):
                out["bosses"] = {}
            out["bosses"]["wins"] = max(int((out.get("bosses") or {}).get("wins") or 0), bw)
    except Exception:
        pass
    return out


def _progression_max_merge_enabled() -> bool:
    return os.environ.get("DISABLE_PROGRESSION_MAX_MERGE", "").strip().lower() not in {"1", "true", "yes", "on"}


def _merge_numeric_dict_max(existing: dict | None, incoming: dict | None) -> dict:
    merged: dict = dict(incoming or {})
    if not isinstance(existing, dict):
        return merged
    for k, v in existing.items():
        try:
            merged[k] = max(int(merged.get(k) or 0), int(v or 0))
        except Exception:
            if k not in merged:
                merged[k] = v
    return merged


def merge_progression_max(existing: dict, incoming: dict) -> dict:
    """
    Второй уровень защиты: база — сохранённый state + патч с клиента; «жёсткий» прогресс не падает ниже БД.
    Валюта (gold/silver/tooth) — из смерженного дерева (клиент), без max — чтобы траты сохранялись.
    """
    if not isinstance(incoming, dict):
        return {}
    if not isinstance(existing, dict) or len(existing) == 0:
        return copy.deepcopy(incoming)
    # Сначала полное дерево из БД, поверх — клиент (не теряем поля при частичном JSON).
    out = _deep_merge_dict(copy.deepcopy(existing), incoming)
    try:
        out["level"] = max(int(existing.get("level") or 1), int(out.get("level") or 1))
    except Exception:
        pass
    try:
        ex_xp = max(int(existing.get("totalXp") or 0), int(existing.get("xp") or 0))
        ox_xp = max(int(out.get("totalXp") or 0), int(out.get("xp") or 0))
        mx = max(ex_xp, ox_xp)
        out["totalXp"] = mx
        out["xp"] = mx
    except Exception:
        pass
    try:
        out["rings"] = max(int(existing.get("rings") or 0), int(out.get("rings") or 0))
    except Exception:
        pass
    try:
        out["activePetId"] = max(int(existing.get("activePetId") or 0), int(out.get("activePetId") or 0))
    except Exception:
        pass
    try:
        out["districtBizFirstPurchaseTs"] = max(
            int(existing.get("districtBizFirstPurchaseTs") or 0),
            int(out.get("districtBizFirstPurchaseTs") or 0),
        )
    except Exception:
        pass

    eb = existing.get("bosses") if isinstance(existing.get("bosses"), dict) else {}
    if not isinstance(out.get("bosses"), dict):
        out["bosses"] = {}
    ib = out["bosses"]
    try:
        ib["wins"] = max(int(eb.get("wins") or 0), int(ib.get("wins") or 0))
    except Exception:
        pass
    try:
        ek = eb.get("keys")
        ik = ib.get("keys")
        if isinstance(ek, dict) or isinstance(ik, dict):
            ib["keys"] = _merge_numeric_dict_max(ek if isinstance(ek, dict) else {}, ik if isinstance(ik, dict) else {})
    except Exception:
        pass
    try:
        ehs = eb.get("hitStocks") if isinstance(eb.get("hitStocks"), dict) else {}
        ihs = ib.get("hitStocks") if isinstance(ib.get("hitStocks"), dict) else {}
        if ehs or ihs:
            ib["hitStocks"] = _merge_numeric_dict_max(ehs, ihs)
    except Exception:
        pass

    try:
        ea = existing.get("arena") if isinstance(existing.get("arena"), dict) else {}
        if not isinstance(out.get("arena"), dict):
            out["arena"] = {}
        ia = out["arena"]
        ia["wins"] = max(int(ea.get("wins") or 0), int(ia.get("wins") or 0))
        ia["losses"] = max(int(ea.get("losses") or 0), int(ia.get("losses") or 0))
        ia["power"] = max(int(ea.get("power") or 0), int(ia.get("power") or 0))
    except Exception:
        pass

    try:
        edf = existing.get("districtFear")
        idf = out.get("districtFear")
        if isinstance(edf, dict) or isinstance(idf, dict):
            out["districtFear"] = _merge_numeric_dict_max(
                edf if isinstance(edf, dict) else {},
                idf if isinstance(idf, dict) else {},
            )
    except Exception:
        pass

    try:
        eg = existing.get("gym")
        ig = out.get("gym")
        if isinstance(eg, dict) or isinstance(ig, dict):
            out["gym"] = _merge_numeric_dict_max(eg if isinstance(eg, dict) else {}, ig if isinstance(ig, dict) else {})
    except Exception:
        pass

    try:
        ed = existing.get("districtBizLvls")
        id_ = out.get("districtBizLvls")
        if isinstance(ed, dict) or isinstance(id_, dict):
            out["districtBizLvls"] = _merge_numeric_dict_max(
                ed if isinstance(ed, dict) else {},
                id_ if isinstance(id_, dict) else {},
            )
    except Exception:
        pass

    try:
        etc = existing.get("districtTaskCounts")
        itc = out.get("districtTaskCounts")
        if isinstance(etc, dict) or isinstance(itc, dict):
            out["districtTaskCounts"] = _merge_numeric_dict_max(
                etc if isinstance(etc, dict) else {},
                itc if isinstance(itc, dict) else {},
            )
    except Exception:
        pass

    for nk in ("armorOwned", "petsOwned"):
        try:
            ed = existing.get(nk)
            id_ = out.get(nk)
            if isinstance(ed, dict) or isinstance(id_, dict):
                merged = dict(id_ or {})
                for k, v in (ed or {}).items():
                    if k not in merged:
                        merged[k] = v
                    else:
                        try:
                            merged[k] = max(int(merged.get(k) or 0), int(v or 0))
                        except Exception:
                            merged[k] = merged[k] if merged[k] is not None else v
                out[nk] = merged
        except Exception:
            pass

    return out


def _payload_refresh_denorms_from_state(payload: dict, s: dict) -> None:
    """Синхронизировать колонки players.* с итоговым JSON state после merge."""
    if not isinstance(payload, dict) or not isinstance(s, dict):
        return
    try:
        payload["level"] = max(1, int(s.get("level", 1) or 1))
    except Exception:
        payload["level"] = 1
    try:
        tx = max(int(s.get("totalXp", 0) or 0), int(s.get("xp", 0) or 0))
        payload["xp"] = tx
    except Exception:
        pass
    try:
        payload["gold"] = max(0, int(s.get("gold", 0) or 0))
        payload["silver"] = max(0, int(s.get("silver", 0) or 0))
        payload["tooth"] = max(0, int(s.get("tooth", 0) or 0))
    except Exception:
        pass
    ar = s.get("arena") if isinstance(s.get("arena"), dict) else {}
    try:
        payload["arena_power"] = max(0, int(ar.get("power", 0) or 0))
        payload["arena_wins"] = max(0, int(ar.get("wins", 0) or 0))
        payload["arena_losses"] = max(0, int(ar.get("losses", 0) or 0))
    except Exception:
        pass
    bs = s.get("bosses") if isinstance(s.get("bosses"), dict) else {}
    try:
        payload["boss_wins"] = max(0, int(bs.get("wins", 0) or 0))
    except Exception:
        pass
    df = s.get("districtFear")
    try:
        if isinstance(df, dict):
            payload["district_fear_total"] = sum(max(0, int(v or 0)) for v in df.values())
    except Exception:
        pass


def _write_player_idempotent(tg_id: str, payload: dict, request_id: str):
    conn = _db_conn()
    try:
        existing_sv = get_write_op_result(conn, tg_id, request_id)
        if existing_sv is not None:
            track_write_event(conn, tg_id, request_id, "duplicate")
            conn.commit()
            return True, existing_sv, True
        written2, state_version2 = upsert_player(payload, conn=conn)
        if written2:
            save_write_op_result(conn, tg_id, request_id, int(state_version2 or 0))
            conn.commit()
            return True, int(state_version2 or 0), False
        existing_sv2 = get_write_op_result(conn, tg_id, request_id)
        if existing_sv2 is not None:
            track_write_event(conn, tg_id, request_id, "duplicate")
            conn.commit()
            return True, existing_sv2, True
        track_write_event(conn, tg_id, request_id, "conflict")
        conn.commit()
        return False, None, False
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


def _write_player_idempotent_with_resync(tg_id: str, payload: dict, request_id: str):
    """
    Повторяет upsert до 3 раз: после конфликта подтягивает актуальный state_version с БД
    и выставляет expected_state_version/next_state_version — лечит гонки вкладок,
    пропущенный ответ save и рассинхрон __expectedStateVersion на клиенте.
    """
    base = str(request_id or "").strip()[:52] or str(uuid.uuid4())
    p = dict(payload)
    for i in range(3):
        rid = (base + ("" if i == 0 else ":rs" + str(i)))[:64]
        written, sv, dup = _write_player_idempotent(tg_id, p, rid)
        if written:
            return True, sv, dup
        cur = fetch_player_state_version(tg_id)
        if cur is None:
            break
        ci = max(0, int(cur))
        p["expected_state_version"] = ci
        p["next_state_version"] = ci + 1
    return False, None, False


def player_patch_set_name(tg_id: str, new_name: str, expected_state_version: int | None, photo_url: str | None) -> dict:
    """Patch-action: только playerName (+ опционально photo_url). Без полного state от клиента."""
    nn = str(new_name or "").strip()[:18] or "Player"
    other = fetch_player_by_name(nn)
    if other and str(other.get("tg_id", "") or "") != str(tg_id):
        return {"ok": False, "error": "name_taken"}
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select state, state_version from public.players where tg_id=%s limit 1",
                (str(tg_id),),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return {"ok": False, "error": "player_not_found"}
            raw_st, cur_sv = row[0], int(row[1] or 0)
            if cur_sv >= 1 and expected_state_version is None:
                conn.commit()
                return {"ok": False, "error": "expected_state_version_required", "current_state_version": cur_sv}
            if expected_state_version is not None and int(expected_state_version) != cur_sv:
                conn.commit()
                return {"ok": False, "error": "state_regress_blocked", "current_state_version": cur_sv}
            st = _parse_state_field(raw_st)
            ok_pre, err_pre = validate_player_state_integrity(st)
            if not ok_pre:
                conn.commit()
                return {"ok": False, "error": "state_integrity_failed", "reason": err_pre}
            st_before = copy.deepcopy(st)
            st["playerName"] = nn
            ok_i, err_i = validate_player_state_integrity(st)
            if not ok_i:
                conn.commit()
                return {"ok": False, "error": "state_integrity_failed", "reason": err_i}
            dg, dg_r = evaluate_patch_merge_downgrade(st_before, st)
            if dg:
                conn.commit()
                return {"ok": False, "error": dg_r or "patch_downgrade_blocked", "hint": "sync_pull_recommended"}
            try:
                full_row = fetch_player(tg_id)
            except Exception:
                full_row = None
            sharp, sr = evaluate_sharp_degradation_block(full_row, st)
            if sharp:
                conn.commit()
                return {"ok": False, "error": sr or "sharp_degradation_blocked", "hint": "sync_pull_recommended"}
            next_sv = cur_sv + 1
            ph_in = str(photo_url or "").strip() if photo_url else ""
            cur.execute(
                """
                update public.players set
                  name=%s,
                  photo_url = case
                    when length(trim(coalesce(%s, ''))) > 0 then %s
                    else photo_url
                  end,
                  state=%s::jsonb,
                  state_version=%s,
                  updated_at=now()
                where tg_id=%s and state_version=%s
                returning state_version
                """,
                (nn, ph_in, ph_in, json.dumps(st, ensure_ascii=False), next_sv, str(tg_id), cur_sv),
            )
            r2 = cur.fetchone()
        if not r2:
            conn.rollback()
            return {"ok": False, "error": "state_regress_blocked", "current_state_version": cur_sv}
        conn.commit()
        player = fetch_player(tg_id)
        return {"ok": True, "player": player, "state_version": int(r2[0] or next_sv)}
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_put(conn)


# --- Серверные игровые действия (лавка / арена / расходники) — без full-state от клиента
GAME_LAVKA_PRICES: dict[int, int] = {
    1: 5,
    2: 5,
    3: 5,
    4: 5,
    5: 5,
    6: 5,
    7: 15,
    8: 5,
    9: 20,
    10: 10,
}
GAME_ARENA_MED_PRICE = 30
GAME_ARENA_NADE_PRICE = 50
GAME_BUFF_MS = 4 * 60 * 60 * 1000
# id -> (kind, stat_or_none, pct)
GAME_CONSUMABLE_META: dict[int, tuple[str, str | None, float]] = {
    1: ("buff", "health", 0.30),
    2: ("buff", "strength", 0.30),
    3: ("buff", "endurance", 0.30),
    4: ("buff", "might", 0.30),
    5: ("buff", "agility", 0.30),
    6: ("buff", "initiative", 0.30),
    7: ("heal", None, 0.80),
    8: ("heal", None, 0.50),
    9: ("dmg", None, 0.30),
    10: ("dmg", None, 0.20),
}


def _pga_currency_cap() -> int:
    return int(os.environ.get("PLAYER_CURRENCY_CAP", "999999999999") or 999999999999)


def _pga_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _pga_clamp(v, lo: int, hi: int) -> int:
    try:
        x = int(v)
    except Exception:
        x = 0
    return max(lo, min(hi, x))


def _pga_ci_get(ci: dict | None, iid: int) -> int:
    if not isinstance(ci, dict):
        return 0
    k = str(int(iid))
    try:
        return max(0, int(ci.get(k, ci.get(str(iid), 0)) or 0))
    except Exception:
        return 0


def _pga_ci_set(ci: dict, iid: int, val: int) -> None:
    k = str(int(iid))
    ci[k] = max(0, int(val))


def _pga_ensure_consumables_items(st: dict) -> dict:
    ci = st.get("consumablesItems")
    if not isinstance(ci, dict):
        ci = {str(i): 0 for i in range(1, 11)}
        st["consumablesItems"] = ci
    return ci


def _pga_gf_battle_active(st: dict) -> bool:
    gf = st.get("groupFight") if isinstance(st.get("groupFight"), dict) else {}
    b = gf.get("battle")
    return isinstance(b, dict) and len(b) > 0


def _pga_battle_weakest_target(targets: list) -> dict | None:
    alive = [t for t in targets if isinstance(t, dict) and int(t.get("hp") or 0) > 0]
    if not alive:
        return None
    try:
        return min(alive, key=lambda t: int(t.get("hp") or 0))
    except Exception:
        return alive[0]


def _pga_mutate_state_for_action(st: dict, action: str, body: dict) -> tuple[str | None, int | None]:
    """
    Мутирует st in-place. Возвращает (error_code, http_status) или (None, None).
    """
    cap = _pga_currency_cap()
    action = str(action or "").strip()
    if action == "lavka_buy":
        try:
            iid = int(body.get("item_id", body.get("id", 0)) or 0)
        except Exception:
            iid = 0
        if iid not in GAME_LAVKA_PRICES:
            return "bad_item_id", 400
        price = int(GAME_LAVKA_PRICES[iid])
        gold = _pga_clamp(st.get("gold", 0), 0, cap)
        if gold < price:
            return "insufficient_gold", 400
        st["gold"] = gold - price
        ci = _pga_ensure_consumables_items(st)
        _pga_ci_set(ci, iid, _pga_ci_get(ci, iid) + 1)
        return None, None

    if action == "arena_buy_med":
        gold = _pga_clamp(st.get("gold", 0), 0, cap)
        if gold < GAME_ARENA_MED_PRICE:
            return "insufficient_gold", 400
        st["gold"] = gold - GAME_ARENA_MED_PRICE
        c = st.get("consumables")
        if not isinstance(c, dict):
            c = {"med": 0, "nade": 0}
            st["consumables"] = c
        c["med"] = int(c.get("med") or 0) + 1
        return None, None

    if action == "arena_buy_nade":
        gold = _pga_clamp(st.get("gold", 0), 0, cap)
        if gold < GAME_ARENA_NADE_PRICE:
            return "insufficient_gold", 400
        st["gold"] = gold - GAME_ARENA_NADE_PRICE
        c = st.get("consumables")
        if not isinstance(c, dict):
            c = {"med": 0, "nade": 0}
            st["consumables"] = c
        c["nade"] = int(c.get("nade") or 0) + 1
        return None, None

    if action == "consumable_buff_use":
        if _pga_gf_battle_active(st):
            return "buff_blocked_during_group_fight", 400
        try:
            iid = int(body.get("item_id", 0) or 0)
        except Exception:
            iid = 0
        meta = GAME_CONSUMABLE_META.get(iid)
        if not meta or meta[0] != "buff":
            return "bad_item_for_buff", 400
        ci = _pga_ensure_consumables_items(st)
        qty = _pga_ci_get(ci, iid)
        if qty <= 0:
            return "no_item", 400
        _, stat, _pct = meta
        assert stat is not None
        bb = st.get("consumablesBuffs")
        if not isinstance(bb, dict):
            bb = {
                "health": 0,
                "strength": 0,
                "agility": 0,
                "initiative": 0,
                "endurance": 0,
                "might": 0,
                "charisma": 0,
            }
            st["consumablesBuffs"] = bb
        bb[str(stat)] = _pga_now_ms() + GAME_BUFF_MS
        _pga_ci_set(ci, iid, qty - 1)
        return None, None

    battle_in = body.get("battle")
    if not isinstance(battle_in, dict):
        battle_in = {}

    if action in {"gb_consumable_item_use", "gb_arena_med_use", "gb_arena_nade_use"}:
        b = copy.deepcopy(battle_in)
        if not isinstance(b, dict) or not b:
            return "bad_battle", 400
        if bool(b.get("acted")):
            return "already_acted", 400
        my_hp = int(b.get("myHp") or 0)
        my_max = int(b.get("myMaxHp") or 0)
        if my_hp <= 0:
            return "dead", 400

        if action == "gb_arena_med_use":
            c = st.get("consumables")
            if not isinstance(c, dict):
                return "no_consumables", 400
            have = int(c.get("med") or 0)
            if have <= 0:
                return "no_med", 400
            c["med"] = have - 1
            heal = max(1, round(my_max * 0.30))
            before = my_hp
            b["myHp"] = _pga_clamp(before + heal, 0, my_max)
            b["acted"] = True
            log = b.get("log")
            if not isinstance(log, list):
                log = []
                b["log"] = log
            log.insert(0, {"kind": "SYS", "text": f"arena_med +{b['myHp'] - before}"})
        elif action == "gb_arena_nade_use":
            c = st.get("consumables")
            if not isinstance(c, dict):
                return "no_consumables", 400
            have = int(c.get("nade") or 0)
            if have <= 0:
                return "no_nade", 400
            targets = b.get("targets")
            if not isinstance(targets, list) or not targets:
                return "no_targets", 400
            my_t = b.get("myTarget")
            t = None
            if my_t is not None and my_t != "":
                t = next((x for x in targets if isinstance(x, dict) and str(x.get("id")) == str(my_t)), None)
            if not t or int(t.get("hp") or 0) <= 0:
                t = _pga_battle_weakest_target(targets)
            if not t or int(t.get("hp") or 0) <= 0:
                return "no_target", 400
            c["nade"] = have - 1
            dmg = 25
            cur_hp = int(t.get("hp") or 0)
            real = min(cur_hp, dmg)
            t["hp"] = max(0, cur_hp - real)
            b["acted"] = True
            log = b.get("log")
            if not isinstance(log, list):
                log = []
                b["log"] = log
            log.insert(0, {"kind": "HIT", "text": f"arena_nade -{real} {t.get('id')}"})
        else:
            try:
                iid = int(body.get("item_id", 0) or 0)
            except Exception:
                iid = 0
            meta = GAME_CONSUMABLE_META.get(iid)
            if not meta or meta[0] not in {"heal", "dmg"}:
                return "bad_item_for_gb", 400
            ci = _pga_ensure_consumables_items(st)
            qty = _pga_ci_get(ci, iid)
            if qty <= 0:
                return "no_item", 400
            typ, _, pct = meta
            log = b.get("log")
            if not isinstance(log, list):
                log = []
                b["log"] = log
            if typ == "heal":
                heal = max(1, round(my_max * float(pct)))
                before = my_hp
                b["myHp"] = _pga_clamp(before + heal, 0, my_max)
                b["acted"] = True
                log.insert(0, {"kind": "SYS", "text": f"item{iid}_heal +{b['myHp'] - before}"})
            else:
                targets = b.get("targets")
                if not isinstance(targets, list) or not targets:
                    return "no_targets", 400
                alive = [t for t in targets if isinstance(t, dict) and int(t.get("hp") or 0) > 0]
                if not alive:
                    return "no_alive_targets", 400
                n = min(3, len(alive))
                pct_f = float(pct)
                hit = []
                pool = list(alive)
                for _ in range(n):
                    if not pool:
                        break
                    t = random.choice(pool)
                    max_hp = int(t.get("maxHp") or 0) or max(1, int(t.get("hp") or 0))
                    dmg = max(1, round(max_hp * pct_f))
                    cur_hp = int(t.get("hp") or 0)
                    t["hp"] = max(0, cur_hp - dmg)
                    hit.append({"id": t.get("id"), "dmg": dmg})
                    pool = [x for x in pool if int(x.get("hp") or 0) > 0]
                b["acted"] = True
                log.insert(0, {"kind": "SYS", "text": "item" + str(iid) + "_dmg " + ",".join(f"{h['id']}-{h['dmg']}" for h in hit)})
            _pga_ci_set(ci, iid, qty - 1)

        gf = st.get("groupFight")
        if not isinstance(gf, dict):
            gf = {}
            st["groupFight"] = gf
        gf["battle"] = b
        return None, None

    return "unknown_action", 400


def player_game_action_sync(tg_id: str, user: dict, body: dict, client_ip: str | None) -> dict:
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
        return {"ok": False, "error": "unknown_action", "http_status": 400}
    try:
        rl_ok, rl_reason = check_player_save_rate_limits(tg_id, client_ip)
        if not rl_ok:
            return {"ok": False, "error": rl_reason or "rate_limited", "http_status": 429, "hint": "slow_down"}
    except Exception:
        pass

    rid = str(body.get("request_id") or "").strip()[:64]
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select state, state_version, name, photo_url, level, xp, gold, silver, tooth,
                       district_fear_total, arena_power, arena_wins, arena_losses, stats_sum, boss_wins,
                       active_session_id
                from public.players
                where tg_id = %s
                for update
                """,
                (str(tg_id),),
            )
            prow = cur.fetchone()
            if not prow:
                conn.commit()
                return {"ok": False, "error": "player_not_found", "http_status": 404}

            raw_st, cur_sv = prow[0], int(prow[1] or 0)
            st = _parse_state_field(raw_st)
            st_before = copy.deepcopy(st)
            fake_row = {
                "tg_id": tg_id,
                "state": st_before,
                "level": int(prow[4] or 1),
                "stats_sum": int(prow[13] or 0),
                "boss_wins": int(prow[14] or 0),
                "arena_power": int(prow[10] or 0),
                "active_session_id": prow[15],
            }
            sess_ok, sess_err, sess_st = evaluate_save_session_gate(fake_row, body, False)
            if not sess_ok:
                conn.commit()
                return {"ok": False, "error": sess_err, "http_status": int(sess_st or 403)}

            ev_raw = body.get("expected_state_version")
            ev = None
            if ev_raw is not None:
                try:
                    ev = int(ev_raw)
                except (TypeError, ValueError):
                    ev = None
            if cur_sv >= 1 and ev is None:
                conn.commit()
                return {
                    "ok": False,
                    "error": "expected_state_version_required",
                    "current_state_version": cur_sv,
                    "http_status": 400,
                }
            if ev is not None and int(ev) != cur_sv:
                conn.commit()
                return {
                    "ok": False,
                    "error": "state_regress_blocked",
                    "current_state_version": cur_sv,
                    "http_status": 409,
                }

            if rid:
                cur.execute(
                    """
                    select state_version from public.player_write_ops
                    where tg_id=%s and request_id=%s
                    limit 1
                    """,
                    (str(tg_id), rid),
                )
                dup = cur.fetchone()
                if dup:
                    conn.commit()
                    pl = fetch_player(tg_id)
                    return {
                        "ok": True,
                        "duplicate": True,
                        "player": _normalize_player_state_field(pl) if pl else None,
                        "state_version": int(dup[0] or 0),
                        "action": client_action,
                    }

            gf_cur_epoch = None
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

            ok_i, er_i = validate_player_state_integrity(st)
            if not ok_i:
                conn.rollback()
                return {"ok": False, "error": "state_integrity_failed", "reason": er_i, "http_status": 400}

            dg, dg_r = evaluate_patch_merge_downgrade(st_before, st)
            if dg:
                conn.rollback()
                return {"ok": False, "error": dg_r or "patch_downgrade_blocked", "http_status": 409}
            try:
                full_row = fetch_player(tg_id)
            except Exception:
                full_row = None
            sharp, sr = evaluate_sharp_degradation_block(full_row, st)
            if sharp:
                conn.rollback()
                return {"ok": False, "error": sr or "sharp_degradation_blocked", "http_status": 409}

            next_sv = cur_sv + 1
            payload = {
                "tg_id": str(tg_id),
                "name": str(prow[2] or "Player")[:18],
                "photo_url": str(prow[3] or ""),
                "level": int(prow[4] or 1),
                "xp": int(prow[5] or 0),
                "gold": int(prow[6] or 0),
                "silver": int(prow[7] or 0),
                "tooth": int(prow[8] or 0),
                "district_fear_total": int(prow[9] or 0),
                "arena_power": int(prow[10] or 0),
                "arena_wins": int(prow[11] or 0),
                "arena_losses": int(prow[12] or 0),
                "stats_sum": int(prow[13] or 0),
                "boss_wins": int(prow[14] or 0),
                "state": json.dumps(st, ensure_ascii=False),
                "updated_at": now_iso(),
                "expected_state_version": cur_sv,
                "next_state_version": next_sv,
            }
            _payload_refresh_denorms_from_state(payload, st)

            cur.execute(
                """
                update public.players set
                  name=%s,
                  level=%s,
                  xp=%s,
                  gold=%s,
                  silver=%s,
                  tooth=%s,
                  district_fear_total=%s,
                  arena_power=%s,
                  arena_wins=%s,
                  arena_losses=%s,
                  stats_sum=%s,
                  boss_wins=%s,
                  state=%s::jsonb,
                  state_version=%s,
                  updated_at=now()
                where tg_id=%s and state_version=%s
                returning state_version
                """,
                (
                    str(payload["name"])[:18],
                    int(payload["level"]),
                    int(payload["xp"]),
                    int(payload["gold"]),
                    int(payload["silver"]),
                    int(payload["tooth"]),
                    int(payload["district_fear_total"]),
                    int(payload["arena_power"]),
                    int(payload["arena_wins"]),
                    int(payload["arena_losses"]),
                    int(payload["stats_sum"]),
                    int(payload["boss_wins"]),
                    payload["state"],
                    next_sv,
                    str(tg_id),
                    cur_sv,
                ),
            )
            r2 = cur.fetchone()
            if not r2:
                conn.rollback()
                return {
                    "ok": False,
                    "error": "state_regress_blocked",
                    "current_state_version": cur_sv,
                    "http_status": 409,
                }
            out_sv = int(r2[0] or next_sv)
            if rid:
                save_write_op_result(conn, tg_id, rid, out_sv)
            conn.commit()

        try:
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
            )
        except Exception:
            pass
        try:
            maybe_auto_player_snapshot(tg_id, out_sv)
        except Exception:
            pass
        try:
            redis_touch_player_rt(tg_id, out_sv)
            redis_publish_player_save(tg_id, out_sv)
        except Exception:
            pass
        pl = fetch_player(tg_id)
        out_ok: dict = {
            "ok": True,
            "player": _normalize_player_state_field(pl) if pl else None,
            "state_version": out_sv,
            "action": client_action,
        }
        if commit_meta:
            if isinstance(commit_meta.get("rewards_summary"), dict):
                out_ok["rewards_summary"] = commit_meta["rewards_summary"]
            if isinstance(commit_meta.get("battle_result"), dict):
                out_ok["battle_result"] = commit_meta["battle_result"]
        return out_ok
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _db_put(conn)


def _normalize_player_state_field(pl: dict | None) -> dict | None:
    if not isinstance(pl, dict):
        return pl
    raw_st = pl.get("state")
    if isinstance(raw_st, str):
        try:
            pl["state"] = json.loads(raw_st) if raw_st.strip() else {}
        except Exception:
            pl["state"] = {}
    elif raw_st is None:
        pl["state"] = {}
    elif not isinstance(raw_st, dict):
        pl["state"] = {}
    return pl


async def _player_upsert_execute(
    request,
    body: dict,
    user: dict,
    tg_id: str,
    cur_row: dict | None,
    is_admin_override: bool,
    t0_ms: int,
    *,
    event_endpoint: str,
    bootstrap_gate: bool = False,
    include_player_in_response: bool = False,
):
    incoming_state_guard = body.get("state", {})
    if not isinstance(incoming_state_guard, dict):
        incoming_state_guard = {}

    if bootstrap_gate:
        allow_b, breason = player_row_allows_bootstrap_write(cur_row, incoming_state_guard)
        if not allow_b:
            pl = await asyncio.to_thread(fetch_player, tg_id)
            csv = await asyncio.to_thread(fetch_player_state_version, tg_id)
            pl = _normalize_player_state_field(pl)
            return web.json_response(
                {
                    "ok": True,
                    "skipped": True,
                    "reason": breason,
                    "player": pl,
                    "state_version": int(csv if csv is not None else -1),
                    "tg_id": tg_id,
                },
                headers=cors_headers(),
            )
        ok_b2, err_b2 = validate_player_state_integrity(incoming_state_guard)
        if not ok_b2:
            return web.json_response(
                {"ok": False, "error": "state_integrity_failed", "reason": err_b2},
                status=400,
                headers=cors_headers(),
            )

    payload = _build_upsert_payload(body, user, tg_id)
    skip_wipe_guard = bool(is_admin_override and bool(body.get("admin_force_state_write", False)))
    if not skip_wipe_guard:
        wipe_block, wipe_reason = evaluate_state_wipe_block(cur_row, incoming_state_guard)
        if wipe_block:
            csv_w = await asyncio.to_thread(fetch_player_state_version, tg_id)
            try:
                record_security_sample(str(wipe_reason or "wipe_blocked")[:22], tg_id)
                player_progress_log("save_wipe_blocked", tg_id, reason=wipe_reason, state_version=csv_w)
            except Exception:
                pass
            return web.json_response(
                {
                    "ok": False,
                    "error": wipe_reason or "state_wipe_blocked",
                    "current_state_version": int(csv_w if csv_w is not None else -1),
                    "hint": "sync_pull_recommended",
                },
                status=409,
                headers=cors_headers(),
            )
    if is_admin_override and bool(body.get("admin_force_state_write", False)):
        current = cur_row
        current_state = (current or {}).get("state") if isinstance(current, dict) else {}
        current_state = current_state if isinstance(current_state, dict) else {}
        in_state = body.get("state", {})
        in_state = in_state if isinstance(in_state, dict) else {}
        replace_state = bool(body.get("admin_replace_state", False))
        merged_state = in_state if replace_state else _deep_merge_dict(current_state, in_state)
        payload["state"] = json.dumps(merged_state, ensure_ascii=False)
        payload["level"] = int(body.get("level", merged_state.get("level", payload.get("level", 1))) or 1)
        payload["xp"] = int(body.get("xp", merged_state.get("totalXp", merged_state.get("xp", payload.get("xp", 0)))) or 0)
        payload["gold"] = int(body.get("gold", merged_state.get("gold", payload.get("gold", 0))) or 0)
        payload["silver"] = int(body.get("silver", merged_state.get("silver", payload.get("silver", 0))) or 0)
        payload["tooth"] = int(body.get("tooth", merged_state.get("tooth", payload.get("tooth", 0))) or 0)
        payload["arena_power"] = int(
            body.get("arena_power", ((merged_state.get("arena", {}) or {}).get("power", payload.get("arena_power", 0))) or 0)
        )
        payload["arena_wins"] = int(
            body.get("arena_wins", ((merged_state.get("arena", {}) or {}).get("wins", payload.get("arena_wins", 0))) or 0)
        )
        payload["arena_losses"] = int(
            body.get("arena_losses", ((merged_state.get("arena", {}) or {}).get("losses", payload.get("arena_losses", 0))) or 0)
        )
        payload["boss_wins"] = int(
            body.get("boss_wins", ((merged_state.get("bosses", {}) or {}).get("wins", payload.get("boss_wins", 0))) or 0)
        )
        cur_sv = await asyncio.to_thread(fetch_player_state_version, tg_id)
        cur_sv_i = max(0, int(cur_sv or 0))
        payload["expected_state_version"] = cur_sv_i
        payload["next_state_version"] = cur_sv_i + 1
    elif _progression_max_merge_enabled():
        ex_st = _enrich_state_from_db_row(_parse_state_field((cur_row or {}).get("state")), cur_row)
        merged_st = merge_progression_max(ex_st, incoming_state_guard)
        payload["state"] = json.dumps(merged_st, ensure_ascii=False)
        _payload_refresh_denorms_from_state(payload, merged_st)
    skip_final_guards = bool(is_admin_override and bool(body.get("admin_force_state_write", False)))
    pre_bad = await asyncio.to_thread(pre_write_state_validation, cur_row, payload, skip_final_guards)
    if pre_bad:
        resp_obj, st = pre_bad
        try:
            record_security_sample(str(resp_obj.get("error") or "pre_write")[:22], tg_id)
            player_progress_log(
                "save_pre_write_blocked",
                tg_id,
                error=str(resp_obj.get("error") or ""),
                reason=str(resp_obj.get("reason") or ""),
                action_type=str(body.get("action_type") or "")[:48],
            )
        except Exception:
            pass
        return web.json_response(resp_obj, status=int(st or 400), headers=cors_headers())
    request_id = str(body.get("request_id", "") or "").strip()[:64]
    if not request_id:
        request_id = str(uuid.uuid4())
    written, state_version, duplicate = await asyncio.to_thread(_write_player_idempotent_with_resync, tg_id, payload, request_id)
    if not written:
        current_state_version = await asyncio.to_thread(fetch_player_state_version, tg_id)
        csv_int = int(current_state_version if current_state_version is not None else -1)
        try:
            record_security_sample("state_version_conflict", tg_id)
            player_progress_log("save_version_conflict", tg_id, current_state_version=csv_int, request_id=str(request_id)[:48])
        except Exception:
            pass
        return web.json_response(
            {
                "ok": False,
                "error": "state_regress_blocked",
                "reason": f"current_state_version={csv_int}",
                "current_state_version": csv_int,
            },
            status=409,
            headers=cors_headers(),
        )
    t1_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    sample_key = "bootstrap_ack" if bootstrap_gate else "save_ack"
    try:
        await asyncio.to_thread(record_rt_sample, sample_key, max(0, t1_ms - t0_ms))
    except Exception:
        pass
    try:
        await asyncio.to_thread(redis_touch_player_rt, tg_id, int(state_version or 0))
        await asyncio.to_thread(redis_publish_player_save, tg_id, int(state_version or 0))
    except Exception:
        pass
    action_type = str(body.get("action_type") or "save")[:64]
    client_reason = str(body.get("client_reason") or body.get("state_change_reason") or "")[:500]
    try:
        player_progress_log(
            "bootstrap_ok" if bootstrap_gate else "save_ok",
            tg_id,
            state_version=int(state_version or 0),
            duplicate=bool(duplicate),
            request_id=str(request_id)[:48],
            action_type=action_type,
            client_reason=client_reason[:240] if client_reason else None,
        )
    except Exception:
        pass
    try:
        final_st_ev = json.loads(str(payload.get("state") or "{}"))
    except Exception:
        final_st_ev = {}
    try:
        await asyncio.to_thread(
            append_player_state_event,
            tg_id,
            action_type,
            event_endpoint,
            str(request_id)[:64],
            int(state_version or 0),
            client_reason,
            _state_summary_for_event(final_st_ev if isinstance(final_st_ev, dict) else {}),
            {
                "admin_override": bool(is_admin_override),
                "duplicate": bool(duplicate),
                "force_immediate": bool(body.get("force_immediate")),
                "bootstrap_gate": bool(bootstrap_gate),
            },
        )
    except Exception:
        pass
    try:
        await asyncio.to_thread(maybe_auto_player_snapshot, tg_id, int(state_version or 0))
    except Exception:
        pass
    out = {
        "ok": True,
        "tg_id": tg_id,
        "state_version": state_version,
        "request_id": request_id,
        "duplicate": bool(duplicate),
    }
    if include_player_in_response:
        pl2 = await asyncio.to_thread(fetch_player, tg_id)
        out["player"] = _normalize_player_state_field(pl2)
    return web.json_response(out, headers=cors_headers())


async def upsert_player_handler(request):
    t0_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())

    actor_tg_id = str(user["id"])
    target_tg = str(body.get("target_tg_id", "")).strip()
    is_admin_override = bool(target_tg and actor_tg_id in ADMIN_TG_IDS)
    if is_admin_override:
        tg_id = target_tg
    else:
        tg_id = str(body.get("tg_id", actor_tg_id)).strip() or actor_tg_id
        if tg_id != actor_tg_id:
            tg_id = actor_tg_id

    cur_row = await asyncio.to_thread(fetch_player, tg_id)
    client_ip = request_client_ip(request)
    rl_ok, rl_reason = await asyncio.to_thread(check_player_save_rate_limits, tg_id, client_ip)
    if not rl_ok:
        try:
            record_security_sample(rl_reason or "ratelimit", tg_id)
            player_progress_log("save_rate_limited", tg_id, reason=rl_reason, ip=str(client_ip or "")[:45])
        except Exception:
            pass
        return web.json_response(
            {"ok": False, "error": rl_reason or "rate_limited", "hint": "slow_down"},
            status=429,
            headers=cors_headers(),
        )
    sess_ok, sess_err, sess_st = evaluate_save_session_gate(cur_row, body, is_admin_override)
    if not sess_ok:
        try:
            record_security_sample(sess_err or "session_block", tg_id)
            player_progress_log("save_session_blocked", tg_id, reason=sess_err, ip=str(client_ip or "")[:45])
        except Exception:
            pass
        hint = "call_session_start" if sess_err == "session_id_required" else "session_takeover_or_relogin"
        return web.json_response(
            {"ok": False, "error": sess_err, "hint": hint},
            status=int(sess_st or 403),
            headers=cors_headers(),
        )

    admin_force = bool(body.get("admin_force_state_write", False)) and actor_tg_id in ADMIN_TG_IDS
    if client_full_state_writes_forbidden() and not is_admin_override and not admin_force:
        try:
            record_security_sample("full_state_blocked", tg_id)
            player_progress_log("client_full_state_blocked", tg_id, endpoint="upsert_player")
        except Exception:
            pass
        return web.json_response(
            {
                "ok": False,
                "error": "client_full_state_save_forbidden",
                "hint": "patches_bootstrap_and_rpc_only_set_ALLOW_CLIENT_FULL_STATE_SAVE_to_migrate",
            },
            status=403,
            headers=cors_headers(),
        )

    return await _player_upsert_execute(
        request,
        body,
        user,
        tg_id,
        cur_row,
        is_admin_override,
        t0_ms,
        event_endpoint="upsert_player",
        bootstrap_gate=False,
        include_player_in_response=False,
    )


async def realtime_save_fast_handler(request):
    # Alias for low-latency client path; keeps one canonical save logic.
    return await upsert_player_handler(request)


async def player_bootstrap_v1_handler(request):
    """Единственный разрешённый полный state-write не из админки: только пустой stub-аккаунт."""
    t0_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())

    tg_id = str(user["id"])
    cur_row = await asyncio.to_thread(fetch_player, tg_id)
    client_ip = request_client_ip(request)
    rl_ok, rl_reason = await asyncio.to_thread(check_player_save_rate_limits, tg_id, client_ip)
    if not rl_ok:
        try:
            record_security_sample(rl_reason or "ratelimit", tg_id)
            player_progress_log("bootstrap_rate_limited", tg_id, reason=rl_reason, ip=str(client_ip or "")[:45])
        except Exception:
            pass
        return web.json_response(
            {"ok": False, "error": rl_reason or "rate_limited", "hint": "slow_down"},
            status=429,
            headers=cors_headers(),
        )
    sess_ok, sess_err, sess_st = evaluate_save_session_gate(cur_row, body, False)
    if not sess_ok:
        try:
            record_security_sample(sess_err or "session_block", tg_id)
            player_progress_log("bootstrap_session_blocked", tg_id, reason=sess_err, ip=str(client_ip or "")[:45])
        except Exception:
            pass
        hint = "call_session_start" if sess_err == "session_id_required" else "session_takeover_or_relogin"
        return web.json_response(
            {"ok": False, "error": sess_err, "hint": hint},
            status=int(sess_st or 403),
            headers=cors_headers(),
        )

    body = dict(body)
    if not str(body.get("action_type") or "").strip():
        body["action_type"] = "player_bootstrap_v1"
    if not str(body.get("client_reason") or "").strip():
        body["client_reason"] = "bootstrap_initial_seed"

    seed_nm = str(body.get("name") or "").strip()[:18] or ""
    body["state"] = build_server_seeded_player_state(user, seed_nm if seed_nm else None)

    return await _player_upsert_execute(
        request,
        body,
        user,
        tg_id,
        cur_row,
        False,
        t0_ms,
        event_endpoint="player_bootstrap_v1",
        bootstrap_gate=True,
        include_player_in_response=True,
    )


async def player_patch_v1_handler(request):
    t0_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    tg_id = str(user["id"])
    action = str(body.get("action", "")).strip()
    if action != "set_player_name":
        return web.json_response({"ok": False, "error": "unknown_patch_action"}, status=400, headers=cors_headers())

    cur_row = await asyncio.to_thread(fetch_player, tg_id)
    client_ip = request_client_ip(request)
    rl_ok, rl_reason = await asyncio.to_thread(check_player_save_rate_limits, tg_id, client_ip)
    if not rl_ok:
        try:
            record_security_sample(rl_reason or "ratelimit", tg_id)
            player_progress_log("patch_rate_limited", tg_id, reason=rl_reason)
        except Exception:
            pass
        return web.json_response(
            {"ok": False, "error": rl_reason or "rate_limited", "hint": "slow_down"},
            status=429,
            headers=cors_headers(),
        )
    sess_ok, sess_err, sess_st = evaluate_save_session_gate(cur_row, body, False)
    if not sess_ok:
        try:
            record_security_sample(sess_err or "session_block", tg_id)
            player_progress_log("patch_session_blocked", tg_id, reason=sess_err)
        except Exception:
            pass
        hint = "call_session_start" if sess_err == "session_id_required" else "session_takeover_or_relogin"
        return web.json_response(
            {"ok": False, "error": sess_err, "hint": hint},
            status=int(sess_st or 403),
            headers=cors_headers(),
        )

    try:
        ev_raw = body.get("expected_state_version")
        ev_i = int(ev_raw) if ev_raw is not None and str(ev_raw).strip() != "" else None
    except Exception:
        ev_i = None
    ph = str(body.get("photo_url", "") or "").strip() or None
    res = await asyncio.to_thread(player_patch_set_name, tg_id, str(body.get("name", "")), ev_i, ph)
    if not res.get("ok"):
        err = str(res.get("error") or "patch_failed")
        st = 400
        if err in {"state_regress_blocked", "name_taken", "patch_downgrade_blocked", "sharp_degradation_blocked"}:
            st = 409
        if err == "name_taken":
            st = 409
        if err == "expected_state_version_required":
            st = 409
        return web.json_response(res, status=st, headers=cors_headers())

    pl = res.get("player")
    if isinstance(pl, dict):
        raw_st = pl.get("state")
        if isinstance(raw_st, str):
            try:
                pl["state"] = json.loads(raw_st) if raw_st.strip() else {}
            except Exception:
                pl["state"] = {}
        elif raw_st is None:
            pl["state"] = {}
        elif not isinstance(raw_st, dict):
            pl["state"] = {}

    sv_out = int(res.get("state_version") or 0)
    req_id = str(body.get("request_id", "") or "").strip()[:64] or str(uuid.uuid4())
    try:
        await asyncio.to_thread(
            append_player_state_event,
            tg_id,
            "set_player_name",
            "player_patch_v1",
            req_id,
            sv_out,
            "player_rename_patch",
            _state_summary_for_event(pl.get("state") if isinstance(pl, dict) else {}),
            {"action": "set_player_name"},
        )
    except Exception:
        pass
    try:
        await asyncio.to_thread(redis_touch_player_rt, tg_id, sv_out)
        await asyncio.to_thread(redis_publish_player_save, tg_id, sv_out)
    except Exception:
        pass
    try:
        await asyncio.to_thread(maybe_auto_player_snapshot, tg_id, sv_out)
    except Exception:
        pass
    try:
        t1_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        await asyncio.to_thread(record_rt_sample, "patch_ack", max(0, t1_ms - t0_ms))
    except Exception:
        pass
    try:
        player_progress_log("patch_ok", tg_id, action="set_player_name", state_version=sv_out)
    except Exception:
        pass
    return web.json_response(
        {"ok": True, "tg_id": tg_id, "player": pl, "state_version": sv_out, "request_id": req_id},
        headers=cors_headers(),
    )


async def player_game_action_v1_handler(request):
    """Игровые действия без full-state: лавка, арена, расходники (в т.ч. ГБ с battle в теле)."""
    t0_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    tg_id = str(user["id"])
    client_ip = request_client_ip(request)
    result = await asyncio.to_thread(player_game_action_sync, tg_id, user, body, client_ip)
    http_st = int(result.get("http_status") or 200)
    resp = {k: v for k, v in result.items() if k != "http_status"}
    try:
        if result.get("ok") and result.get("state_version") is not None:
            t1_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            await asyncio.to_thread(record_rt_sample, "game_action_ack", max(0, t1_ms - t0_ms))
    except Exception:
        pass
    return web.json_response(resp, status=http_st, headers=cors_headers())


async def realtime_boss_metric_emit_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    metrics = body.get("metrics")
    if not isinstance(metrics, dict):
        return web.json_response({"ok": False, "error": "bad_metrics"}, status=400, headers=cors_headers())
    accepted = 0
    for k, v in metrics.items():
        try:
            vv = int(v or 0)
        except Exception:
            vv = 0
        if vv <= 0:
            continue
        await asyncio.to_thread(record_boss_metric, str(k), vv)
        accepted += 1
    return web.json_response({"ok": True, "accepted": int(accepted), "tg_id": str(user.get("id", ""))}, headers=cors_headers())


async def admin_snapshot_create_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    target_tg = str(body.get("target_tg_id", "")).strip()
    if not target_tg:
        return web.json_response({"ok": False, "error": "missing_target_tg_id"}, status=400, headers=cors_headers())
    note = str(body.get("note", "") or "")[:300]
    snap = await asyncio.to_thread(admin_snapshot_create, actor_tg, target_tg, note)
    if not snap:
        return web.json_response({"ok": False, "error": "player_not_found"}, status=404, headers=cors_headers())
    await asyncio.to_thread(admin_audit_log, actor_tg, "snapshot_create", target_tg, {"snapshot_id": snap.get("id"), "note": note})
    return web.json_response({"ok": True, "snapshot": snap}, headers=cors_headers())


async def admin_snapshot_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    target_tg = str(body.get("target_tg_id", "")).strip()
    if not target_tg:
        return web.json_response({"ok": False, "error": "missing_target_tg_id"}, status=400, headers=cors_headers())
    limit = int(body.get("limit", 30) or 30)
    rows = await asyncio.to_thread(admin_snapshot_list, target_tg, limit)
    return web.json_response({"ok": True, "snapshots": rows}, headers=cors_headers())


async def admin_snapshot_restore_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    target_tg = str(body.get("target_tg_id", "")).strip()
    snapshot_id = int(body.get("snapshot_id", 0) or 0)
    if not target_tg or snapshot_id <= 0:
        return web.json_response({"ok": False, "error": "bad_restore_args"}, status=400, headers=cors_headers())
    res = await asyncio.to_thread(admin_snapshot_restore, actor_tg, target_tg, snapshot_id)
    return web.json_response(res, headers=cors_headers())


async def admin_snapshot_restore_latest_auto_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    target_tg = str(body.get("target_tg_id", "")).strip()
    if not target_tg:
        return web.json_response({"ok": False, "error": "missing_target_tg_id"}, status=400, headers=cors_headers())
    res = await asyncio.to_thread(admin_snapshot_restore_latest_auto, actor_tg, target_tg)
    return web.json_response(res, headers=cors_headers())


def admin_player_recovery_report(target_tg_id: str, snapshot_id: int | None = None, events_limit: int = 500):
    """Отчёт для аварийного восстановления: последний (или выбранный) snapshot + хвост журнала после него."""
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            if snapshot_id and int(snapshot_id) > 0:
                cur.execute(
                    """
                    select id, created_at, note, state_version
                    from public.admin_player_snapshots
                    where id=%s and target_tg_id=%s
                    limit 1
                    """,
                    (int(snapshot_id), str(target_tg_id)),
                )
            else:
                cur.execute(
                    """
                    select id, created_at, note, state_version
                    from public.admin_player_snapshots
                    where target_tg_id=%s
                    order by id desc
                    limit 1
                    """,
                    (str(target_tg_id),),
                )
            snap = cur.fetchone()
        conn.commit()
        if not snap:
            return {"ok": False, "error": "no_snapshot"}
        sid, cat, note, ssv = int(snap[0]), snap[1], snap[2], int(snap[3] or 0)
        lim = max(1, min(2000, int(events_limit or 500)))
        conn2 = _db_conn()
        try:
            with conn2.cursor() as cur:
                cur.execute(
                    """
                    select id, created_at, action_type, endpoint, state_version_after, client_reason, summary
                    from public.player_state_events
                    where tg_id=%s and created_at > %s
                    order by id asc
                    limit %s
                    """,
                    (str(target_tg_id), cat, lim),
                )
                erows = cur.fetchall() or []
            conn2.commit()
        finally:
            _db_put(conn2)
        events = []
        for r in erows:
            events.append(
                {
                    "id": int(r[0]),
                    "created_at": r[1].isoformat() if r[1] else None,
                    "action_type": str(r[2] or ""),
                    "endpoint": str(r[3] or ""),
                    "state_version_after": int(r[4] or 0),
                    "client_reason": str(r[5] or ""),
                    "summary": r[6] if isinstance(r[6], dict) else {},
                }
            )
        return {
            "ok": True,
            "snapshot": {
                "id": sid,
                "created_at": cat.isoformat() if cat else None,
                "note": str(note or ""),
                "state_version": ssv,
            },
            "events_after_snapshot": events,
            "events_after_count": len(events),
            "replay_note": (
                "Авто-replay всех player_state_events в state сейчас невозможен: в журнале нет полных дельт JSON. "
                "Снимок — базовая точка отката; события — аудит и ручная диагностика."
            ),
        }
    finally:
        _db_put(conn)


def admin_player_recovery_apply(actor_tg_id: str, target_tg_id: str, snapshot_id: int | None = None) -> dict:
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            if snapshot_id and int(snapshot_id) > 0:
                cur.execute(
                    """
                    select id from public.admin_player_snapshots
                    where id=%s and target_tg_id=%s
                    limit 1
                    """,
                    (int(snapshot_id), str(target_tg_id)),
                )
            else:
                cur.execute(
                    """
                    select id from public.admin_player_snapshots
                    where target_tg_id=%s
                    order by id desc
                    limit 1
                    """,
                    (str(target_tg_id),),
                )
            row = cur.fetchone()
        conn.commit()
        if not row:
            return {"ok": False, "error": "no_snapshot"}
        sid = int(row[0])
    finally:
        _db_put(conn)
    r = admin_snapshot_restore(actor_tg_id, target_tg_id, sid)
    if isinstance(r, dict):
        r["recovery_snapshot_id"] = sid
        r["replay_note"] = (
            "Полный replay событий не выполняется (нет дельт в player_state_events). "
            "Проверьте отчёт mode=report и при необходимости patch вручную."
        )
    return r


async def admin_player_recovery_v1_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    target_tg = str(body.get("target_tg_id", "")).strip()
    if not target_tg:
        return web.json_response({"ok": False, "error": "missing_target_tg_id"}, status=400, headers=cors_headers())
    mode = str(body.get("mode") or "report").strip().lower()
    raw_sid = body.get("snapshot_id")
    sid = int(raw_sid) if raw_sid not in (None, "") else 0
    events_limit = int(body.get("events_limit", 500) or 500)
    if mode == "apply":
        if not bool(body.get("confirm_apply")):
            return web.json_response({"ok": False, "error": "confirm_apply_required"}, status=400, headers=cors_headers())
        res = await asyncio.to_thread(admin_player_recovery_apply, actor_tg, target_tg, sid if sid > 0 else None)
        return web.json_response(res, headers=cors_headers())
    rep = await asyncio.to_thread(admin_player_recovery_report, target_tg, sid if sid > 0 else None, events_limit)
    return web.json_response(rep, headers=cors_headers())


async def admin_player_state_events_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    target_tg = str(body.get("target_tg_id", "")).strip()
    if not target_tg:
        return web.json_response({"ok": False, "error": "missing_target_tg_id"}, status=400, headers=cors_headers())
    limit = int(body.get("limit", 80) or 80)
    rows = await asyncio.to_thread(admin_player_state_events_list, target_tg, limit)
    return web.json_response({"ok": True, "events": rows}, headers=cors_headers())


async def admin_force_logout_player_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    target_tg = str(body.get("target_tg_id", "")).strip()
    if not target_tg:
        return web.json_response({"ok": False, "error": "missing_target_tg_id"}, status=400, headers=cors_headers())
    res = await asyncio.to_thread(admin_force_logout_player, actor_tg, target_tg)
    return web.json_response(res, headers=cors_headers())


async def admin_audit_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    target_tg = str(body.get("target_tg_id", "")).strip()
    limit = int(body.get("limit", 100) or 100)
    rows = await asyncio.to_thread(admin_audit_list, limit, target_tg or None)
    return web.json_response({"ok": True, "events": rows}, headers=cors_headers())


async def admin_list_players_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    query = str(body.get("query", "") or "")
    limit = int(body.get("limit", 50) or 50)
    offset = int(body.get("offset", 0) or 0)
    filters = {
        "clan_id": body.get("clan_id"),
        "min_level": body.get("min_level"),
        "max_level": body.get("max_level"),
        "active_within_hours": body.get("active_within_hours"),
    }
    rows = await asyncio.to_thread(admin_list_players, query, limit, offset, filters)
    return web.json_response({"ok": True, "players": rows}, headers=cors_headers())


async def admin_player_godmode_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    if not _is_super_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_super_admin_only"}, status=403, headers=cors_headers())
    danger_token = str(body.get("danger_token", "")).strip()
    if not _consume_admin_danger_token(actor_tg, "admin_player_godmode", danger_token):
        return web.json_response({"ok": False, "error": "danger_token_required"}, status=400, headers=cors_headers())
    target_tg = str(body.get("target_tg_id", "")).strip()
    if not target_tg:
        return web.json_response({"ok": False, "error": "missing_target_tg_id"}, status=400, headers=cors_headers())
    res = await asyncio.to_thread(admin_make_player_god, actor_tg, target_tg)
    return web.json_response(res, headers=cors_headers())


async def admin_player_reset_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    if not _is_super_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_super_admin_only"}, status=403, headers=cors_headers())
    danger_token = str(body.get("danger_token", "")).strip()
    if not _consume_admin_danger_token(actor_tg, "admin_player_reset", danger_token):
        return web.json_response({"ok": False, "error": "danger_token_required"}, status=400, headers=cors_headers())
    target_tg = str(body.get("target_tg_id", "")).strip()
    if not target_tg:
        return web.json_response({"ok": False, "error": "missing_target_tg_id"}, status=400, headers=cors_headers())
    res = await asyncio.to_thread(admin_reset_player_state, actor_tg, target_tg)
    return web.json_response(res, headers=cors_headers())


async def admin_bulk_grant_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    if not _is_super_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_super_admin_only"}, status=403, headers=cors_headers())
    is_dry_run = bool((body or {}).get("dry_run", False))
    if not is_dry_run:
        danger_token = str(body.get("danger_token", "")).strip()
        if not _consume_admin_danger_token(actor_tg, "admin_bulk_grant", danger_token):
            return web.json_response({"ok": False, "error": "danger_token_required"}, status=400, headers=cors_headers())
    res = await asyncio.to_thread(admin_bulk_grant, actor_tg, body if isinstance(body, dict) else {})
    return web.json_response(res, headers=cors_headers())


async def promo_redeem_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    code = str(body.get("code", "")).strip()
    try:
        await asyncio.to_thread(promo_campaign_tick)
    except Exception:
        pass
    res = await asyncio.to_thread(promo_apply_to_player, str(user.get("id", "")), code)
    try:
        uid = str(user.get("id", ""))
        await asyncio.to_thread(
            append_player_activity_event,
            uid,
            "promo_redeem",
            "promo_code",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"code": code[:32], "ok": bool(res.get("ok")) if isinstance(res, dict) else False},
            None,
        )
    except Exception:
        pass
    return web.json_response(res, headers=cors_headers())


async def admin_promo_upsert_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    res = await asyncio.to_thread(promo_create_or_update, actor_tg, body if isinstance(body, dict) else {})
    return web.json_response(res, headers=cors_headers())


async def admin_promo_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    limit = int(body.get("limit", 100) or 100)
    rows = await asyncio.to_thread(promo_list, limit)
    return web.json_response({"ok": True, "promos": rows}, headers=cors_headers())


async def admin_promo_targets_set_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    code = str(body.get("code", "")).strip()
    tg_ids = body.get("tg_ids", [])
    if not isinstance(tg_ids, list):
        return web.json_response({"ok": False, "error": "bad_tg_ids"}, status=400, headers=cors_headers())
    res = await asyncio.to_thread(promo_targets_set, actor_tg, code, tg_ids)
    return web.json_response(res, headers=cors_headers())


async def admin_promo_targets_get_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    code = str(body.get("code", "")).strip()
    rows = await asyncio.to_thread(promo_targets_get, code, int(body.get("limit", 5000) or 5000))
    return web.json_response({"ok": True, "code": promo_norm(code), "tg_ids": rows}, headers=cors_headers())


async def admin_promo_auto_rule_upsert_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    res = await asyncio.to_thread(promo_auto_rule_upsert, actor_tg, body if isinstance(body, dict) else {})
    return web.json_response(res, headers=cors_headers())


async def admin_promo_auto_rule_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    rows = await asyncio.to_thread(promo_auto_rule_list, int(body.get("limit", 300) or 300))
    return web.json_response({"ok": True, "rules": rows}, headers=cors_headers())


async def admin_promo_auto_rule_delete_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    rid = int(body.get("id", 0) or 0)
    if rid <= 0:
        return web.json_response({"ok": False, "error": "bad_id"}, status=400, headers=cors_headers())
    res = await asyncio.to_thread(promo_auto_rule_delete, actor_tg, rid)
    return web.json_response(res, headers=cors_headers())


async def admin_promo_report_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    code = str(body.get("code", "")).strip()
    limit = int(body.get("limit", 300) or 300)
    out = await asyncio.to_thread(promo_report, code, limit)
    return web.json_response(out, headers=cors_headers())


async def admin_promo_holiday_run_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    event_key = str(body.get("event_key", "holiday")).strip()[:80]
    tg_ids = body.get("tg_ids", [])
    if not isinstance(tg_ids, list):
        tg_ids = []
    out = []
    for tid in tg_ids:
        t = str(tid or "").strip()
        if not t:
            continue
        res = await asyncio.to_thread(promo_run_auto_event, t, "holiday", event_key)
        out.append({"tg_id": t, "applied": (res or {}).get("applied", [])})
    admin_audit_log(actor_tg, "promo_holiday_run", "", {"event_key": event_key, "targets": len(out)})
    return web.json_response({"ok": True, "event_key": event_key, "results": out}, headers=cors_headers())


async def admin_promo_holiday_run_filters_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    event_key = str(body.get("event_key", "holiday")).strip()[:80]
    query = str(body.get("query", "") or "").strip()
    limit = int(body.get("limit", 2000) or 2000)
    filters = {
        "clan_id": body.get("clan_id"),
        "min_level": body.get("min_level"),
        "max_level": body.get("max_level"),
        "active_within_hours": body.get("active_within_hours"),
    }
    players = await asyncio.to_thread(admin_list_players, query, limit, 0, filters)
    dry_run = bool(body.get("dry_run", True))
    if dry_run:
        sample = [{"tg_id": str(x.get("tg_id", "")), "name": str(x.get("name", "")), "level": int(x.get("level", 1) or 1)} for x in players[:80]]
        return web.json_response({"ok": True, "dry_run": True, "matched": int(len(players)), "sample": sample}, headers=cors_headers())
    out = []
    for p in players:
        tid = str(p.get("tg_id", "") or "").strip()
        if not tid:
            continue
        res = await asyncio.to_thread(promo_run_auto_event, tid, "holiday", event_key)
        out.append({"tg_id": tid, "applied": (res or {}).get("applied", [])})
    admin_audit_log(actor_tg, "promo_holiday_run_filters", "", {"event_key": event_key, "matched": len(players), "query": query, "filters": filters})
    return web.json_response({"ok": True, "event_key": event_key, "matched": int(len(players)), "results": out}, headers=cors_headers())


async def admin_promo_campaign_upsert_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    res = await asyncio.to_thread(promo_campaign_upsert, actor_tg, body if isinstance(body, dict) else {})
    return web.json_response(res, headers=cors_headers())


async def admin_promo_campaign_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    rows = await asyncio.to_thread(promo_campaign_list, int(body.get("limit", 300) or 300))
    return web.json_response({"ok": True, "campaigns": rows}, headers=cors_headers())


async def admin_promo_campaign_delete_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    cid = int(body.get("id", 0) or 0)
    if cid <= 0:
        return web.json_response({"ok": False, "error": "bad_id"}, status=400, headers=cors_headers())
    res = await asyncio.to_thread(promo_campaign_delete, actor_tg, cid)
    return web.json_response(res, headers=cors_headers())


async def admin_promo_campaign_tick_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_admin_only"}, status=403, headers=cors_headers())
    res = await asyncio.to_thread(promo_campaign_tick)
    admin_audit_log(actor_tg, "promo_campaign_tick", "", {"changed": int((res or {}).get("changed", 0) or 0)})
    return web.json_response(res, headers=cors_headers())


async def admin_danger_token_start_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    actor_tg = str(user.get("id", ""))
    if not _is_super_admin_tg(actor_tg):
        return web.json_response({"ok": False, "error": "forbidden_super_admin_only"}, status=403, headers=cors_headers())
    action = str(body.get("action", "")).strip()
    if action not in {"admin_player_godmode", "admin_player_reset", "admin_bulk_grant"}:
        return web.json_response({"ok": False, "error": "invalid_action"}, status=400, headers=cors_headers())
    ttl = int(body.get("ttl_sec", 90) or 90)
    out = _issue_admin_danger_token(actor_tg, action, ttl)
    return web.json_response({"ok": True, "token": out.get("token"), "expires_at": out.get("expires_at"), "action": action}, headers=cors_headers())


async def session_start_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    tg_id = str(user["id"])
    try:
        await asyncio.to_thread(promo_campaign_tick)
    except Exception:
        pass
    session_id = str(uuid.uuid4())
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.players (tg_id,name,photo_url,active_session_id,active_session_updated_at,active_device_id,updated_at)
                values (%s,%s,%s,%s,now(),%s,now())
                on conflict (tg_id) do update set
                  active_session_id=excluded.active_session_id,
                  active_session_updated_at=excluded.active_session_updated_at,
                  active_device_id=excluded.active_device_id,
                  updated_at=excluded.updated_at
                """,
                (tg_id, str(user.get("first_name", "Player"))[:18], str(user.get("photo_url", "")), session_id, str(body.get("device_id", body.get("deviceId", "")))),
            )
        conn.commit()
    try:
        await asyncio.to_thread(promo_run_auto_event, tg_id, "first_login", "")
    except Exception:
        pass
    player = await asyncio.to_thread(fetch_player, tg_id)
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            tg_id,
            "session_start",
            "session_begin",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"session_id": str(session_id)[:36]},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True, "session_id": session_id, "player": player}, headers=cors_headers())


async def district_leaders_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, _user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    limit = int(body.get("limit", 100) or 100)
    leaders = await asyncio.to_thread(list_district_leaders, limit)
    return web.json_response({"ok": True, "leaders": leaders}, headers=cors_headers())


async def district_daily_leaders_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, _user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    limit = int(body.get("limit", 100) or 100)
    day = str(body.get("day", "") or "").strip()
    if day:
        leaders = await asyncio.to_thread(list_district_daily_leaders_for_day, day, limit)
    else:
        leaders = await asyncio.to_thread(list_district_daily_leaders, limit)
    return web.json_response({"ok": True, "leaders": leaders}, headers=cors_headers())


async def district_leader_upsert_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    # Compatibility endpoint for legacy client calls; data is derived from players table.
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            str(user.get("id", "")),
            "district_leader_upsert",
            "district_leader_compat",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True, "tg_id": str(user.get("id", "")), "compat": True}, headers=cors_headers())


async def district_daily_leader_upsert_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    day = str(body.get("day", "") or "").strip()
    district_key = str(body.get("district_key", "") or "").strip()
    if not day or not district_key:
        return web.json_response({"ok": False, "error": "missing_day_or_district_key"}, status=400, headers=cors_headers())
    try:
        fear = max(0, int(body.get("fear", 0) or 0))
    except Exception:
        fear = 0
    name = str(body.get("name", "") or "").strip()[:80]
    photo_url = str(body.get("photo_url", "") or "").strip()[:500]
    tg_id = str(user.get("id", "") or "").strip()
    await asyncio.to_thread(
        upsert_district_daily_leader,
        day,
        district_key,
        tg_id,
        name,
        fear,
        photo_url,
    )
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            tg_id,
            "district_daily_leader_upsert",
            "district_daily_leader",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"day": day[:16], "district_key": district_key[:48], "fear": int(fear)},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True, "tg_id": tg_id}, headers=cors_headers())


async def boss_last_winners_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, _user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    limit = int(body.get("limit", 50) or 50)
    winners = await asyncio.to_thread(list_boss_last_winners, limit)
    return web.json_response({"ok": True, "winners": winners}, headers=cors_headers())


async def top_players_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, _user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    kind = str(body.get("kind", "LVL") or "LVL")
    limit = int(body.get("limit", 100) or 100)
    players = await asyncio.to_thread(list_top_players, kind, limit)
    return web.json_response({"ok": True, "players": players}, headers=cors_headers())


async def boss_fights_list_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    fights = await asyncio.to_thread(list_boss_fights, str(user["id"]))
    return web.json_response({"ok": True, "fights": fights}, headers=cors_headers())


async def boss_fight_start_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())

    boss_id = int(body.get("boss_id", body.get("bossId", 0)) or 0)
    if boss_id <= 0:
        return web.json_response({"ok": False, "error": "bad_boss_id"}, status=400, headers=cors_headers())
    max_hp = int(body.get("max_hp", body.get("maxHp", 1000)) or 1000)
    if max_hp <= 0:
        max_hp = 1000
    expires_at = body.get("expires_at")
    row = await asyncio.to_thread(start_boss_fight, str(user["id"]), boss_id, max_hp, expires_at)
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            str(user["id"]),
            "boss_fight_start",
            "boss_start",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"boss_id": int(boss_id), "max_hp": int(max_hp)},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True, "fight": row}, headers=cors_headers())


async def boss_help_pull_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    limit = int(body.get("limit", 500) or 500)
    events = await asyncio.to_thread(pull_boss_help_events, str(user["id"]), limit)
    return web.json_response({"ok": True, "events": events}, headers=cors_headers())


async def list_clans_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, _user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    limit = int(body.get("limit", 200) or 200)
    clans = await asyncio.to_thread(list_clans, limit)
    return web.json_response({"ok": True, "clans": clans}, headers=cors_headers())


async def upsert_clan_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    clan_id = str(body.get("id", "")).strip()
    name = str(body.get("name", "")).strip()
    data_obj = body.get("data", {})
    if not clan_id or not name or not isinstance(data_obj, dict):
        return web.json_response({"ok": False, "error": "bad_payload"}, status=400, headers=cors_headers())
    row = await asyncio.to_thread(upsert_clan, clan_id, name, str(user["id"]), data_obj)
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            str(user["id"]),
            "upsert_clan",
            "clan_upsert",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"clan_id": clan_id[:32], "name": name[:48]},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True, "clan": row}, headers=cors_headers())


async def clan_apply_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    clan_id = str(body.get("clan_id", body.get("id", "")) or "").strip()
    applicant = str(body.get("applicant_name", body.get("name", "")) or "").strip()[:18]
    err = await asyncio.to_thread(clan_apply_append, clan_id, applicant)
    if err in ("bad_args", "bad_clan_id"):
        return web.json_response({"ok": False, "error": err}, status=400, headers=cors_headers())
    if err == "not_found":
        return web.json_response({"ok": False, "error": "not_found"}, status=404, headers=cors_headers())
    if err:
        return web.json_response({"ok": False, "error": str(err)}, status=500, headers=cors_headers())
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            str(user.get("id", "")),
            "clan_apply",
            "clan_apply",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"clan_id": clan_id[:32], "applicant": applicant},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True}, headers=cors_headers())


def _clan_err_response(err: str):
    if err in ("bad_args", "bad_clan_id", "bad_member"):
        return web.json_response({"ok": False, "error": err}, status=400, headers=cors_headers())
    if err == "not_found":
        return web.json_response({"ok": False, "error": "not_found"}, status=404, headers=cors_headers())
    if err == "forbidden":
        return web.json_response({"ok": False, "error": "forbidden"}, status=403, headers=cors_headers())
    if err == "leader_must_transfer":
        return web.json_response({"ok": False, "error": "leader_must_transfer"}, status=409, headers=cors_headers())
    if err:
        return web.json_response({"ok": False, "error": str(err)}, status=500, headers=cors_headers())
    return None


async def clan_accept_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    clan_id = str(body.get("clan_id", body.get("id", "")) or "").strip()
    applicant = str(body.get("applicant_name", body.get("name", "")) or "").strip()[:18]
    err = await asyncio.to_thread(clan_accept_member, clan_id, str(user.get("id", "")), applicant)
    resp = _clan_err_response(err or "")
    if resp:
        return resp
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            str(user.get("id", "")),
            "clan_accept",
            "clan_accept",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"clan_id": clan_id[:32], "applicant": applicant},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True}, headers=cors_headers())


async def clan_reject_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    clan_id = str(body.get("clan_id", body.get("id", "")) or "").strip()
    applicant = str(body.get("applicant_name", body.get("name", "")) or "").strip()[:18]
    err = await asyncio.to_thread(clan_reject_applicant, clan_id, str(user.get("id", "")), applicant)
    resp = _clan_err_response(err or "")
    if resp:
        return resp
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            str(user.get("id", "")),
            "clan_reject",
            "clan_reject",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"clan_id": clan_id[:32], "applicant": applicant},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True}, headers=cors_headers())


async def clan_cancel_apply_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    clan_id = str(body.get("clan_id", body.get("id", "")) or "").strip()
    applicant = str(body.get("applicant_name", body.get("name", "")) or "").strip()[:18]
    err = await asyncio.to_thread(clan_cancel_apply, clan_id, applicant)
    resp = _clan_err_response(err or "")
    if resp:
        return resp
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            str(user.get("id", "")),
            "clan_cancel_apply",
            "clan_cancel_apply",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"clan_id": clan_id[:32], "applicant": applicant},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True}, headers=cors_headers())


async def clan_leave_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    clan_id = str(body.get("clan_id", body.get("id", "")) or "").strip()
    member = str(body.get("member_name", body.get("name", "")) or "").strip()[:18]
    err, deleted = await asyncio.to_thread(clan_leave_member, clan_id, member)
    if err:
        resp = _clan_err_response(err)
        if resp:
            return resp
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            str(user.get("id", "")),
            "clan_leave",
            "clan_leave",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"clan_id": clan_id[:32], "member": member, "deleted": bool(deleted)},
            None,
        )
    except Exception:
        pass
    return web.json_response({"ok": True, "deleted": bool(deleted)}, headers=cors_headers())


async def boss_fight_get_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    boss_id = int(body.get("boss_id", body.get("bossId", 0)) or 0)
    if boss_id <= 0:
        return web.json_response({"ok": False, "error": "bad_boss_id"}, status=400, headers=cors_headers())
    fight = await asyncio.to_thread(get_boss_fight, str(user["id"]), boss_id)
    log = await asyncio.to_thread(redis_boss_log_get, str(user["id"]), boss_id)
    top = await asyncio.to_thread(redis_boss_top_get, str(user["id"]), boss_id)
    return web.json_response({"ok": True, "fight": fight, "log": log, "top": top}, headers=cors_headers())


async def boss_fight_hit_handler(request):
    t0_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    boss_id = int(body.get("boss_id", body.get("bossId", 0)) or 0)
    if boss_id <= 0:
        return web.json_response({"ok": False, "error": "bad_boss_id"}, status=400, headers=cors_headers())
    dmg = max(0, int(body.get("dmg", 0) or 0))
    max_hp = max(1, int(body.get("max_hp", body.get("maxHp", 2500)) or 2500))
    expires_at = body.get("expires_at")
    from_name = str(body.get("from_name", "PLAYER") or "PLAYER").strip()[:18]
    event_id = str(body.get("event_id", "") or "").strip()[:64]
    if not event_id:
        event_id = uuid.uuid4().hex
    tg_id = str(user["id"])
    claimed_event = await asyncio.to_thread(redis_claim_boss_event, tg_id, boss_id, event_id, 28800)
    if claimed_event:
        fight = await asyncio.to_thread(hit_boss_fight, tg_id, boss_id, dmg, max_hp, expires_at)
        try:
            await asyncio.to_thread(redis_boss_log_push, tg_id, boss_id, from_name, dmg, event_id)
            await asyncio.to_thread(redis_boss_top_add, tg_id, boss_id, from_name, dmg)
        except Exception:
            pass
    else:
        try:
            await asyncio.to_thread(record_boss_metric, "duplicate_event_dropped", 1)
        except Exception:
            pass
        fight = await asyncio.to_thread(get_boss_fight, tg_id, boss_id)
    log = await asyncio.to_thread(redis_boss_log_get, tg_id, boss_id)
    top = await asyncio.to_thread(redis_boss_top_get, tg_id, boss_id)
    seq_out = 0
    try:
        if claimed_event and fight and isinstance(fight, dict):
            seq = await asyncio.to_thread(redis_next_boss_seq, str(user["id"]), boss_id)
            seq_out = int(seq or 0)
            nonce = uuid.uuid4().hex
            payload = {
                "fight": fight,
                "log": log,
                "top": top,
                "seq": int(seq_out),
                "nonce": nonce,
                "event_id": event_id,
            }
            ch = f"rt:boss:hit:{str(user['id'])}"
            await asyncio.to_thread(redis_boss_event_append, str(user["id"]), boss_id, int(seq_out), payload)
            await asyncio.to_thread(_redis_call, "PUBLISH", ch, json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass
    t1_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        await asyncio.to_thread(record_rt_sample, "boss_update", max(0, t1_ms - t0_ms))
    except Exception:
        pass
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            tg_id,
            "boss_fight_hit",
            "boss_hit",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"boss_id": int(boss_id), "dmg": int(dmg), "duplicate": bool(not claimed_event)},
            {"event_id": str(event_id or "")[:48]},
        )
    except Exception:
        pass
    return web.json_response(
        {
            "ok": True,
            "fight": fight,
            "log": log,
            "top": top,
            "dmg_applied": int(dmg if claimed_event else 0),
            "event_id": event_id,
            "seq": int(seq_out or 0),
            "duplicate": bool(not claimed_event),
        },
        headers=cors_headers(),
    )


async def boss_hit_and_save_fast_handler(request):
    t0_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    tg_id = str(user["id"])
    boss_id = int(body.get("boss_id", body.get("bossId", 0)) or 0)
    if boss_id <= 0:
        return web.json_response({"ok": False, "error": "bad_boss_id"}, status=400, headers=cors_headers())
    dmg = max(0, int(body.get("dmg", 0) or 0))
    max_hp = max(1, int(body.get("max_hp", body.get("maxHp", 2500)) or 2500))
    expires_at = body.get("expires_at")
    from_name = str(body.get("from_name", "PLAYER") or "PLAYER").strip()[:18]
    event_id = str(body.get("event_id", "") or "").strip()[:64]
    if not event_id:
        event_id = uuid.uuid4().hex

    claimed_event = await asyncio.to_thread(redis_claim_boss_event, tg_id, boss_id, event_id, 28800)
    if claimed_event:
        fight = await asyncio.to_thread(hit_boss_fight, tg_id, boss_id, dmg, max_hp, expires_at)
        await asyncio.to_thread(redis_boss_log_push, tg_id, boss_id, from_name, dmg, event_id)
        await asyncio.to_thread(redis_boss_top_add, tg_id, boss_id, from_name, dmg)
    else:
        try:
            await asyncio.to_thread(record_boss_metric, "duplicate_event_dropped", 1)
        except Exception:
            pass
        fight = await asyncio.to_thread(get_boss_fight, tg_id, boss_id)
    log = await asyncio.to_thread(redis_boss_log_get, tg_id, boss_id)
    top = await asyncio.to_thread(redis_boss_top_get, tg_id, boss_id)

    save_result = {"ok": True, "skipped": True}
    if client_full_state_writes_forbidden() and isinstance(body.get("state"), dict):
        save_result = {"ok": True, "skipped": True, "reason": "client_full_state_save_disabled"}
    if isinstance(body.get("state"), dict) and not client_full_state_writes_forbidden():
        cur_boss_guard = await asyncio.to_thread(fetch_player, tg_id)
        client_ip_bs = request_client_ip(request)
        rl_ok_bs, rl_reason_bs = await asyncio.to_thread(check_player_save_rate_limits, tg_id, client_ip_bs)
        if not rl_ok_bs:
            try:
                record_security_sample(rl_reason_bs or "ratelimit", tg_id)
                player_progress_log("boss_save_rate_limited", tg_id, reason=rl_reason_bs, boss_id=int(boss_id))
            except Exception:
                pass
            save_result = {
                "ok": False,
                "error": rl_reason_bs or "rate_limited",
                "hint": "slow_down",
                "http_status": 429,
            }
        else:
            sess_ok_bs, sess_err_bs, sess_st_bs = evaluate_save_session_gate(cur_boss_guard, body, False)
            if not sess_ok_bs:
                try:
                    record_security_sample(sess_err_bs or "session_block", tg_id)
                    player_progress_log("boss_save_session_blocked", tg_id, reason=sess_err_bs, boss_id=int(boss_id))
                except Exception:
                    pass
                save_result = {
                    "ok": False,
                    "error": sess_err_bs,
                    "hint": "call_session_start" if sess_err_bs == "session_id_required" else "session_takeover_or_relogin",
                    "http_status": int(sess_st_bs or 403),
                }
            else:
                wipe_boss, wipe_reason_boss = evaluate_state_wipe_block(cur_boss_guard, body.get("state"))
                if wipe_boss:
                    csv_b = await asyncio.to_thread(fetch_player_state_version, tg_id)
                    try:
                        record_security_sample(str(wipe_reason_boss or "wipe_blocked")[:22], tg_id)
                        player_progress_log("boss_save_wipe_blocked", tg_id, reason=wipe_reason_boss, state_version=csv_b)
                    except Exception:
                        pass
                    save_result = {
                        "ok": False,
                        "error": wipe_reason_boss or "state_wipe_blocked",
                        "current_state_version": csv_b if csv_b is not None else -1,
                        "hint": "sync_pull_recommended",
                    }
                else:
                    payload = _build_upsert_payload(body, user, tg_id)
                    inc_bs = body.get("state") if isinstance(body.get("state"), dict) else {}
                    if _progression_max_merge_enabled():
                        ex_bs = _enrich_state_from_db_row(_parse_state_field((cur_boss_guard or {}).get("state")), cur_boss_guard)
                        merged_bs = merge_progression_max(ex_bs, inc_bs)
                        payload["state"] = json.dumps(merged_bs, ensure_ascii=False)
                        _payload_refresh_denorms_from_state(payload, merged_bs)
                    pre_bad_boss = await asyncio.to_thread(pre_write_state_validation, cur_boss_guard, payload, False)
                    if pre_bad_boss:
                        resp_b, st_b = pre_bad_boss
                        try:
                            record_security_sample(str(resp_b.get("error") or "pre_write")[:22], tg_id)
                            player_progress_log(
                                "boss_save_pre_write_blocked",
                                tg_id,
                                error=str(resp_b.get("error") or ""),
                                reason=str(resp_b.get("reason") or ""),
                                boss_id=int(boss_id),
                            )
                        except Exception:
                            pass
                        _csv_pre = await asyncio.to_thread(fetch_player_state_version, tg_id)
                        save_result = {
                            "ok": False,
                            "error": str(resp_b.get("error") or "save_blocked"),
                            "current_state_version": int(_csv_pre if _csv_pre is not None else -1),
                            "hint": resp_b.get("hint"),
                            "reason": resp_b.get("reason"),
                            "http_status": int(st_b or 400),
                        }
                    else:
                        request_id = str(body.get("request_id", "") or "").strip()[:64]
                        if not request_id:
                            request_id = str(uuid.uuid4())
                        written, state_version, duplicate = await asyncio.to_thread(
                            _write_player_idempotent_with_resync, tg_id, payload, request_id
                        )
                        if not written:
                            current_state_version = await asyncio.to_thread(fetch_player_state_version, tg_id)
                            try:
                                record_security_sample("state_version_conflict", tg_id)
                            except Exception:
                                pass
                            save_result = {
                                "ok": False,
                                "error": "state_regress_blocked",
                                "current_state_version": current_state_version if current_state_version is not None else -1,
                            }
                        else:
                            save_result = {
                                "ok": True,
                                "state_version": int(state_version or 0),
                                "request_id": request_id,
                                "duplicate": bool(duplicate),
                            }
                            try:
                                await asyncio.to_thread(redis_touch_player_rt, tg_id, int(state_version or 0))
                                await asyncio.to_thread(redis_publish_player_save, tg_id, int(state_version or 0))
                            except Exception:
                                pass
                            action_type_b = str(body.get("action_type") or "boss_hit_save")[:64]
                            reason_b = str(body.get("client_reason") or body.get("state_change_reason") or "")[:500]
                            if not reason_b:
                                reason_b = f"boss_hit boss_id={int(boss_id)}"
                            try:
                                final_b = json.loads(str(payload.get("state") or "{}"))
                            except Exception:
                                final_b = {}
                            try:
                                player_progress_log(
                                    "boss_save_ok",
                                    tg_id,
                                    state_version=int(state_version or 0),
                                    boss_id=int(boss_id),
                                    action_type=action_type_b,
                                    client_reason=reason_b[:240],
                                    request_id=str(request_id)[:48],
                                )
                            except Exception:
                                pass
                            try:
                                await asyncio.to_thread(
                                    append_player_state_event,
                                    tg_id,
                                    action_type_b,
                                    "boss_hit_and_save_fast",
                                    str(request_id)[:64],
                                    int(state_version or 0),
                                    reason_b,
                                    _state_summary_for_event(final_b if isinstance(final_b, dict) else {}),
                                    {"boss_id": int(boss_id), "dmg": int(dmg), "duplicate": bool(duplicate)},
                                )
                            except Exception:
                                pass
                            try:
                                await asyncio.to_thread(maybe_auto_player_snapshot, tg_id, int(state_version or 0))
                            except Exception:
                                pass

    try:
        req_b = str(body.get("request_id", "") or "").strip()[:64] or None
        await asyncio.to_thread(
            append_player_activity_event,
            tg_id,
            "boss_hit_and_save_fast",
            "boss_hit_request",
            req_b,
            {
                "boss_id": int(boss_id),
                "dmg": int(dmg),
                "save_ok": bool(save_result.get("ok")) if isinstance(save_result, dict) else False,
                "save_skipped": bool(save_result.get("skipped")) if isinstance(save_result, dict) else True,
            },
            {"event_id": str(event_id or "")[:48], "claimed": bool(claimed_event)},
        )
    except Exception:
        pass

    seq_out = 0
    try:
        if claimed_event:
            seq = await asyncio.to_thread(redis_next_boss_seq, tg_id, boss_id)
            seq_out = int(seq or 0)
            nonce = uuid.uuid4().hex
            payload = {"fight": fight, "log": log, "top": top}
            payload["seq"] = int(seq_out)
            payload["nonce"] = nonce
            payload["event_id"] = event_id
            ch = f"rt:boss:hit:{tg_id}"
            await asyncio.to_thread(redis_boss_event_append, tg_id, boss_id, int(seq_out), payload)
            await asyncio.to_thread(_redis_call, "PUBLISH", ch, json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass
    t1_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        await asyncio.to_thread(record_rt_sample, "boss_update", max(0, t1_ms - t0_ms))
    except Exception:
        pass
    return web.json_response(
        {
            "ok": True,
            "fight": fight,
            "log": log,
            "top": top,
            "save": save_result,
            "dmg_applied": int(dmg if claimed_event else 0),
            "event_id": event_id,
            "seq": int(seq_out or 0),
            "duplicate": bool(not claimed_event),
        },
        headers=cors_headers(),
    )


async def boss_fight_claim_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    boss_id = int(body.get("boss_id", body.get("bossId", 0)) or 0)
    try:
        await asyncio.to_thread(promo_campaign_tick)
    except Exception:
        pass
    if boss_id <= 0:
        return web.json_response({"ok": False, "error": "bad_boss_id"}, status=400, headers=cors_headers())
    tg_id = str(user["id"])
    client_ip = request_client_ip(request)
    result = await asyncio.to_thread(boss_fight_commit_rewards_sync, tg_id, boss_id, user, body, client_ip)
    http_st = int(result.get("http_status") or 200)
    resp = {k: v for k, v in result.items() if k != "http_status"}
    try:
        if result and bool(result.get("can_claim")):
            await asyncio.to_thread(promo_run_auto_event, tg_id, "boss_win", str(int(boss_id)))
    except Exception:
        pass
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            tg_id,
            "boss_fight_claim",
            "boss_claim",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {
                "boss_id": int(boss_id),
                "can_claim": bool(result.get("can_claim")) if isinstance(result, dict) else False,
                "server_commit": True,
            },
            None,
        )
    except Exception:
        pass
    return web.json_response(resp, status=http_st, headers=cors_headers())


async def find_arena_opponent_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    min_sum = int(body.get("min_sum", 0) or 0)
    max_sum = int(body.get("max_sum", 0) or 0)
    min_power = int(body.get("min_power", 0) or 0)
    max_power = int(body.get("max_power", 0) or 0)
    opp = await asyncio.to_thread(find_arena_opponent, str(user["id"]), min_sum, max_sum, min_power, max_power)
    return web.json_response({"ok": True, "opponent": opp}, headers=cors_headers())


async def stars_create_invoice_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    if bool(body.get("warm")):
        return web.json_response({"ok": True, "warm": True, "ready": False}, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, _user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    return web.json_response(
        {"ok": False, "skipped": True, "error": "stars_not_configured", "reason": "telegram_stars_backend_not_connected"},
        headers=cors_headers(),
    )


async def get_players_by_names_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, _user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    names = body.get("names", [])
    if not isinstance(names, list):
        return web.json_response({"ok": False, "error": "bad_names"}, status=400, headers=cors_headers())
    players = await asyncio.to_thread(get_players_by_names, names)
    return web.json_response({"ok": True, "players": players}, headers=cors_headers())


async def boss_help_send_handler(request):
    if request.method == "OPTIONS":
        return await options_ok(request)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400, headers=cors_headers())
    init_data = str(body.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
    ok, user, reason = authorize_user_from_init_data(init_data)
    if not ok:
        status = 403 if reason == "access_denied_whitelist" else 401
        return web.json_response({"ok": False, "error": reason or "unauthorized"}, status=status, headers=cors_headers())
    boss_id = int(body.get("boss_id", body.get("bossId", 0)) or 0)
    if boss_id <= 0:
        return web.json_response({"ok": False, "error": "bad_boss_id"}, status=400, headers=cors_headers())
    dmg = max(1, int(body.get("dmg", 0) or 0))
    clan_id = str(body.get("clan_id", "")).strip()
    from_name = str(body.get("from_name", "BRAT")).strip()[:18]
    result = await asyncio.to_thread(boss_help_send, str(user["id"]), boss_id, dmg, clan_id, from_name)
    try:
        recips = (((result or {}).get("debug") or {}).get("recipient_ids")) or []
        for to_tg in recips:
            payload = {
                "boss_id": int(boss_id),
                "from_name": from_name,
                "dmg": int(max(1, int(dmg or 0))),
                "clan_id": str(clan_id or ""),
                "to_tg_id": str(to_tg),
                "ts": now_iso(),
            }
            ch = f"rt:boss:help:{str(to_tg)}"
            await asyncio.to_thread(_redis_call, "PUBLISH", ch, json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass
    try:
        await asyncio.to_thread(
            append_player_activity_event,
            str(user["id"]),
            "boss_help_send",
            "boss_help",
            str(body.get("request_id", "") or "").strip()[:64] or None,
            {"boss_id": int(boss_id), "dmg": int(dmg), "clan_id": clan_id[:24]},
            {"ok": bool((result or {}).get("ok"))},
        )
    except Exception:
        pass
    return web.json_response(result, headers=cors_headers())


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    authed_tg = None
    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
            except Exception:
                data = {}
            tp = data.get("type")
            if tp == "auth":
                init_data = str(data.get("initData", "") or request.headers.get("x-telegram-init-data", "")).strip()
                ok, user, reason = authorize_user_from_init_data(init_data)
                if not ok:
                    await ws.send_json({"type": "auth_err", "error": reason or "unauthorized"})
                    continue
                authed_tg = str(user["id"])
                await _ws_register(authed_tg, ws)
                await ws.send_json({"type": "auth_ok", "online": 1})
            elif tp == "boss_replay_req":
                if not authed_tg:
                    await ws.send_json({"type": "boss_replay", "ok": False, "error": "unauthorized"})
                    continue
                try:
                    boss_id = int(data.get("boss_id", data.get("bossId", 0)) or 0)
                except Exception:
                    boss_id = 0
                if boss_id <= 0:
                    await ws.send_json({"type": "boss_replay", "ok": False, "error": "bad_boss_id"})
                    continue
                try:
                    since_seq = int(data.get("since_seq", data.get("sinceSeq", 0)) or 0)
                except Exception:
                    since_seq = 0
                try:
                    limit = int(data.get("limit", 120) or 120)
                except Exception:
                    limit = 120
                events = await asyncio.to_thread(redis_boss_events_after_seq, authed_tg, boss_id, since_seq, limit)
                try:
                    await asyncio.to_thread(record_boss_metric, "replay_requested_total", 1)
                    await asyncio.to_thread(record_boss_metric, "replay_served_events_total", len(events if isinstance(events, list) else []))
                except Exception:
                    pass
                await ws.send_json(
                    {
                        "type": "boss_replay",
                        "ok": True,
                        "boss_id": int(boss_id),
                        "since_seq": int(max(0, since_seq)),
                        "events": events if isinstance(events, list) else [],
                    }
                )
            elif tp == "pong":
                pass
            elif tp == "ping":
                await ws.send_json({"type": "pong"})
    if authed_tg:
        await _ws_unregister(authed_tg, ws)
    return ws


async def _on_startup(app: web.Application):
    try:
        await asyncio.to_thread(ensure_player_audit_schema)
    except Exception:
        pass
    app["redis_pubsub_task"] = asyncio.create_task(redis_pubsub_loop(app))


async def _on_cleanup(app: web.Application):
    task = app.get("redis_pubsub_task")
    if task:
        task.cancel()
        try:
            await task
        except Exception:
            pass


def build_app():
    app = web.Application()
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    app.router.add_route("GET", "/health", health)
    app.router.add_route("OPTIONS", "/health", health)
    app.router.add_route("POST", "/functions/v1/get_player", get_player_handler)
    app.router.add_route("OPTIONS", "/functions/v1/get_player", get_player_handler)
    app.router.add_route("POST", "/functions/v1/sync_pull_after_conflict", sync_pull_after_conflict_handler)
    app.router.add_route("OPTIONS", "/functions/v1/sync_pull_after_conflict", sync_pull_after_conflict_handler)
    app.router.add_route("POST", "/functions/v1/upsert_player", upsert_player_handler)
    app.router.add_route("OPTIONS", "/functions/v1/upsert_player", upsert_player_handler)
    app.router.add_route("POST", "/functions/v1/realtime_save_fast", realtime_save_fast_handler)
    app.router.add_route("OPTIONS", "/functions/v1/realtime_save_fast", realtime_save_fast_handler)
    app.router.add_route("POST", "/functions/v1/player_patch_v1", player_patch_v1_handler)
    app.router.add_route("OPTIONS", "/functions/v1/player_patch_v1", player_patch_v1_handler)
    app.router.add_route("POST", "/functions/v1/player_game_action_v1", player_game_action_v1_handler)
    app.router.add_route("OPTIONS", "/functions/v1/player_game_action_v1", player_game_action_v1_handler)
    app.router.add_route("POST", "/functions/v1/player_bootstrap_v1", player_bootstrap_v1_handler)
    app.router.add_route("OPTIONS", "/functions/v1/player_bootstrap_v1", player_bootstrap_v1_handler)
    app.router.add_route("POST", "/functions/v1/realtime_boss_metric_emit", realtime_boss_metric_emit_handler)
    app.router.add_route("OPTIONS", "/functions/v1/realtime_boss_metric_emit", realtime_boss_metric_emit_handler)
    app.router.add_route("POST", "/functions/v1/admin_snapshot_create", admin_snapshot_create_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_snapshot_create", admin_snapshot_create_handler)
    app.router.add_route("POST", "/functions/v1/admin_snapshot_list", admin_snapshot_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_snapshot_list", admin_snapshot_list_handler)
    app.router.add_route("POST", "/functions/v1/admin_snapshot_restore", admin_snapshot_restore_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_snapshot_restore", admin_snapshot_restore_handler)
    app.router.add_route("POST", "/functions/v1/admin_snapshot_restore_latest_auto", admin_snapshot_restore_latest_auto_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_snapshot_restore_latest_auto", admin_snapshot_restore_latest_auto_handler)
    app.router.add_route("POST", "/functions/v1/admin_player_recovery_v1", admin_player_recovery_v1_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_player_recovery_v1", admin_player_recovery_v1_handler)
    app.router.add_route("POST", "/functions/v1/admin_player_state_events_list", admin_player_state_events_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_player_state_events_list", admin_player_state_events_list_handler)
    app.router.add_route("POST", "/functions/v1/admin_force_logout_player", admin_force_logout_player_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_force_logout_player", admin_force_logout_player_handler)
    app.router.add_route("POST", "/functions/v1/admin_audit_list", admin_audit_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_audit_list", admin_audit_list_handler)
    app.router.add_route("POST", "/functions/v1/admin_list_players", admin_list_players_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_list_players", admin_list_players_handler)
    app.router.add_route("POST", "/functions/v1/admin_player_godmode", admin_player_godmode_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_player_godmode", admin_player_godmode_handler)
    app.router.add_route("POST", "/functions/v1/admin_player_reset", admin_player_reset_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_player_reset", admin_player_reset_handler)
    app.router.add_route("POST", "/functions/v1/admin_bulk_grant", admin_bulk_grant_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_bulk_grant", admin_bulk_grant_handler)
    app.router.add_route("POST", "/functions/v1/admin_danger_token_start", admin_danger_token_start_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_danger_token_start", admin_danger_token_start_handler)
    app.router.add_route("POST", "/functions/v1/session_start", session_start_handler)
    app.router.add_route("OPTIONS", "/functions/v1/session_start", session_start_handler)
    app.router.add_route("POST", "/functions/v1/district_leaders_list", district_leaders_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/district_leaders_list", district_leaders_list_handler)
    app.router.add_route("POST", "/functions/v1/district_daily_leaders_list", district_daily_leaders_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/district_daily_leaders_list", district_daily_leaders_list_handler)
    app.router.add_route("POST", "/functions/v1/district_leader_upsert", district_leader_upsert_handler)
    app.router.add_route("OPTIONS", "/functions/v1/district_leader_upsert", district_leader_upsert_handler)
    app.router.add_route("POST", "/functions/v1/district_daily_leader_upsert", district_daily_leader_upsert_handler)
    app.router.add_route("OPTIONS", "/functions/v1/district_daily_leader_upsert", district_daily_leader_upsert_handler)
    app.router.add_route("POST", "/functions/v1/boss_last_winners_list", boss_last_winners_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/boss_last_winners_list", boss_last_winners_list_handler)
    app.router.add_route("POST", "/functions/v1/top_players_list", top_players_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/top_players_list", top_players_list_handler)
    app.router.add_route("POST", "/functions/v1/boss_fights_list", boss_fights_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/boss_fights_list", boss_fights_list_handler)
    app.router.add_route("POST", "/functions/v1/boss_fight_start", boss_fight_start_handler)
    app.router.add_route("OPTIONS", "/functions/v1/boss_fight_start", boss_fight_start_handler)
    app.router.add_route("POST", "/functions/v1/boss_help_pull", boss_help_pull_handler)
    app.router.add_route("OPTIONS", "/functions/v1/boss_help_pull", boss_help_pull_handler)
    app.router.add_route("POST", "/functions/v1/list_clans", list_clans_handler)
    app.router.add_route("OPTIONS", "/functions/v1/list_clans", list_clans_handler)
    app.router.add_route("POST", "/functions/v1/upsert_clan", upsert_clan_handler)
    app.router.add_route("OPTIONS", "/functions/v1/upsert_clan", upsert_clan_handler)
    app.router.add_route("POST", "/functions/v1/clan_apply", clan_apply_handler)
    app.router.add_route("OPTIONS", "/functions/v1/clan_apply", clan_apply_handler)
    app.router.add_route("POST", "/functions/v1/clan_accept", clan_accept_handler)
    app.router.add_route("OPTIONS", "/functions/v1/clan_accept", clan_accept_handler)
    app.router.add_route("POST", "/functions/v1/clan_reject", clan_reject_handler)
    app.router.add_route("OPTIONS", "/functions/v1/clan_reject", clan_reject_handler)
    app.router.add_route("POST", "/functions/v1/clan_cancel_apply", clan_cancel_apply_handler)
    app.router.add_route("OPTIONS", "/functions/v1/clan_cancel_apply", clan_cancel_apply_handler)
    app.router.add_route("POST", "/functions/v1/clan_leave", clan_leave_handler)
    app.router.add_route("OPTIONS", "/functions/v1/clan_leave", clan_leave_handler)
    app.router.add_route("POST", "/functions/v1/boss_fight_get", boss_fight_get_handler)
    app.router.add_route("OPTIONS", "/functions/v1/boss_fight_get", boss_fight_get_handler)
    app.router.add_route("POST", "/functions/v1/boss_fight_hit", boss_fight_hit_handler)
    app.router.add_route("OPTIONS", "/functions/v1/boss_fight_hit", boss_fight_hit_handler)
    app.router.add_route("POST", "/functions/v1/boss_hit_and_save_fast", boss_hit_and_save_fast_handler)
    app.router.add_route("OPTIONS", "/functions/v1/boss_hit_and_save_fast", boss_hit_and_save_fast_handler)
    app.router.add_route("POST", "/functions/v1/boss_fight_claim", boss_fight_claim_handler)
    app.router.add_route("OPTIONS", "/functions/v1/boss_fight_claim", boss_fight_claim_handler)
    app.router.add_route("POST", "/functions/v1/find_arena_opponent", find_arena_opponent_handler)
    app.router.add_route("OPTIONS", "/functions/v1/find_arena_opponent", find_arena_opponent_handler)
    app.router.add_route("POST", "/functions/v1/stars_create_invoice", stars_create_invoice_handler)
    app.router.add_route("OPTIONS", "/functions/v1/stars_create_invoice", stars_create_invoice_handler)
    app.router.add_route("POST", "/functions/v1/get_players_by_names", get_players_by_names_handler)
    app.router.add_route("OPTIONS", "/functions/v1/get_players_by_names", get_players_by_names_handler)
    app.router.add_route("POST", "/functions/v1/promo_redeem", promo_redeem_handler)
    app.router.add_route("OPTIONS", "/functions/v1/promo_redeem", promo_redeem_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_upsert", admin_promo_upsert_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_upsert", admin_promo_upsert_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_list", admin_promo_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_list", admin_promo_list_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_targets_set", admin_promo_targets_set_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_targets_set", admin_promo_targets_set_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_targets_get", admin_promo_targets_get_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_targets_get", admin_promo_targets_get_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_auto_rule_upsert", admin_promo_auto_rule_upsert_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_auto_rule_upsert", admin_promo_auto_rule_upsert_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_auto_rule_list", admin_promo_auto_rule_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_auto_rule_list", admin_promo_auto_rule_list_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_auto_rule_delete", admin_promo_auto_rule_delete_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_auto_rule_delete", admin_promo_auto_rule_delete_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_report", admin_promo_report_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_report", admin_promo_report_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_holiday_run", admin_promo_holiday_run_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_holiday_run", admin_promo_holiday_run_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_holiday_run_filters", admin_promo_holiday_run_filters_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_holiday_run_filters", admin_promo_holiday_run_filters_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_campaign_upsert", admin_promo_campaign_upsert_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_campaign_upsert", admin_promo_campaign_upsert_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_campaign_list", admin_promo_campaign_list_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_campaign_list", admin_promo_campaign_list_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_campaign_delete", admin_promo_campaign_delete_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_campaign_delete", admin_promo_campaign_delete_handler)
    app.router.add_route("POST", "/functions/v1/admin_promo_campaign_tick", admin_promo_campaign_tick_handler)
    app.router.add_route("OPTIONS", "/functions/v1/admin_promo_campaign_tick", admin_promo_campaign_tick_handler)
    app.router.add_route("POST", "/functions/v1/boss_help_send", boss_help_send_handler)
    app.router.add_route("OPTIONS", "/functions/v1/boss_help_send", boss_help_send_handler)
    app.router.add_route("GET", "/ws", ws_handler)
    return app


if __name__ == "__main__":
    app = build_app()
    web.run_app(app, host=BACKEND_HOST, port=BACKEND_PORT)
