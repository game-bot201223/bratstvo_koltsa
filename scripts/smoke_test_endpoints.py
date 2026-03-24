#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import subprocess
import sys
import time
import urllib.parse


BASE = "https://bratstvokoltsa.com"
def build_init_data(user_id: int, first_name: str):
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    user_obj = {"id": int(user_id), "first_name": str(first_name)}
    user_json = json.dumps(user_obj, separators=(",", ":"), ensure_ascii=False)
    auth_date = str(int(time.time()))
    if not token:
        return "user=" + urllib.parse.quote(user_json, safe="")
    check_pairs = [("auth_date", auth_date), ("user", user_json)]
    data_check_string = "\n".join("{0}={1}".format(k, v) for k, v in sorted(check_pairs, key=lambda x: x[0]))
    secret_key = hmac.new(b"WebAppData", token.encode("utf-8"), hashlib.sha256).digest()
    calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    parts = [
        "auth_date=" + urllib.parse.quote(auth_date, safe=""),
        "user=" + urllib.parse.quote(user_json, safe=""),
        "hash=" + urllib.parse.quote(calc_hash, safe=""),
    ]
    return "&".join(parts)


ALLOWED_INIT_DATA = build_init_data(8794843839, "Admin")
BLOCKED_INIT_DATA = build_init_data(123456789, "Blocked")


def run_curl(method: str, path: str, payload):
    cmd = [
        "curl",
        "-sS",
        "-o",
        "/tmp/smoke_body.json",
        "-w",
        "%{http_code}",
        "-X",
        method,
        BASE + path,
    ]
    if payload is not None:
        cmd += ["-H", "Content-Type: application/json", "--data-raw", json.dumps(payload, separators=(",", ":"))]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    code = 0
    if proc.returncode == 0:
        raw = (proc.stdout or "").strip()
        if raw.isdigit():
            code = int(raw)
    body = ""
    try:
        with open("/tmp/smoke_body.json", "r", encoding="utf-8", errors="replace") as f:
            body = f.read().strip().replace("\n", " ")[:220]
    except Exception:
        pass
    return code, body


def main():
    tests = [
        ("GET", "/health", None, 200, "health"),
        ("POST", "/functions/v1/get_player", {"initData": ALLOWED_INIT_DATA}, 200, "get_player allowed"),
        ("POST", "/functions/v1/get_player", {"initData": BLOCKED_INIT_DATA}, 403, "get_player blocked"),
        (
            "POST",
            "/functions/v1/upsert_player",
            {"initData": ALLOWED_INIT_DATA, "player": {"state": {"hp": 100}, "xp": 1, "gold": 2, "silver": 3, "tooth": 0}},
            200,
            "upsert_player allowed",
        ),
        (
            "POST",
            "/functions/v1/upsert_player",
            {"initData": BLOCKED_INIT_DATA, "player": {"state": {"hp": 1}}},
            403,
            "upsert_player blocked",
        ),
        ("POST", "/functions/v1/session_start", {"initData": ALLOWED_INIT_DATA, "active_session_id": "smoke-session"}, 200, "session_start allowed"),
        ("POST", "/functions/v1/session_start", {"initData": BLOCKED_INIT_DATA, "active_session_id": "smoke-session"}, 403, "session_start blocked"),
        ("POST", "/functions/v1/district_leaders_list", {"initData": ALLOWED_INIT_DATA}, 200, "district_leaders_list"),
        ("POST", "/functions/v1/district_daily_leaders_list", {"initData": ALLOWED_INIT_DATA}, 200, "district_daily_leaders_list"),
        ("POST", "/functions/v1/boss_last_winners_list", {"initData": ALLOWED_INIT_DATA}, 200, "boss_last_winners_list"),
        ("POST", "/functions/v1/top_players_list", {"initData": ALLOWED_INIT_DATA}, 200, "top_players_list"),
        ("POST", "/functions/v1/boss_fights_list", {"initData": ALLOWED_INIT_DATA}, 200, "boss_fights_list"),
        ("POST", "/functions/v1/boss_fight_start", {"initData": ALLOWED_INIT_DATA, "boss_id": 1, "max_hp": 1500}, 200, "boss_fight_start"),
        ("POST", "/functions/v1/boss_fight_get", {"initData": ALLOWED_INIT_DATA, "boss_id": 1}, 200, "boss_fight_get"),
        ("POST", "/functions/v1/boss_fight_hit", {"initData": ALLOWED_INIT_DATA, "boss_id": 1, "dmg": 7}, 200, "boss_fight_hit"),
        ("POST", "/functions/v1/boss_fight_claim", {"initData": ALLOWED_INIT_DATA, "boss_id": 1}, 200, "boss_fight_claim"),
        ("POST", "/functions/v1/boss_help_pull", {"initData": ALLOWED_INIT_DATA, "since_id": 0}, 200, "boss_help_pull"),
        ("POST", "/functions/v1/list_clans", {"initData": ALLOWED_INIT_DATA, "limit": 50}, 200, "list_clans"),
        ("POST", "/functions/v1/upsert_clan", {"initData": ALLOWED_INIT_DATA, "id": "CLN100", "name": "SmokeClan", "data": {"id": "CLN100", "name": "SmokeClan", "members": ["ADMIN"]}}, 200, "upsert_clan"),
        ("POST", "/functions/v1/find_arena_opponent", {"initData": ALLOWED_INIT_DATA, "min_sum": 1, "max_sum": 999999, "min_power": 0, "max_power": 0}, 200, "find_arena_opponent"),
        ("POST", "/functions/v1/get_players_by_names", {"initData": ALLOWED_INIT_DATA, "names": ["Admin", "SmokeClan"]}, 200, "get_players_by_names"),
        ("POST", "/functions/v1/boss_help_send", {"initData": ALLOWED_INIT_DATA, "boss_id": 1, "dmg": 5, "clan_id": "CLN100", "from_name": "Admin"}, 200, "boss_help_send"),
        ("POST", "/functions/v1/stars_create_invoice", {"warm": True}, 200, "stars_create_invoice warm"),
        ("POST", "/functions/v1/stars_create_invoice", {"initData": ALLOWED_INIT_DATA, "productId": "gold_small"}, 200, "stars_create_invoice"),
    ]

    all_ok = True
    for method, path, payload, expected, label in tests:
        code, body = run_curl(method, path, payload)
        ok = code == expected
        if not ok:
            all_ok = False
        tag = "OK" if ok else "FAIL"
        print("[{0}] {1}: code={2} expected={3} body={4}".format(tag, label, code, expected, body))

    print("SMOKE_RESULT={0}".format("PASS" if all_ok else "FAIL"))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
