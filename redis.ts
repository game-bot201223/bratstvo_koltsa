// Redis client for Deno — lightweight, no external deps
// Uses raw Redis protocol over TCP (RESP)
// Each call creates a fresh connection to avoid stale socket hangs

const REDIS_HOST = "127.0.0.1";
const REDIS_PORT = 6379;
const REDIS_PASS = Deno.env.get("REDIS_PASS") || "BrKo1tsaR3d1s2024!";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const REDIS_TIMEOUT_MS = 2000;

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error("redis_timeout")), ms);
    p.then(v => { clearTimeout(t); resolve(v); }).catch(e => { clearTimeout(t); reject(e); });
  });
}

async function redisCall(...args: (string | number)[]): Promise<string> {
  let conn: Deno.TcpConn | null = null;
  try {
    conn = await withTimeout(Deno.connect({ hostname: REDIS_HOST, port: REDIS_PORT }), REDIS_TIMEOUT_MS);
    // AUTH
    if (REDIS_PASS) {
      const authParts = ["AUTH", REDIS_PASS];
      let authReq = "*" + authParts.length + "\r\n";
      for (const p of authParts) authReq += "$" + encoder.encode(p).length + "\r\n" + p + "\r\n";
      await conn.write(encoder.encode(authReq));
      const buf0 = new Uint8Array(512);
      await withTimeout(readFromConn(conn, buf0), REDIS_TIMEOUT_MS);
    }
    // Command
    const parts = args.map(a => String(a));
    let req = "*" + parts.length + "\r\n";
    for (const p of parts) req += "$" + encoder.encode(p).length + "\r\n" + p + "\r\n";
    await conn.write(encoder.encode(req));
    const buf = new Uint8Array(65536);
    const n = await withTimeout(readFromConn(conn, buf), REDIS_TIMEOUT_MS);
    if (n === 0) throw new Error("redis: connection closed");
    return decoder.decode(buf.subarray(0, n));
  } finally {
    try { conn?.close(); } catch (_) {}
  }
}

async function readFromConn(conn: Deno.TcpConn, buf: Uint8Array): Promise<number> {
  const n = await conn.read(buf);
  return n ?? 0;
}

function parseResp(raw: string): string | null {
  if (!raw) return null;
  const first = raw.charAt(0);
  if (first === "+") return raw.slice(1).split("\r\n")[0];
  if (first === "-") return null;
  if (first === ":") return raw.slice(1).split("\r\n")[0];
  if (first === "$") {
    if (raw.startsWith("$-1")) return null;
    const idx = raw.indexOf("\r\n");
    if (idx < 0) return null;
    const len = parseInt(raw.slice(1, idx), 10);
    if (isNaN(len) || len < 0) return null;
    return raw.slice(idx + 2, idx + 2 + len);
  }
  if (first === "*") return raw;
  return raw;
}

// ─── Exported helpers ───

export async function redisSet(key: string, value: string, ttlMs?: number): Promise<void> {
  if (ttlMs && ttlMs > 0) {
    await redisCall("SET", key, value, "PX", ttlMs);
  } else {
    await redisCall("SET", key, value);
  }
}

export async function redisGet(key: string): Promise<string | null> {
  const raw = await redisCall("GET", key);
  return parseResp(raw);
}

export async function redisDel(key: string): Promise<void> {
  await redisCall("DEL", key);
}

export async function redisHset(key: string, field: string, value: string): Promise<void> {
  await redisCall("HSET", key, field, value);
}

export async function redisHget(key: string, field: string): Promise<string | null> {
  const raw = await redisCall("HGET", key, field);
  return parseResp(raw);
}

export async function redisHgetall(key: string): Promise<Record<string, string>> {
  const raw = await redisCall("HGETALL", key);
  const result: Record<string, string> = {};
  if (!raw || !raw.startsWith("*")) return result;
  const lines = raw.split("\r\n");
  let i = 1;
  while (i < lines.length) {
    if (lines[i] && lines[i].startsWith("$")) {
      const kLen = parseInt(lines[i].slice(1), 10);
      const k = lines[i + 1] || "";
      if (lines[i + 2] && lines[i + 2].startsWith("$")) {
        const v = lines[i + 3] || "";
        result[k] = v;
        i += 4;
      } else {
        i += 2;
      }
    } else {
      i++;
    }
  }
  return result;
}

export async function redisPublish(channel: string, message: string): Promise<void> {
  await redisCall("PUBLISH", channel, message);
}

// ─── Game-specific helpers ───

export async function redisBossSetHp(
  ownerTgId: string, bossId: number, hp: number, maxHp: number, expiresAt: string
): Promise<void> {
  const key = `boss:${ownerTgId}:${bossId}`;
  await redisCall("HSET", key, "hp", String(hp), "max_hp", String(maxHp), "expires_at", expiresAt);
  await redisCall("EXPIRE", key, "28800");
}

export async function redisBossGetHp(
  ownerTgId: string, bossId: number
): Promise<{ hp: number; max_hp: number; expires_at: string } | null> {
  const key = `boss:${ownerTgId}:${bossId}`;
  const data = await redisHgetall(key);
  if (!data || !data.hp) return null;
  return {
    hp: parseInt(data.hp, 10) || 0,
    max_hp: parseInt(data.max_hp, 10) || 0,
    expires_at: data.expires_at || "",
  };
}

export async function redisRateLimit(key: string, windowMs: number): Promise<boolean> {
  const rlKey = `rl:${key}`;
  const raw = await redisCall("SET", rlKey, "1", "PX", windowMs, "NX");
  const parsed = parseResp(raw);
  return parsed === "OK";
}

export async function redisDmgLogPush(
  ownerTgId: string, bossId: number, who: string, dmg: number
): Promise<void> {
  const key = `dmglog:${ownerTgId}:${bossId}`;
  const entry = JSON.stringify({ who, dmg, ts: Date.now() });
  await redisCall("LPUSH", key, entry);
  await redisCall("LTRIM", key, "0", "99");
  await redisCall("EXPIRE", key, "28800");
}

export async function redisDmgLogGet(
  ownerTgId: string, bossId: number
): Promise<Array<{ who: string; dmg: number; ts: number }>> {
  const key = `dmglog:${ownerTgId}:${bossId}`;
  const raw = await redisCall("LRANGE", key, "0", "99");
  if (!raw || !raw.startsWith("*")) return [];
  const entries: Array<{ who: string; dmg: number; ts: number }> = [];
  const lines = raw.split("\r\n");
  let i = 1;
  while (i < lines.length) {
    if (lines[i] && lines[i].startsWith("$")) {
      const val = lines[i + 1] || "";
      try { entries.push(JSON.parse(val)); } catch (_) {}
      i += 2;
    } else {
      i++;
    }
  }
  return entries;
}

export async function redisTopDmgAdd(
  ownerTgId: string, bossId: number, who: string, dmg: number
): Promise<void> {
  const key = `topdmg:${ownerTgId}:${bossId}`;
  await redisCall("ZINCRBY", key, String(dmg), who);
  await redisCall("EXPIRE", key, "28800");
}

export async function redisTopDmgGet(
  ownerTgId: string, bossId: number
): Promise<Array<{ who: string; dmg: number }>> {
  const key = `topdmg:${ownerTgId}:${bossId}`;
  const raw = await redisCall("ZREVRANGE", key, "0", "49", "WITHSCORES");
  if (!raw || !raw.startsWith("*")) return [];
  const result: Array<{ who: string; dmg: number }> = [];
  const lines = raw.split("\r\n");
  let i = 1;
  while (i < lines.length) {
    if (lines[i] && lines[i].startsWith("$")) {
      const who = lines[i + 1] || "";
      if (lines[i + 2] && lines[i + 2].startsWith("$")) {
        const dmg = parseInt(lines[i + 3] || "0", 10) || 0;
        result.push({ who, dmg });
        i += 4;
      } else {
        i += 2;
      }
    } else {
      i++;
    }
  }
  return result;
}

export async function redisSetOnline(tgId: string, ttlSec = 120): Promise<void> {
  await redisCall("SET", `online:${tgId}`, "1", "EX", ttlSec);
}

export async function redisRemoveOnline(tgId: string): Promise<void> {
  await redisCall("DEL", `online:${tgId}`);
}

// Aliases for boss_fight_hit.ts compatibility
export const redisBossDmgLogPush = redisDmgLogPush;
export const redisBossTopAdd = redisTopDmgAdd;
