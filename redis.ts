// Redis client for Deno — lightweight, no external deps
// Uses raw Redis protocol over TCP (RESP)

const REDIS_HOST = "127.0.0.1";
const REDIS_PORT = 6379;
const REDIS_PASS = Deno.env.get("REDIS_PASS") || "BrKo1tsaR3d1s2024!";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

class RedisConn {
  private conn: Deno.TcpConn | null = null;
  private authed = false;

  async connect(): Promise<void> {
    if (this.conn) return;
    this.conn = await Deno.connect({ hostname: REDIS_HOST, port: REDIS_PORT });
    if (REDIS_PASS) {
      await this.rawCommand("AUTH", REDIS_PASS);
      this.authed = true;
    }
  }

  async rawCommand(...args: (string | number)[]): Promise<string> {
    if (!this.conn) await this.connect();
    const parts = args.map(a => String(a));
    let req = "*" + parts.length + "\r\n";
    for (const p of parts) {
      req += "$" + encoder.encode(p).length + "\r\n" + p + "\r\n";
    }
    await this.conn!.write(encoder.encode(req));
    // Read response — may need multiple reads for large responses
    let result = "";
    const buf = new Uint8Array(65536);
    const n = await this.conn!.read(buf);
    if (n === null) { this.conn = null; throw new Error("redis: connection closed"); }
    result = decoder.decode(buf.subarray(0, n));
    return result;
  }

  async close(): Promise<void> {
    try { this.conn?.close(); } catch (_) {}
    this.conn = null;
    this.authed = false;
  }
}

// Parse simple RESP responses
function parseResp(raw: string): string | null {
  if (!raw) return null;
  const first = raw.charAt(0);
  if (first === "+") return raw.slice(1).split("\r\n")[0];
  if (first === "-") throw new Error("redis: " + raw.slice(1).split("\r\n")[0]);
  if (first === ":") return raw.slice(1).split("\r\n")[0];
  if (first === "$") {
    const lines = raw.split("\r\n");
    const len = parseInt(lines[0].slice(1), 10);
    if (len < 0) return null;
    return lines[1] || null;
  }
  return raw;
}

// Parse RESP array into string[]
function parseRespArray(raw: string): string[] {
  const lines = raw.split("\r\n");
  if (!lines[0].startsWith("*")) return [];
  const count = parseInt(lines[0].slice(1), 10);
  if (count <= 0) return [];
  const result: string[] = [];
  let idx = 1;
  for (let i = 0; i < count; i++) {
    if (idx >= lines.length) break;
    const hdr = lines[idx];
    idx++;
    if (hdr && hdr.startsWith("$")) {
      const len = parseInt(hdr.slice(1), 10);
      if (len < 0) { result.push(""); continue; }
      result.push(lines[idx] || "");
      idx++;
    } else if (hdr && hdr.startsWith(":")) {
      result.push(hdr.slice(1));
    } else if (hdr && hdr.startsWith("+")) {
      result.push(hdr.slice(1));
    } else {
      result.push("");
    }
  }
  return result;
}

// Singleton connection with auto-reconnect
let _conn: RedisConn | null = null;

async function getConn(): Promise<RedisConn> {
  if (!_conn) _conn = new RedisConn();
  try {
    await _conn.connect();
  } catch (_e) {
    _conn = new RedisConn();
    await _conn.connect();
  }
  return _conn;
}

// === Public API ===

export async function redisGet(key: string): Promise<string | null> {
  const c = await getConn();
  const raw = await c.rawCommand("GET", key);
  return parseResp(raw);
}

export async function redisSet(key: string, value: string, ttlSec?: number): Promise<void> {
  const c = await getConn();
  if (ttlSec && ttlSec > 0) {
    await c.rawCommand("SET", key, value, "EX", ttlSec);
  } else {
    await c.rawCommand("SET", key, value);
  }
}

export async function redisDel(key: string): Promise<void> {
  const c = await getConn();
  await c.rawCommand("DEL", key);
}

export async function redisIncr(key: string): Promise<number> {
  const c = await getConn();
  const raw = await c.rawCommand("INCR", key);
  return parseInt(parseResp(raw) || "0", 10);
}

export async function redisExpire(key: string, ttlSec: number): Promise<void> {
  const c = await getConn();
  await c.rawCommand("EXPIRE", key, ttlSec);
}

export async function redisHSet(key: string, ...fieldValues: string[]): Promise<void> {
  const c = await getConn();
  await c.rawCommand("HSET", key, ...fieldValues);
}

export async function redisHGet(key: string, field: string): Promise<string | null> {
  const c = await getConn();
  const raw = await c.rawCommand("HGET", key, field);
  return parseResp(raw);
}

export async function redisHGetAll(key: string): Promise<Record<string, string>> {
  const c = await getConn();
  const raw = await c.rawCommand("HGETALL", key);
  const arr = parseRespArray(raw);
  const result: Record<string, string> = {};
  for (let i = 0; i < arr.length - 1; i += 2) {
    result[arr[i]] = arr[i + 1];
  }
  return result;
}

export async function redisPublish(channel: string, message: string): Promise<number> {
  const c = await getConn();
  const raw = await c.rawCommand("PUBLISH", channel, message);
  return parseInt(parseResp(raw) || "0", 10);
}

// === Game-specific helpers ===

// Boss fight real-time HP in Redis (fast reads, no DB round-trip)
export async function redisBossSetHp(
  ownerTgId: string, bossId: number, hp: number, maxHp: number, expiresAt: string
): Promise<void> {
  const key = "boss:" + ownerTgId + ":" + bossId;
  await redisHSet(key, "hp", String(hp), "max_hp", String(maxHp), "expires_at", expiresAt, "ts", String(Date.now()));
  await redisExpire(key, 30000);
}

export async function redisBossGetHp(
  ownerTgId: string, bossId: number
): Promise<{ hp: number; max_hp: number; expires_at: string } | null> {
  const key = "boss:" + ownerTgId + ":" + bossId;
  const data = await redisHGetAll(key);
  if (!data || !data.hp) return null;
  return {
    hp: parseInt(data.hp || "0", 10),
    max_hp: parseInt(data.max_hp || "0", 10),
    expires_at: data.expires_at || "",
  };
}

// Rate limiting via Redis (replaces PostgREST RPC rate_limit_allow)
export async function redisRateLimit(key: string, windowMs: number): Promise<boolean> {
  const rlKey = "rl:" + key;
  const c = await getConn();
  const raw = await c.rawCommand("SET", rlKey, "1", "PX", windowMs, "NX");
  const resp = parseResp(raw);
  return resp === "OK";
}

// Online players tracking
export async function redisSetOnline(tgId: string, ttlSec = 120): Promise<void> {
  await redisSet("online:" + tgId, "1", ttlSec);
}

export async function redisRemoveOnline(tgId: string): Promise<void> {
  await redisDel("online:" + tgId);
}

// Damage log — append to list with TTL
export async function redisBossDmgLogPush(
  ownerTgId: string, bossId: number, who: string, dmg: number
): Promise<void> {
  const key = "bossdmg:" + ownerTgId + ":" + bossId;
  const entry = JSON.stringify({ who, dmg, ts: Date.now() });
  const c = await getConn();
  await c.rawCommand("LPUSH", key, entry);
  await c.rawCommand("LTRIM", key, "0", "199"); // keep last 200
  await c.rawCommand("EXPIRE", key, "30000");
}

export async function redisBossDmgLogGet(
  ownerTgId: string, bossId: number, limit = 50
): Promise<Array<{ who: string; dmg: number; ts: number }>> {
  const key = "bossdmg:" + ownerTgId + ":" + bossId;
  const c = await getConn();
  const raw = await c.rawCommand("LRANGE", key, "0", String(limit - 1));
  const arr = parseRespArray(raw);
  return arr.map(s => {
    try { return JSON.parse(s); } catch (_) { return { who: "?", dmg: 0, ts: 0 }; }
  });
}

// Top damage — sorted set
export async function redisBossTopAdd(
  ownerTgId: string, bossId: number, who: string, dmg: number
): Promise<void> {
  const key = "bosstop:" + ownerTgId + ":" + bossId;
  const c = await getConn();
  await c.rawCommand("ZINCRBY", key, String(dmg), who);
  await c.rawCommand("EXPIRE", key, "30000");
}

export async function redisBossTopGet(
  ownerTgId: string, bossId: number, limit = 20
): Promise<Array<{ who: string; dmg: number }>> {
  const key = "bosstop:" + ownerTgId + ":" + bossId;
  const c = await getConn();
  const raw = await c.rawCommand("ZREVRANGE", key, "0", String(limit - 1), "WITHSCORES");
  const arr = parseRespArray(raw);
  const result: Array<{ who: string; dmg: number }> = [];
  for (let i = 0; i < arr.length - 1; i += 2) {
    result.push({ who: arr[i], dmg: parseInt(arr[i + 1] || "0", 10) });
  }
  return result;
}
