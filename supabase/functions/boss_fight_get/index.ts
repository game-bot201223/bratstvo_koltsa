import "@supabase/functions-js/edge-runtime.d.ts"

const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-telegram-init-data",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
}

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false
  let out = 0
  for (let i = 0; i < a.length; i++) out |= a.charCodeAt(i) ^ b.charCodeAt(i)
  return out === 0
}

async function hmacSha256Bytes(keyStr: string, data: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(keyStr),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  )
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data))
  return new Uint8Array(sig)
}

async function hmacSha256Hex(keyBytes: Uint8Array, data: string): Promise<string> {
  const keyCopy = new Uint8Array(keyBytes)
  const rawKey = keyCopy.buffer
  const key = await crypto.subtle.importKey("raw", rawKey, { name: "HMAC", hash: "SHA-256" }, false, ["sign"])
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data))
  return Array.from(new Uint8Array(sig)).map((b) => b.toString(16).padStart(2, "0")).join("")
}

function parseInitData(initData: string): Map<string, string> {
  const params = new URLSearchParams(initData)
  const out = new Map<string, string>()
  for (const [k, v] of params.entries()) out.set(k, v)
  return out
}

function parseInitDataRaw(initData: string): Map<string, string> {
  const out = new Map<string, string>()
  try {
    const parts = String(initData || "").split("&")
    for (const p of parts) {
      if (!p) continue
      const idx = p.indexOf("=")
      if (idx <= 0) continue
      const k = p.slice(0, idx)
      const v = p.slice(idx + 1)
      out.set(k, v)
    }
  } catch (_e) {
    // ignore
  }
  return out
}

function buildDataCheckString(map: Map<string, string>): string {
  const pairs: string[] = []
  for (const [k, v] of map.entries()) {
    if (k === "hash") continue
    pairs.push(`${k}=${v}`)
  }
  pairs.sort((a, b) => a.localeCompare(b))
  return pairs.join("\n")
}

async function verifyTelegramInitData(initData: string, botToken: string): Promise<{ ok: boolean; user?: any }>{
  if (!initData || !botToken) return { ok: false }
  const secretKey = await hmacSha256Bytes("WebAppData", botToken)

  const variants: Array<{ map: Map<string, string>; userJsonNeedsDecode: boolean }> = []
  try {
    variants.push({ map: parseInitData(initData), userJsonNeedsDecode: false })
  } catch (_e) {}
  try {
    variants.push({ map: parseInitData(String(initData).replace(/\+/g, "%2B")), userJsonNeedsDecode: false })
  } catch (_e) {}
  try {
    variants.push({ map: parseInitData(String(initData).replace(/\+/g, "%20")), userJsonNeedsDecode: false })
  } catch (_e) {}
  try {
    variants.push({ map: parseInitDataRaw(initData), userJsonNeedsDecode: true })
  } catch (_e) {}

  for (const v of variants) {
    const map = v.map
    const hash = map.get("hash") || ""
    if (!hash) continue
    const dataCheckString = buildDataCheckString(map)
    const computed = await hmacSha256Hex(secretKey, dataCheckString)
    if (!timingSafeEqual(computed, String(hash).toLowerCase())) continue

    let user: any = undefined
    try {
      const u = map.get("user")
      if (u) user = JSON.parse(v.userJsonNeedsDecode ? decodeURIComponent(u) : u)
    } catch (_) {
      user = undefined
    }
    return { ok: true, user }
  }

  return { ok: false }
}

function safeInt(v: unknown, def = 0): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return def
  return Math.floor(n)
}

function bossDef(bossId: number): { boss_id: number; max_hp: number; reward: { xp: number; tooth: number; gold: number } } | null {
  const defs: Record<number, { max_hp: number; reward: { xp: number; tooth: number; gold: number } }> = {
    1: { max_hp: 1000, reward: { xp: 50, tooth: 100, gold: 0 } },
    2: { max_hp: 10000, reward: { xp: 100, tooth: 300, gold: 0 } },
    3: { max_hp: 50000, reward: { xp: 200, tooth: 500, gold: 0 } },
    4: { max_hp: 200000, reward: { xp: 500, tooth: 800, gold: 0 } },
    5: { max_hp: 1000000, reward: { xp: 1000, tooth: 1200, gold: 0 } },
    6: { max_hp: 2000000, reward: { xp: 2000, tooth: 2000, gold: 0 } },
    7: { max_hp: 5000000, reward: { xp: 5000, tooth: 5000, gold: 0 } },
    8: { max_hp: 10000000, reward: { xp: 10000, tooth: 10000, gold: 0 } },
    9: { max_hp: 20000000, reward: { xp: 20000, tooth: 20000, gold: 0 } },
    10: { max_hp: 50000000, reward: { xp: 50000, tooth: 50000, gold: 0 } },
    11: { max_hp: 100000000, reward: { xp: 100000, tooth: 100000, gold: 0 } },
    12: { max_hp: 200000000, reward: { xp: 200000, tooth: 200000, gold: 0 } },
  }
  const d = defs[bossId]
  if (!d) return null
  return { boss_id: bossId, max_hp: d.max_hp, reward: d.reward }
}

async function postgrestGetFight(projectUrl: string, serviceKey: string, ownerTgId: string, bossId: number): Promise<any | null> {
  const url = projectUrl.replace(/\/$/, "") +
    `/rest/v1/player_boss_fights?owner_tg_id=eq.${encodeURIComponent(ownerTgId)}&boss_id=eq.${encodeURIComponent(String(bossId))}&select=owner_tg_id,boss_id,hp,max_hp,expires_at,reward_claimed,updated_at&limit=1`
  const resp = await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
  })
  if (!resp.ok) return null
  const rows = await resp.json().catch(() => [])
  const row = Array.isArray(rows) && rows.length ? rows[0] : null
  return row && typeof row === "object" ? row : null
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders })
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ ok: false, error: "method_not_allowed" }), {
      status: 405,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const botToken = String(Deno.env.get("TELEGRAM_BOT_TOKEN") || "").trim()
  const projectUrl = String(Deno.env.get("PROJECT_URL") || "").trim()
  const serviceKey = String(Deno.env.get("SERVICE_ROLE_KEY") || "").trim()
  if (!botToken || !projectUrl || !serviceKey) {
    return new Response(JSON.stringify({ ok: false, error: "missing_secrets" }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  let body: any = {}
  try {
    body = await req.json()
  } catch (_) {
    body = {}
  }

  const initData = String(body?.initData || req.headers.get("x-telegram-init-data") || "").trim()
  if (!initData) {
    return new Response(JSON.stringify({ ok: false, error: "missing_init_data" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const verified = await verifyTelegramInitData(initData, botToken)
  if (!verified.ok || !verified.user?.id) {
    return new Response(JSON.stringify({ ok: false, error: "unauthorized", reason: "bad_init_data" }), {
      status: 401,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const ownerTgId = String(verified.user.id)
  const bossId = Math.max(1, safeInt(body?.boss_id ?? body?.bossId, 0))
  const def = bossDef(bossId)
  if (!def) {
    return new Response(JSON.stringify({ ok: false, error: "bad_boss_id" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const fight = await postgrestGetFight(projectUrl, serviceKey, ownerTgId, bossId)
  if (!fight) {
    return new Response(JSON.stringify({ ok: true, fight: null, boss: def }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  return new Response(JSON.stringify({ ok: true, fight, boss: def }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})
