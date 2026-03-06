import "jsr:@supabase/functions-js/edge-runtime.d.ts"

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
  } catch (_e) {}
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

function decodeUriMaybe(s: string): string {
  try {
    if (!s) return s
    if (!/%[0-9A-Fa-f]{2}/.test(s)) return s
    return decodeURIComponent(s)
  } catch (_e) {
    return s
  }
}

async function verifyTelegramInitData(initData: string, botToken: string): Promise<{ ok: boolean; user?: any }> {
  if (!initData || !botToken) return { ok: false }
  const secretKey = await hmacSha256Bytes("WebAppData", botToken)
  const initBase = String(initData || "")
  const initPlusLit = initBase.replace(/\+/g, "%2B")
  const initPlusSpace = initBase.replace(/\+/g, "%20")
  const initDecoded = decodeUriMaybe(initBase)
  const initDecodedPlusLit = decodeUriMaybe(initPlusLit)
  const initDecodedPlusSpace = decodeUriMaybe(initPlusSpace)
  const candidates = [initBase, initPlusLit, initPlusSpace, initDecoded, initDecodedPlusLit, initDecodedPlusSpace]
  const variants: Array<{ map: Map<string, string>; userJsonNeedsDecode: boolean }> = []
  for (const c of candidates) {
    try { variants.push({ map: parseInitData(c), userJsonNeedsDecode: false }) } catch (_e) {}
    try { variants.push({ map: parseInitDataRaw(c), userJsonNeedsDecode: true }) } catch (_e) {}
  }
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

function getTodayUtcStartIso(): string {
  const d = new Date()
  const y = d.getUTCFullYear()
  const m = d.getUTCMonth()
  const day = d.getUTCDate()
  return new Date(Date.UTC(y, m, day, 0, 0, 0, 0)).toISOString()
}

const HAVCHIK_CLAIM_DAILY_CAP = 200

async function postgrestSumClaimedEnergyToday(projectUrl: string, serviceKey: string, toTgId: string): Promise<number> {
  const todayStart = getTodayUtcStartIso()
  const url = projectUrl.replace(/\/$/, "") +
    `/rest/v1/havchik_inbox?to_tg_id=eq.${encodeURIComponent(toTgId)}` +
    `&claimed=eq.true` +
    `&claimed_at=gte.${encodeURIComponent(todayStart)}` +
    `&select=energy`
  const resp = await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
  })
  if (!resp.ok) return 0
  const rows = await resp.json().catch(() => [])
  if (!Array.isArray(rows)) return 0
  let sum = 0
  for (const r of rows) {
    sum += safeInt((r as any)?.energy, 0)
  }
  return sum
}

async function postgrestClaimHavchik(
  projectUrl: string,
  serviceKey: string,
  toTgId: string,
  id: number,
): Promise<{ row: any | null; err?: string }> {
  try {
    const getUrl = projectUrl.replace(/\/$/, "") +
      `/rest/v1/havchik_inbox?id=eq.${encodeURIComponent(String(id))}` +
      `&to_tg_id=eq.${encodeURIComponent(toTgId)}` +
      `&claimed=eq.false` +
      `&select=id,from_name,type_id,energy`
    const getResp = await fetch(getUrl, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!getResp.ok) return { row: null, err: "fetch_failed" }
    const rows = await getResp.json().catch(() => [])
    const row = Array.isArray(rows) && rows.length ? rows[0] : null
    if (!row || typeof row !== "object") return { row: null, err: "not_found" }

    const energy = safeInt((row as any).energy, 0)
    const sumToday = await postgrestSumClaimedEnergyToday(projectUrl, serviceKey, toTgId)
    if (sumToday + energy > HAVCHIK_CLAIM_DAILY_CAP) {
      return { row: null, err: "daily_cap" }
    }

    const patchUrl = projectUrl.replace(/\/$/, "") +
      `/rest/v1/havchik_inbox?id=eq.${encodeURIComponent(String(id))}` +
      `&to_tg_id=eq.${encodeURIComponent(toTgId)}`
    const patchResp = await fetch(patchUrl, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        Prefer: "return=minimal",
      },
      body: JSON.stringify({ claimed: true, claimed_at: new Date().toISOString() }),
    })
    if (!patchResp.ok) return { row: null, err: "patch_failed" }
    return { row }
  } catch (_e) {
    return { row: null, err: "exception" }
  }
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

  const toTgId = String(verified.user.id)
  const id = safeInt(body?.id ?? body?.inbox_id, 0)
  if (id <= 0) {
    return new Response(JSON.stringify({ ok: false, error: "bad_id" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const { row, err } = await postgrestClaimHavchik(projectUrl, serviceKey, toTgId, id)
  if (!row) {
    const status = err === "daily_cap" ? 429 : 404
    const msg = err === "daily_cap"
      ? "Лимит 200 ⚡ в сутки (сброс в 00:00 UTC)"
      : (err || "not_found")
    return new Response(JSON.stringify({ ok: false, error: err || "not_found", reason: msg }), {
      status,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  return new Response(JSON.stringify({ ok: true, item: row }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})
