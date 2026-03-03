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

function decodeUriMaybe(s: string): string {
  try {
    if (!s) return s
    // Only attempt decode if it looks encoded.
    if (!/%[0-9A-Fa-f]{2}/.test(s)) return s
    return decodeURIComponent(s)
  } catch (_e) {
    return s
  }
}

type VerifyResult = { ok: boolean; user?: any; debug?: { provided_hash_prefix: string; computed_hash_prefix: string; variant_kind: string; len: number } }

async function verifyTelegramInitData(initData: string, botToken: string): Promise<VerifyResult>{
  if (!initData || !botToken) return { ok: false }
  const secretKey = await hmacSha256Bytes("WebAppData", botToken)

  const initBase = String(initData || "")
  const initPlusLit = initBase.replace(/\+/g, "%2B")
  const initPlusSpace = initBase.replace(/\+/g, "%20")
  const initDecoded = decodeUriMaybe(initBase)
  const initDecodedPlusLit = decodeUriMaybe(initPlusLit)
  const initDecodedPlusSpace = decodeUriMaybe(initPlusSpace)

  const candidates = [initBase, initPlusLit, initPlusSpace, initDecoded, initDecodedPlusLit, initDecodedPlusSpace]
  const variants: Array<{ kind: string; map: Map<string, string>; userJsonNeedsDecode: boolean }> = []
  for (const c of candidates) {
    try { variants.push({ kind: "url", map: parseInitData(c), userJsonNeedsDecode: false }) } catch (_e) {}
    try { variants.push({ kind: "raw", map: parseInitDataRaw(c), userJsonNeedsDecode: true }) } catch (_e) {}
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
    } catch (_){
      user = undefined
    }
    return { ok: true, user, debug: { provided_hash_prefix: String(hash).slice(0, 12), computed_hash_prefix: String(computed).slice(0, 12), variant_kind: String(v.kind||''), len: String(initData||'').length } }
  }

  // best-effort debug based on first variant with hash
  try {
    for (const v of variants) {
      const map = v.map
      const hash = map.get("hash") || ""
      if (!hash) continue
      const dataCheckString = buildDataCheckString(map)
      const computed = await hmacSha256Hex(secretKey, dataCheckString)
      return { ok: false, debug: { provided_hash_prefix: String(hash).slice(0, 12), computed_hash_prefix: String(computed).slice(0, 12), variant_kind: String(v.kind||''), len: String(initData||'').length } }
    }
  } catch (_e) {}

  return { ok: false, debug: { provided_hash_prefix: "", computed_hash_prefix: "", variant_kind: "no_hash", len: String(initData||'').length } }
}

function safeInt(v: unknown, def = 0): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return def
  return Math.floor(n)
}

async function postgrestGetEvents(projectUrl: string, serviceKey: string, toTgId: string, limit: number): Promise<any[]> {
  const url = projectUrl.replace(/\/$/, "") +
    `/rest/v1/boss_help_events?to_tg_id=eq.${encodeURIComponent(toTgId)}&consumed=eq.false&select=id,from_name,from_tg_id,boss_id,dmg,created_at,clan_id&order=id.asc&limit=${encodeURIComponent(String(limit))}`
  const resp = await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
  })
  if (!resp.ok) return []
  const rows = await resp.json().catch(() => [])
  return Array.isArray(rows) ? rows : []
}

async function postgrestMarkConsumed(projectUrl: string, serviceKey: string, ids: number[]): Promise<Response> {
  const inList = ids.map((x) => String(Math.max(0, Math.floor(x)))).filter(Boolean).join(",")
  const url = projectUrl.replace(/\/$/, "") +
    `/rest/v1/boss_help_events?id=in.(${encodeURIComponent(inList)})`
  return await fetch(url, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
      Prefer: "return=minimal",
    },
    body: JSON.stringify({ consumed: true, consumed_at: new Date().toISOString() }),
  })
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
  } catch (_){
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
    return new Response(JSON.stringify({ ok: false, error: "unauthorized", reason: "bad_init_data", debug: verified.debug || null }), {
      status: 401,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const toTgId = String(verified.user.id)
  const limit = Math.max(1, Math.min(60, safeInt(body?.limit, 30)))

  const ev = await postgrestGetEvents(projectUrl, serviceKey, toTgId, limit)
  const ids = ev.map((r: any) => safeInt(r?.id, 0)).filter((x) => x > 0)
  if (ids.length) {
    await postgrestMarkConsumed(projectUrl, serviceKey, ids).catch(() => null)
  }

  return new Response(JSON.stringify({ ok: true, events: ev }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})
