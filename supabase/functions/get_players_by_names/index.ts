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
    try {
      variants.push({ map: parseInitData(c), userJsonNeedsDecode: false })
    } catch (_e) {}
    try {
      variants.push({ map: parseInitDataRaw(c), userJsonNeedsDecode: true })
    } catch (_e) {}
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
    } catch (_e) {
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

function sanitizeName(nm: unknown): string {
  const s = String(nm || "").trim().slice(0, 18)
  return s
}

function escapeLikePattern(s: string): string {
  // Escape SQL LIKE wildcards. PostgREST supports backslash escaping in patterns.
  return String(s || "").replace(/([%_\\])/g, "\\$1")
}

async function postgrestGetPlayersByNames(projectUrl: string, serviceKey: string, names: string[]): Promise<any[]> {
  try {
    const uniq: string[] = []
    const seen = new Set<string>()
    for (const n0 of names) {
      const n = sanitizeName(n0)
      if (!n) continue
      const k = n.toLowerCase()
      if (seen.has(k)) continue
      seen.add(k)
      uniq.push(n)
      if (uniq.length >= 60) break
    }
    if (!uniq.length) return []
    const base = projectUrl.replace(/\/$/, "")

    // Use case-insensitive matching (names coming from events can differ by case).
    // Also escape LIKE wildcards to avoid accidental broad matches.
    const orParts: string[] = []
    for (const n0 of uniq) {
      const n = escapeLikePattern(String(n0 || "").trim())
      if (!n) continue
      // Match exact string case-insensitively.
      orParts.push(`name.ilike.${n}`)
    }
    if (!orParts.length) return []
    const orExpr = `(${orParts.join(",")})`
    const url = base +
      `/rest/v1/players?select=tg_id,name,photo_url,level,stats_sum,boss_wins,arena_power&or=${encodeURIComponent(orExpr)}`

    const resp = await fetch(url, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp.ok) return []
    const rows = await resp.json().catch(() => [])
    return Array.isArray(rows) ? rows : []
  } catch (_e) {
    return []
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
  } catch (_e) {
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

  const names0 = body?.names
  const names = Array.isArray(names0) ? names0 : []
  const players = await postgrestGetPlayersByNames(projectUrl, serviceKey, names.map(String))

  return new Response(JSON.stringify({ ok: true, players }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})
