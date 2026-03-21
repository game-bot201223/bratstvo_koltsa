import "jsr:@supabase/functions-js/edge-runtime.d.ts"

const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-telegram-init-data",
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
  const map = parseInitData(initData)
  const hash = map.get("hash") || ""
  if (!hash) return { ok: false }

  const dataCheckString = buildDataCheckString(map)
  const secretKey = await hmacSha256Bytes("WebAppData", botToken)
  const computed = await hmacSha256Hex(secretKey, dataCheckString)
  if (!timingSafeEqual(computed, String(hash).toLowerCase())) return { ok: false }

  let user: any = undefined
  try {
    const u = map.get("user")
    if (u) user = JSON.parse(u)
  } catch (_){
    user = undefined
  }
  return { ok: true, user }
}

function sanitizeClanId(id: unknown): string {
  const s = String(id || "").trim()
  if (!s) return ""
  if (!/^CLN\d{1,20}$/.test(s)) return ""
  return s
}

function safeStr(v: unknown): string {
  return String(v || "").trim()
}

function escapeLikeExact(input: string): string {
  // Escape LIKE wildcards so ilike matches exact string
  return input.replace(/[\\%_]/g, (m) => `\\${m}`)
}

async function postgrestFindExistingNames(
  projectUrl: string,
  serviceKey: string,
  names: string[],
): Promise<Set<string>> {
  const out = new Set<string>()
  const uniq = Array.from(new Set(names.map((x) => safeStr(x)).filter(Boolean)))
  if (!uniq.length) return out

  // Chunk to keep URL manageable
  const chunkSize = 40
  for (let i = 0; i < uniq.length; i += chunkSize) {
    const chunk = uniq.slice(i, i + chunkSize)
    const orParts = chunk.map((nm) => `name.ilike.${escapeLikeExact(nm)}`)
    const url = projectUrl.replace(/\/$/, "") +
      `/rest/v1/players?select=name&or=(${encodeURIComponent(orParts.join(","))})&limit=${chunk.length}`
    const resp = await fetch(url, {
      method: "GET",
      headers: {
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
      },
    })
    if (!resp.ok) continue
    const rows = await resp.json().catch(() => [])
    if (!Array.isArray(rows)) continue
    rows.forEach((r) => {
      try {
        const nm = safeStr((r as any)?.name)
        if (nm) out.add(nm.toUpperCase())
      } catch (_e) {}
    })
  }
  return out
}

async function postgrestListClans(projectUrl: string, serviceKey: string, limit: number): Promise<Response> {
  const lim = Math.max(1, Math.min(200, Math.floor(limit || 100)))
  const url = projectUrl.replace(/\/$/, "") + `/rest/v1/clans?select=id,name,owner_tg_id,data,updated_at&order=updated_at.desc&limit=${lim}`
  return await fetch(url, {
    method: "GET",
    headers: {
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
    },
  })
}

function isJwtLike(s: string): boolean {
  try {
    const t = String(s || "").trim()
    return !!(t && t.startsWith("eyJ") && t.split(".").length >= 3)
  } catch (_e) {
    return false
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
  const projectUrl = String(Deno.env.get("PROJECT_URL") || Deno.env.get("SUPABASE_URL") || "").trim()
  const serviceKeyRaw = String(Deno.env.get("SERVICE_ROLE_KEY") || "").trim()
  const serviceKeyFallback = String(Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "").trim()
  const serviceKey = isJwtLike(serviceKeyRaw) ? serviceKeyRaw : serviceKeyFallback
  if (!botToken || !projectUrl || !serviceKey) {
    const missing: string[] = []
    if (!botToken) missing.push("TELEGRAM_BOT_TOKEN")
    if (!projectUrl) missing.push("PROJECT_URL/SUPABASE_URL")
    if (!serviceKey) missing.push("SERVICE_ROLE_KEY/SUPABASE_SERVICE_ROLE_KEY")
    return new Response(JSON.stringify({ ok: false, error: "missing_secrets", missing }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  let body: any = {}
  try {
    body = await req.json()
  } catch (_){
    return new Response(JSON.stringify({ ok: false, error: "invalid_json" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
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

  const limit = Number(body?.limit)
  const resp = await postgrestListClans(projectUrl, serviceKey, Number.isFinite(limit) ? limit : 100)
  if (!resp.ok) {
    const text = await resp.text().catch(() => "")
    return new Response(JSON.stringify({ ok: false, error: "supabase_error", details: text.slice(0, 500) }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const rows = await resp.json().catch(() => [])
  const arr = Array.isArray(rows) ? rows : []

  // Remove stale deleted accounts from clan members/apps.
  // Clan membership is stored by name. Since names are unique, we can validate against players.
  const allNames: string[] = []
  arr.forEach((row: any) => {
    try {
      const data = row?.data
      if (!data || typeof data !== "object") return
      const members = Array.isArray(data.members) ? data.members : []
      const apps = Array.isArray(data.apps) ? data.apps : []
      members.forEach((x: any) => { const s = safeStr(x); if (s) allNames.push(s) })
      apps.forEach((x: any) => { const s = safeStr(x); if (s) allNames.push(s) })
    } catch (_e) {}
  })
  const existing = await postgrestFindExistingNames(projectUrl, serviceKey, allNames)

  const cleaned = arr.map((row: any) => {
    try {
      const data = row?.data
      if (!data || typeof data !== "object") return row
      const members0 = Array.isArray(data.members) ? data.members : []
      const apps0 = Array.isArray(data.apps) ? data.apps : []
      const members = members0.filter((nm: any) => {
        const s = safeStr(nm)
        if (!s) return false
        return existing.has(s.toUpperCase())
      })
      const apps = apps0.filter((nm: any) => {
        const s = safeStr(nm)
        if (!s) return false
        return existing.has(s.toUpperCase())
      })
      return { ...row, data: { ...data, members, apps } }
    } catch (_e) {
      return row
    }
  })

  return new Response(JSON.stringify({ ok: true, clans: cleaned }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})

 export {}
