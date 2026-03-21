// Setup type definitions for built-in Supabase Runtime APIs
import "jsr:@supabase/functions-js/edge-runtime.d.ts"

const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-telegram-init-data",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
}

function isJwtLike(s: string): boolean {
  const v = String(s || "").trim()
  if (!v) return false
  const parts = v.split(".")
  return parts.length === 3 && parts.every((p) => !!p)
}

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false
  let out = 0
  for (let i = 0; i < a.length; i++) out |= a.charCodeAt(i) ^ b.charCodeAt(i)
  return out === 0
}

async function sha256Bytes(input: string): Promise<Uint8Array> {
  const data = new TextEncoder().encode(input)
  const hash = await crypto.subtle.digest("SHA-256", data)
  return new Uint8Array(hash)
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

async function postgrestGetPlayer(projectUrl: string, serviceKey: string, tgId: string): Promise<Response> {
  const url = projectUrl.replace(/\/$/, "") + `/rest/v1/players?tg_id=eq.${encodeURIComponent(tgId)}&select=tg_id,name,photo_url,arena_power,level,stats_sum,boss_wins,state,active_session_id,active_session_updated_at,active_device_id,updated_at`
  return await fetch(url, {
    method: "GET",
    headers: {
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
    },
  })
}

async function postgrestGetPlayerByName(projectUrl: string, serviceKey: string, name: string): Promise<Response> {
  const url = projectUrl.replace(/\/$/, "") + `/rest/v1/players?name=ilike.*${encodeURIComponent(name)}*&select=tg_id,name,photo_url,arena_power,level,stats_sum,boss_wins,state,active_session_id,active_session_updated_at,active_device_id,updated_at&limit=1`
  return await fetch(url, {
    method: "GET",
    headers: {
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
    },
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
  const projectUrl = String(Deno.env.get("PROJECT_URL") || Deno.env.get("SUPABASE_URL") || "").trim()
  const serviceKeyRaw = String(Deno.env.get("SERVICE_ROLE_KEY") || "").trim()
  const serviceKeyFallback = String(Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "").trim()
  const serviceKey = isJwtLike(serviceKeyRaw)
    ? serviceKeyRaw
    : (isJwtLike(serviceKeyFallback) ? serviceKeyFallback : "")
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

  const tgId = String(verified.user.id)
  const adminIdsRaw = String(Deno.env.get("ADMIN_TG_IDS") || Deno.env.get("ADMIN_TG_ID") || "8794843839").trim()
  const adminIds = adminIdsRaw.split(/[\s,;]+/g).map((x) => x.trim()).filter((x) => x)
  const isAdmin = adminIds.includes(tgId)

  const targetTgId = String(body?.target_tg_id ?? (body as any)?.targetTgId ?? "").trim()
  const targetName = String(body?.target_name ?? (body as any)?.targetName ?? "").trim()

  const resp = (isAdmin && (targetTgId || targetName))
    ? (targetTgId ? await postgrestGetPlayer(projectUrl, serviceKey, targetTgId) : await postgrestGetPlayerByName(projectUrl, serviceKey, targetName))
    : await postgrestGetPlayer(projectUrl, serviceKey, tgId)
  if (!resp.ok) {
    const text = await resp.text().catch(() => "")
    return new Response(JSON.stringify({ ok: false, error: "supabase_error", details: text.slice(0, 500) }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const rows = await resp.json().catch(() => [])
  const row = Array.isArray(rows) && rows.length ? rows[0] : null

  return new Response(JSON.stringify({ ok: true, player: row || null }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})

 export {}
