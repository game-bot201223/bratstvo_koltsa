import "@supabase/functions-js/edge-runtime.d.ts"

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

async function verifyTelegramInitData(
  initData: string,
  botToken: string,
): Promise<{ ok: boolean; user?: any; reason?: string }>{
  if (!botToken) return { ok: false, reason: "missing_bot_token" }
  if (!String(botToken).includes(":")) return { ok: false, reason: "bad_bot_token_format" }
  if (!initData) return { ok: false, reason: "missing_init_data" }
  const map = parseInitData(initData)
  const hash = map.get("hash") || ""
  if (!hash) return { ok: false, reason: "missing_hash" }

  const dataCheckString = buildDataCheckString(map)
  const secretKey = await hmacSha256Bytes("WebAppData", botToken)
  const computed = await hmacSha256Hex(secretKey, dataCheckString)
  if (!timingSafeEqual(computed, String(hash).toLowerCase())) return { ok: false, reason: "hash_mismatch" }

  let user: any = undefined
  try {
    const u = map.get("user")
    if (u) user = JSON.parse(u)
  } catch (_){
    user = undefined
  }
  return { ok: true, user }
}

function safeNonNegInt(v: unknown): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return 0
  return Math.max(0, Math.floor(n))
}

async function postgrestFindOpponent(projectUrl: string, serviceKey: string, tgId: string, minP: number, maxP: number): Promise<Response> {
  const base = projectUrl.replace(/\/$/, "")
  const url = base +
    `/rest/v1/players?select=tg_id,name,photo_url,arena_power,stats_sum,level&tg_id=neq.${encodeURIComponent(tgId)}` +
    `&arena_power=gte.${minP}&arena_power=lte.${maxP}&order=arena_power.asc&limit=25`
  return await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
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
    return new Response(JSON.stringify({ ok: false, error: "invalid_json" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const initData = String(body?.initData || req.headers.get("x-telegram-init-data") || "").trim()
  const verified = await verifyTelegramInitData(initData, botToken)
  if (!verified.ok || !verified.user?.id) {
    return new Response(JSON.stringify({ ok: false, error: "unauthorized", reason: verified.reason || "bad_init_data" }), {
      status: 401,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const tgId = String(verified.user.id)
  const minPower = safeNonNegInt(body?.min_power ?? body?.minPower)
  const maxPower = safeNonNegInt(body?.max_power ?? body?.maxPower)
  if (!minPower || !maxPower || maxPower < minPower) {
    return new Response(JSON.stringify({ ok: false, error: "bad_range" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const resp = await postgrestFindOpponent(projectUrl, serviceKey, tgId, minPower, maxPower)
  if (!resp.ok) {
    const text = await resp.text().catch(() => "")
    return new Response(JSON.stringify({ ok: false, error: "supabase_error", details: text.slice(0, 500) }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const rows = await resp.json().catch(() => [])
  const arr = Array.isArray(rows) ? rows : []
  const pick = arr.length ? arr[Math.floor(Math.random() * arr.length)] : null

  return new Response(JSON.stringify({ ok: true, opponent: pick }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})
