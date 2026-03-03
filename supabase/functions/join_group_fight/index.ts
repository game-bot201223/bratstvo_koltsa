// Follow this setup guide to integrate the Deno language server with your editor:
// https://deno.land/manual/getting_started/setup_your_environment
// This enables autocomplete, go to definition, etc.

// Setup type definitions for built-in Supabase Runtime APIs
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
  } catch (_) {
    user = undefined
  }
  return { ok: true, user }
}

async function postgrestUpsertGroupFightEntry(projectUrl: string, serviceKey: string, payload: Record<string, unknown>): Promise<Response> {
  const url = projectUrl.replace(/\/$/, "") + "/rest/v1/group_fight_entries?on_conflict=start_ts,tg_id"
  return await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
      Prefer: "resolution=merge-duplicates",
    },
    body: JSON.stringify(payload),
  })
}

function safeNonNegInt(v: unknown): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return 0
  return Math.max(0, Math.floor(n))
}

function normalizeIsoTs(input: string): string | null {
  const d = new Date(input)
  const t = d.getTime()
  if (!Number.isFinite(t)) return null
  return new Date(t).toISOString()
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
    return new Response(JSON.stringify({ ok: false, error: "unauthorized", reason: verified.reason || "bad_init_data" }), {
      status: 401,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const startTsRaw = String(body?.start_ts || body?.startTs || "").trim()
  const startTsIso = startTsRaw ? normalizeIsoTs(startTsRaw) : null
  if (!startTsIso) {
    return new Response(JSON.stringify({ ok: false, error: "missing_start_ts" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const tgId = String(verified.user.id)
  const name = String(body?.name || verified.user?.first_name || verified.user?.username || "Player").trim() || "Player"
  const photoUrl = String(body?.photo_url || verified.user?.photo_url || "")
  const safeStatsSum = safeNonNegInt(body?.stats_sum ?? (body as any)?.statsSum)

  const payload: Record<string, unknown> = {
    start_ts: startTsIso,
    tg_id: tgId,
    name,
    photo_url: photoUrl,
    stats_sum: safeStatsSum,
    updated_at: new Date().toISOString(),
  }
  delete (payload as any).initData

  const resp = await postgrestUpsertGroupFightEntry(projectUrl, serviceKey, payload)
  if (!resp.ok) {
    const text = await resp.text().catch(() => "")
    return new Response(JSON.stringify({ ok: false, error: "supabase_error", details: text.slice(0, 500) }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  return new Response(JSON.stringify({ ok: true, tg_id: tgId, start_ts: startTsIso }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})

/* To invoke locally:

  1. Run `supabase start` (see: https://supabase.com/docs/reference/cli/supabase-start)
  2. Make an HTTP request:

  curl -i --location --request POST 'http://127.0.0.1:54321/functions/v1/join_group_fight' \
    --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0' \
    --header 'Content-Type: application/json' \
    --data '{"name":"Functions"}'

*/
