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

function buildDataCheckString(map: Map<string, string>): string {
  const pairs: string[] = []
  for (const [k, v] of map.entries()) {
    if (k === "hash") continue
    pairs.push(`${k}=${v}`)
  }
  pairs.sort((a, b) => a.localeCompare(b))
  return pairs.join("\n")
}

async function verifyTelegramInitData(initData: string, botToken: string): Promise<{ ok: boolean; user?: any }> {
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
  } catch (_) {
    user = undefined
  }
  return { ok: true, user }
}

async function getPlayer(projectUrl: string, serviceKey: string, tgId: string): Promise<any | null> {
  const url = projectUrl.replace(/\/$/, "") + `/rest/v1/players?tg_id=eq.${encodeURIComponent(tgId)}&select=tg_id,state&limit=1`
  const resp = await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
  })
  if (!resp.ok) return null
  const rows = await resp.json().catch(() => [])
  return Array.isArray(rows) && rows.length ? rows[0] : null
}

async function patchPlayerState(projectUrl: string, serviceKey: string, tgId: string, state: any): Promise<Response> {
  const url = projectUrl.replace(/\/$/, "") + `/rest/v1/players?tg_id=eq.${encodeURIComponent(tgId)}`
  return await fetch(url, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
    },
    body: JSON.stringify({ state }),
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
  } catch (_) {
    return new Response(JSON.stringify({ ok: false, error: "invalid_json" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const initData = String(body?.initData || req.headers.get("x-telegram-init-data") || "").trim()
  const referrerTgId = String(body?.referrer_tg_id ?? body?.referrerTgId ?? "").trim()
  if (!initData || !referrerTgId) {
    return new Response(JSON.stringify({ ok: false, error: "missing_init_data_or_referrer" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const verified = await verifyTelegramInitData(initData, botToken)
  if (!verified.ok || !verified.user?.id) {
    return new Response(JSON.stringify({ ok: false, error: "unauthorized" }), {
      status: 401,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const newUserTgId = String(verified.user.id)
  if (newUserTgId === referrerTgId) {
    return new Response(JSON.stringify({ ok: true, added: false, reason: "self" }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const newUserName = String(verified.user.first_name || verified.user.username || "Браток").trim().slice(0, 18) || "Браток"

  const referrerRow = await getPlayer(projectUrl, serviceKey, referrerTgId)
  if (!referrerRow) {
    return new Response(JSON.stringify({ ok: false, error: "referrer_not_found" }), {
      status: 404,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const state = referrerRow.state && typeof referrerRow.state === "object" ? referrerRow.state : {}
  const friends: any[] = Array.isArray(state.friends) ? state.friends.slice() : []
  const exists = friends.some(
    (f: any) =>
      (f && (String(f.tg_id || f.tgId || "").trim() === newUserTgId)) ||
      (f && String(f.name || "").trim().toUpperCase() === newUserName.toUpperCase())
  )
  if (exists) {
    return new Response(JSON.stringify({ ok: true, added: false, reason: "already_friend" }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  friends.push({ tg_id: newUserTgId, name: newUserName })
  const maxFriends = 250
  const trimmed = friends.slice(-maxFriends)
  const newState = { ...state, friends: trimmed }

  const patchResp = await patchPlayerState(projectUrl, serviceKey, referrerTgId, newState)
  if (!patchResp.ok) {
    const text = await patchResp.text().catch(() => "")
    return new Response(JSON.stringify({ ok: false, error: "supabase_error", details: text.slice(0, 300) }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  return new Response(JSON.stringify({ ok: true, added: true }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})
