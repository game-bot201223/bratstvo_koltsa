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

async function verifyTelegramInitData(
  initData: string,
  botToken: string,
): Promise<{ ok: boolean; user?: any }> {
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

function sanitizeClanId(id: unknown): string {
  const s = String(id || "").trim().toUpperCase()
  if (!s) return ""
  if (!/^CLN\d{1,20}$/.test(s)) return ""
  return s
}

function sanitizeName(name: unknown): string {
  return String(name || "").trim().slice(0, 18)
}

function sanitizeText(text: unknown): string {
  return String(text || "").trim().slice(0, 140)
}

async function postgrestGetClan(projectUrl: string, serviceKey: string, clanId: string): Promise<any | null> {
  const url = projectUrl.replace(/\/$/, "") +
    `/rest/v1/clans?id=eq.${encodeURIComponent(clanId)}&select=id,owner_tg_id,data&limit=1`
  const resp = await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
  })
  if (!resp.ok) return null
  const rows = await resp.json().catch(() => [])
  const row = Array.isArray(rows) && rows.length ? rows[0] : null
  return row && typeof row === "object" ? row : null
}

async function postgrestUpdateClanData(
  projectUrl: string,
  serviceKey: string,
  clanId: string,
  data: any,
): Promise<Response> {
  const url = projectUrl.replace(/\/$/, "") + `/rest/v1/clans?id=eq.${encodeURIComponent(clanId)}`
  return await fetch(url, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
      Prefer: "return=minimal",
    },
    body: JSON.stringify({ data, updated_at: new Date().toISOString() }),
  })
}

function isMember(clanObj: any, memberName: string): boolean {
  try {
    const up = String(memberName || "").trim().toUpperCase()
    if (!up) return false
    const leader = String(clanObj?.leader || "").trim().toUpperCase()
    if (leader && leader === up) return true
    const deputy = String(clanObj?.deputy || "").trim().toUpperCase()
    if (deputy && deputy === up) return true
    const mem = Array.isArray(clanObj?.members) ? clanObj.members : []
    return mem.some((x: any) => String(x || "").trim().toUpperCase() === up)
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
    return new Response(JSON.stringify({ ok: false, error: "unauthorized", reason: "bad_init_data" }), {
      status: 401,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const clanId = sanitizeClanId(body?.clan_id ?? body?.id)
  const memberName = sanitizeName(body?.member_name ?? body?.name)
  const text = sanitizeText(body?.text)

  if (!clanId) {
    return new Response(JSON.stringify({ ok: false, error: "bad_clan_id" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }
  if (!memberName) {
    return new Response(JSON.stringify({ ok: false, error: "bad_member" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }
  if (!text) {
    return new Response(JSON.stringify({ ok: false, error: "bad_text" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const row = await postgrestGetClan(projectUrl, serviceKey, clanId)
  if (!row) {
    return new Response(JSON.stringify({ ok: false, error: "not_found" }), {
      status: 404,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const data = (row as any).data
  const clanObj = (data && typeof data === "object") ? data : {}

  if (!isMember(clanObj, memberName)) {
    return new Response(JSON.stringify({ ok: false, error: "forbidden" }), {
      status: 403,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  if (!Array.isArray((clanObj as any).chat)) (clanObj as any).chat = []
  ;(clanObj as any).chat.push({ ts: Date.now(), from: memberName, text })
  if ((clanObj as any).chat.length > 120) (clanObj as any).chat = (clanObj as any).chat.slice(-120)

  const resp = await postgrestUpdateClanData(projectUrl, serviceKey, clanId, clanObj)
  if (!resp.ok) {
    const details = await resp.text().catch(() => "")
    return new Response(JSON.stringify({ ok: false, error: "supabase_error", details: details.slice(0, 500) }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  return new Response(JSON.stringify({ ok: true }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})
