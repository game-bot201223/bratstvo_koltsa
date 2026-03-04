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

async function verifyTelegramInitData(
  initData: string,
  botToken: string,
): Promise<{ ok: boolean; user?: any; reason?: string; debug?: { provided_hash_prefix: string; computed_hash_prefix: string; variant_kind: string; len: number } }>{
  if (!botToken) return { ok: false, reason: "missing_bot_token" }
  if (!String(botToken).includes(":")) return { ok: false, reason: "bad_bot_token_format" }
  if (!initData) return { ok: false, reason: "missing_init_data" }
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

  try {
    for (const v of variants) {
      const map = v.map
      const hash = map.get("hash") || ""
      if (!hash) continue
      const dataCheckString = buildDataCheckString(map)
      const computed = await hmacSha256Hex(secretKey, dataCheckString)
      return { ok: false, reason: "hash_mismatch", debug: { provided_hash_prefix: String(hash).slice(0, 12), computed_hash_prefix: String(computed).slice(0, 12), variant_kind: String(v.kind||''), len: String(initData||'').length } }
    }
  } catch (_e) {}

  return { ok: false, reason: "missing_hash", debug: { provided_hash_prefix: "", computed_hash_prefix: "", variant_kind: "no_hash", len: String(initData||'').length } }
}

function sanitizeClanId(id: unknown): string {
  const s = String(id || "").trim()
  if (!s) return ""
  if (!/^CLN\d{1,20}$/.test(s)) return ""
  return s
}

function sanitizeName(name: unknown): string {
  return String(name || "").trim().slice(0, 18)
}

function normName(name: unknown): string {
  return String(name || "").trim().toUpperCase()
}

async function postgrestRateLimitAllow(
  projectUrl: string,
  serviceKey: string,
  key: string,
  windowMs: number,
): Promise<{ ok: boolean; allowed: boolean; next_allow_at?: string }> {
  try {
    const url = projectUrl.replace(/\/$/, "") + "/rest/v1/rpc/rate_limit_allow"
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
      },
      body: JSON.stringify({ p_key: key, p_window_ms: windowMs }),
    })
    if (!resp.ok) return { ok: false, allowed: true }
    const rows = await resp.json().catch(() => [])
    const row = Array.isArray(rows) && rows.length ? rows[0] : null
    const allowed = !!(row && typeof row === "object" ? (row as any).allowed : true)
    const next = row && typeof row === "object" ? String((row as any).next_allow_at || "") : ""
    return { ok: true, allowed, next_allow_at: next || undefined }
  } catch (_e) {
    return { ok: false, allowed: true }
  }
}

async function postgrestGetClan(projectUrl: string, serviceKey: string, clanId: string): Promise<any | null> {
  const url = projectUrl.replace(/\/$/, "") + `/rest/v1/clans?id=eq.${encodeURIComponent(clanId)}&select=id,owner_tg_id,data&limit=1`
  const resp = await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
  })
  if (!resp.ok) return null
  const rows = await resp.json().catch(() => [])
  const row = Array.isArray(rows) && rows.length ? rows[0] : null
  return row && typeof row === "object" ? row : null
}

async function postgrestUpdateClanData(projectUrl: string, serviceKey: string, clanId: string, data: any): Promise<Response> {
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

async function postgrestDeleteClan(projectUrl: string, serviceKey: string, clanId: string): Promise<Response> {
  const url = projectUrl.replace(/\/$/, "") + `/rest/v1/clans?id=eq.${encodeURIComponent(clanId)}`
  return await fetch(url, {
    method: "DELETE",
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
  if (!initData) {
    return new Response(JSON.stringify({ ok: false, error: "missing_init_data" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const verified = await verifyTelegramInitData(initData, botToken)
  if (!verified.ok || !verified.user?.id) {
    return new Response(JSON.stringify({ ok: false, error: "unauthorized", reason: verified.reason || "bad_init_data", debug: verified.debug || null }), {
      status: 401,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  // Soft rate limit: avoid repeated leave spam.
  try {
    const tgId2 = String(verified.user.id)
    const rlKey = `clan_leave:${tgId2}`
    const rl = await postgrestRateLimitAllow(projectUrl, serviceKey, rlKey, 1200)
    if (rl.ok && !rl.allowed) {
      return new Response(JSON.stringify({ ok: true, skipped: true, reason: "rate_limited", next_allow_at: rl.next_allow_at || null }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      })
    }
  } catch (_e) {}

  const clanId = sanitizeClanId(body?.clan_id ?? body?.id)
  const member = sanitizeName(body?.member_name ?? body?.name)
  if (!clanId) {
    return new Response(JSON.stringify({ ok: false, error: "bad_clan_id" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }
  if (!member) {
    return new Response(JSON.stringify({ ok: false, error: "bad_member" }), {
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
  const members = Array.isArray((clanObj as any).members) ? (clanObj as any).members : []
  const memberNorm = normName(member)
  const nextMembers = members.filter((x: any) => String(x || "").trim().toUpperCase() !== memberNorm)
  ;(clanObj as any).members = nextMembers
  if (String((clanObj as any).deputy || "").trim().toUpperCase() === memberNorm) (clanObj as any).deputy = ""

  const leaderNorm = normName((clanObj as any).leader)
  if (leaderNorm && leaderNorm === memberNorm && nextMembers.length > 0) {
    return new Response(JSON.stringify({ ok: false, error: "leader_must_transfer" }), {
      status: 409,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  // if empty after leaving -> delete clan row
  if (!nextMembers.length) {
    const del = await postgrestDeleteClan(projectUrl, serviceKey, clanId)
    if (!del.ok) {
      const text = await del.text().catch(() => "")
      return new Response(JSON.stringify({ ok: false, error: "supabase_error", details: text.slice(0, 500) }), {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      })
    }
    return new Response(JSON.stringify({ ok: true, deleted: true }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const resp = await postgrestUpdateClanData(projectUrl, serviceKey, clanId, clanObj)
  if (!resp.ok) {
    const text = await resp.text().catch(() => "")
    return new Response(JSON.stringify({ ok: false, error: "supabase_error", details: text.slice(0, 500) }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  return new Response(JSON.stringify({ ok: true, deleted: false }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})
