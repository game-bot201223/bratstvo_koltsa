import "jsr:@supabase/functions-js/edge-runtime.d.ts"

const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-telegram-init-data",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
}

const HAVCHIK_ENERGY_BY_TYPE: number[] = [5, 10, 15, 20, 25, 30]
const HAVCHIK_LEVEL_FOR_TYPE: number[] = [1, 10, 30, 60, 120, 200]
const HAVCHIK_CLAIM_DAILY_CAP = 200
const HAVCHIK_SEND_ONCE_PER_DAY = true

function getTodayUtcStartIso(): string {
  const d = new Date()
  const y = d.getUTCFullYear()
  const m = d.getUTCMonth()
  const day = d.getUTCDate()
  return new Date(Date.UTC(y, m, day, 0, 0, 0, 0)).toISOString()
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

function sanitizeClanId(v: unknown): string {
  const raw = String(v || "").trim().toUpperCase()
  if (!raw) return ""
  const clean = raw.replace(/[^A-Z0-9_\-]/g, "")
  return clean.slice(0, 24)
}

function sanitizeName(name: unknown): string {
  return String(name || "").trim().slice(0, 18)
}

function normName(name: unknown): string {
  return String(name || "").trim().toUpperCase()
}

function escapeLikeExact(s: string): string {
  return String(s || "").replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_")
}

function safeInt(v: unknown, def = 0): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return def
  return Math.floor(n)
}

async function postgrestGetPlayer(projectUrl: string, serviceKey: string, tgId: string): Promise<any | null> {
  const url = projectUrl.replace(/\/$/, "") +
    `/rest/v1/players?tg_id=eq.${encodeURIComponent(tgId)}&select=tg_id,name,state,level&limit=1`
  const resp = await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
  })
  if (!resp.ok) return null
  const rows = await resp.json().catch(() => [])
  const row = Array.isArray(rows) && rows.length ? rows[0] : null
  return row && typeof row === "object" ? row : null
}

async function postgrestCountSendsToday(projectUrl: string, serviceKey: string, fromTgId: string): Promise<number> {
  const todayStart = getTodayUtcStartIso()
  const url = projectUrl.replace(/\/$/, "") +
    `/rest/v1/havchik_inbox?from_tg_id=eq.${encodeURIComponent(fromTgId)}` +
    `&created_at=gte.${encodeURIComponent(todayStart)}` +
    `&select=id`
  const resp = await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
  })
  if (!resp.ok) return 999
  const rows = await resp.json().catch(() => [])
  return Array.isArray(rows) ? rows.length : 0
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

async function postgrestFindPlayerByNameLower(projectUrl: string, serviceKey: string, nameLower: string): Promise<any | null> {
  try {
    const nm = String(nameLower || "").trim()
    if (!nm) return null
    const nmNorm = normName(nm)
    const likeExact = escapeLikeExact(nm)
    const url = projectUrl.replace(/\/$/, "") +
      `/rest/v1/players?name=ilike.${encodeURIComponent(likeExact)}&select=tg_id,name&limit=5`
    const resp = await fetch(url, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp.ok) return null
    const rows = await resp.json().catch(() => [])
    const arr = Array.isArray(rows) ? rows : []
    for (const row of arr) {
      if (!row || typeof row !== "object") continue
      const n2 = String((row as any).name || "").trim()
      if (!n2) continue
      if (normName(n2) !== nmNorm) continue
      return row
    }
    return null
  } catch (_e) {
    return null
  }
}

async function postgrestListPlayersByClanId(projectUrl: string, serviceKey: string, clanId: string): Promise<any[]> {
  try {
    const filter = JSON.stringify({ clan: { id: clanId } })
    const url1 = projectUrl.replace(/\/$/, "") +
      `/rest/v1/players?state=cs.${encodeURIComponent(filter)}&select=tg_id,name&limit=200`
    const resp1 = await fetch(url1, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    const rows1 = resp1.ok ? await resp1.json().catch(() => []) : []
    const out1 = Array.isArray(rows1) ? rows1 : []
    if (out1.length) return out1
    const url2 = projectUrl.replace(/\/$/, "") +
      `/rest/v1/players?state->clan->>id=ilike.${encodeURIComponent(clanId)}&select=tg_id,name&limit=200`
    const resp2 = await fetch(url2, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    const rows2 = resp2.ok ? await resp2.json().catch(() => []) : []
    return Array.isArray(rows2) ? rows2 : []
  } catch (_e) {
    return []
  }
}

async function postgrestInsertHavchik(projectUrl: string, serviceKey: string, rows: any[]): Promise<Response> {
  const url = projectUrl.replace(/\/$/, "") + "/rest/v1/havchik_inbox"
  return await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
      Prefer: "return=minimal",
    },
    body: JSON.stringify(rows),
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

  const senderTgId = String(verified.user.id)
  const typeId = Math.max(0, Math.min(5, safeInt(body?.type_id ?? body?.typeId, 0)))
  const energy = HAVCHIK_ENERGY_BY_TYPE[typeId] ?? 5

  const senderRow = await postgrestGetPlayer(projectUrl, serviceKey, senderTgId)
  if (!senderRow) {
    return new Response(JSON.stringify({ ok: false, error: "sender_not_found" }), {
      status: 404,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const senderLevel = Math.max(1, safeInt(senderRow?.level ?? (senderRow?.state && (senderRow.state as any).level), 1))
  const minLevel = HAVCHIK_LEVEL_FOR_TYPE[typeId] ?? 1
  if (senderLevel < minLevel) {
    return new Response(JSON.stringify({
      ok: false,
      error: "level_required",
      required_level: minLevel,
      current_level: senderLevel,
    }), {
      status: 403,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  if (HAVCHIK_SEND_ONCE_PER_DAY) {
    const sendsToday = await postgrestCountSendsToday(projectUrl, serviceKey, senderTgId)
    if (sendsToday >= 1) {
      return new Response(JSON.stringify({
        ok: false,
        error: "one_send_per_day",
        reason: "Уже отправляли хавчик сегодня (сброс в 00:00 UTC)",
      }), {
        status: 429,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      })
    }
  }

  let clanId = sanitizeClanId(body?.clan_id ?? body?.clanId)
  if (!clanId) {
    try {
      const st = senderRow?.state
      if (st && typeof st === "object") {
        try { clanId = sanitizeClanId((st as any)?.clan?.id) } catch (_e0) {}
        if (!clanId) try { clanId = sanitizeClanId((st as any)?.clanId) } catch (_e1) {}
        if (!clanId) try { clanId = sanitizeClanId((st as any)?.clan_id) } catch (_e2) {}
      }
    } catch (_e) {}
  }

  const fromName = String(senderRow?.name || "Player").trim().slice(0, 18) || "Player"
  const senderNameLower = fromName.toLowerCase()
  const recipients = new Set<string>()

  try {
    const st = senderRow?.state
    const fr = st && typeof st === "object" ? (st as any).friends : null
    const list: any[] = Array.isArray(fr) ? fr : []
    let resolvedByName = 0
    for (const it of list.slice(0, 80)) {
      if (typeof it === "string") {
        const nm0 = String(it || "").trim().slice(0, 18)
        if (!nm0) continue
        const nl0 = nm0.toLowerCase()
        if (nl0 === senderNameLower) continue
        if (resolvedByName >= 20) continue
        try {
          const pr = await postgrestFindPlayerByNameLower(projectUrl, serviceKey, nl0)
          const tid2 = String(pr?.tg_id || "").trim()
          if (!tid2 || tid2 === senderTgId) continue
          recipients.add(tid2)
          resolvedByName += 1
        } catch (_e0) {}
        continue
      }
      const tid = String((it && typeof it === "object" ? ((it as any).tg_id || (it as any).tgId) : "") || "").trim()
      if (tid && tid !== senderTgId) recipients.add(tid)
      else if (it && typeof it === "object") {
        const nmRaw = String(it?.name || "").trim()
        const nm = nmRaw.slice(0, 18)
        if (!nm || nm.toLowerCase() === senderNameLower || resolvedByName >= 20) continue
        try {
          const pr = await postgrestFindPlayerByNameLower(projectUrl, serviceKey, nm.toLowerCase())
          const tid2 = String(pr?.tg_id || "").trim()
          if (!tid2 || tid2 === senderTgId) continue
          recipients.add(tid2)
          resolvedByName += 1
        } catch (_e0) {}
      }
    }
  } catch (_) {}

  if (clanId) {
    const rows = await postgrestListPlayersByClanId(projectUrl, serviceKey, clanId)
    for (const r of rows) {
      const tid = String((r as any)?.tg_id || "").trim()
      if (!tid || tid === senderTgId) continue
      recipients.add(tid)
    }
    try {
      const clanRow = await postgrestGetClan(projectUrl, serviceKey, clanId)
      const clanObj = (clanRow && typeof clanRow === "object") ? (clanRow as any).data : null
      const c = clanObj && typeof clanObj === "object" ? clanObj : {}
      const mem = Array.isArray((c as any).members) ? (c as any).members : []
      const ownerTid = String((clanRow as any)?.owner_tg_id || "").trim()
      if (ownerTid && ownerTid !== senderTgId) recipients.add(ownerTid)
      for (const m of mem) {
        if (!m) continue
        const tid = typeof m === "object" ? String((m as any).tg_id || (m as any).tgId || "").trim() : ""
        if (tid && tid !== senderTgId) recipients.add(tid)
      }
    } catch (_) {}
  }

  const recArr = Array.from(recipients)
  if (!recArr.length) {
    return new Response(JSON.stringify({ ok: true, inserted: 0, debug: { reason: "no_recipients" } }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const ts = new Date().toISOString()
  const rows = recArr.map((toTgId) => ({
    to_tg_id: toTgId,
    from_tg_id: senderTgId,
    from_name: fromName,
    type_id: typeId,
    energy,
    created_at: ts,
  }))

  const ins = await postgrestInsertHavchik(projectUrl, serviceKey, rows)
  if (!ins.ok) {
    const text = await ins.text().catch(() => "")
    return new Response(
      JSON.stringify({
        ok: false,
        error: "supabase_error",
        details: text.slice(0, 500),
      }),
      {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      },
    )
  }

  return new Response(
    JSON.stringify({ ok: true, inserted: rows.length }),
    {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    },
  )
})
