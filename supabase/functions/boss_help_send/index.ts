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

async function postgrestFindActiveBossFightMetaForBoss(
  projectUrl: string,
  serviceKey: string,
  ownerTgId: string,
  bossId: number,
): Promise<{ boss_id: number; fight_started_at: string } | null> {
  try {
    const bid = safeInt(bossId, 0)
    if (bid <= 0) return null
    const nowIso = new Date().toISOString()
    const url = projectUrl.replace(/\/$/, "") +
      `/rest/v1/player_boss_fights?owner_tg_id=eq.${encodeURIComponent(ownerTgId)}` +
      `&boss_id=eq.${encodeURIComponent(String(bid))}` +
      `&reward_claimed=eq.false` +
      `&hp=gt.0` +
      `&or=${encodeURIComponent(`(expires_at.is.null,expires_at.gt.${nowIso})`)}` +
      `&select=boss_id,fight_started_at&order=updated_at.desc&limit=1`
    const resp = await fetch(url, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp.ok) return null
    const rows = await resp.json().catch(() => [])
    const row = Array.isArray(rows) && rows.length ? rows[0] : null
    const bid2 = safeInt(row && typeof row === "object" ? (row as any).boss_id : 0, 0)
    const started = row && typeof row === "object" ? String((row as any).fight_started_at || "").trim() : ""
    if (bid2 <= 0) return null
    return { boss_id: bid2, fight_started_at: started }
  } catch (_e) {
    return null
  }
}

async function verifyTelegramInitData(initData: string, botToken: string): Promise<{ ok: boolean; user?: any }>{
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
  // keep only letters, numbers, underscore, dash
  const clean = raw.replace(/[^A-Z0-9_\-]/g, "")
  return clean.slice(0, 24)
}

function inferClanIdFromState(st: any): string {
  try {
    if (!st || typeof st !== "object") return ""
    let cid = ""
    try { cid = String((st as any)?.clan?.id || "").trim() } catch (_e0) {}
    if (!cid) {
      try { cid = String((st as any)?.clanId || "").trim() } catch (_e1) {}
    }
    if (!cid) {
      try { cid = String((st as any)?.clan_id || "").trim() } catch (_e2) {}
    }
    if (!cid) {
      try {
        const c = (st as any)?.clan
        if (typeof c === "string") cid = String(c || "").trim()
        else if (c && typeof c === "object") cid = String((c as any).id || "").trim()
      } catch (_e3) {}
    }
    return sanitizeClanId(cid)
  } catch (_e) {
    return ""
  }
}

function recipientAllowsSenderHelp(recipientState: any, senderTgId: string, senderNameLower: string, senderClanId: string): boolean {
  try {
    // Clanmates are bros automatically
    const rcid = inferClanIdFromState(recipientState)
    if (rcid && senderClanId && rcid === senderClanId) return true

    // Otherwise must be in recipient friends list
    const fr = recipientState && typeof recipientState === "object" ? (recipientState as any).friends : null
    const list: any[] = Array.isArray(fr) ? fr : []
    for (const it of list.slice(0, 200)) {
      if (!it) continue
      if (typeof it === "string") {
        const nl = String(it || "").trim().slice(0, 18).toLowerCase()
        if (nl && nl === senderNameLower) return true
        continue
      }
      if (typeof it === "object") {
        const tid = String((it as any).tg_id || (it as any).tgId || "").trim()
        if (tid && tid === senderTgId) return true
        const nm = String((it as any).name || "").trim().slice(0, 18).toLowerCase()
        if (nm && nm === senderNameLower) return true
      }
    }
    return false
  } catch (_e) {
    return false
  }
}

function escapeLikeExact(s: string): string {
  // Escape LIKE wildcards so names containing %/_ don't match other rows.
  return String(s || "").replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_")
}

function sanitizeName(name: unknown): string {
  return String(name || "").trim().slice(0, 18)
}

function normName(name: unknown): string {
  return String(name || "").trim().toUpperCase()
}

function safeInt(v: unknown, def = 0): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return def
  return Math.floor(n)
}

function clampInt(n: number, lo: number, hi: number): number {
  if (!Number.isFinite(n)) return lo
  return Math.max(lo, Math.min(hi, Math.floor(n)))
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

function bossDef(bossId: number): { boss_id: number; max_hp: number } | null {
  const defs: Record<number, number> = {
    1: 2500,
    2: 10000,
    3: 50000,
    4: 200000,
    5: 1000000,
    6: 2000000,
    7: 5000000,
    8: 10000000,
    9: 20000000,
    10: 50000000,
    11: 100000000,
    12: 200000000,
  }
  const maxHp = defs[bossId]
  if (!maxHp) return null
  return { boss_id: bossId, max_hp: maxHp }
}

type ActiveFightMeta = { boss_id: number; fight_started_at: string }

async function postgrestFindActiveBossFightMeta(
  projectUrl: string,
  serviceKey: string,
  ownerTgId: string,
): Promise<ActiveFightMeta | null> {
  try {
    const nowIso = new Date().toISOString()
    const url = projectUrl.replace(/\/$/, "") +
      `/rest/v1/player_boss_fights?owner_tg_id=eq.${encodeURIComponent(ownerTgId)}` +
      `&reward_claimed=eq.false` +
      `&hp=gt.0` +
      `&or=${encodeURIComponent(`(expires_at.is.null,expires_at.gt.${nowIso})`)}` +
      `&select=boss_id,fight_started_at&order=updated_at.desc&limit=1`
    const resp = await fetch(url, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp.ok) return null
    const rows = await resp.json().catch(() => [])
    const row = Array.isArray(rows) && rows.length ? rows[0] : null
    const bid = safeInt(row && typeof row === "object" ? (row as any).boss_id : 0, 0)
    const started = row && typeof row === "object" ? String((row as any).fight_started_at || "").trim() : ""
    if (bid <= 0) return null
    return { boss_id: bid, fight_started_at: started }
  } catch (_e) {
    return null
  }
}

async function postgrestGetPlayer(projectUrl: string, serviceKey: string, tgId: string): Promise<any | null> {
  const url = projectUrl.replace(/\/$/, "") +
    `/rest/v1/players?tg_id=eq.${encodeURIComponent(tgId)}&select=tg_id,name,state&limit=1`
  const resp = await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
  })
  if (!resp.ok) return null
  const rows = await resp.json().catch(() => [])
  const row = Array.isArray(rows) && rows.length ? rows[0] : null
  return row && typeof row === "object" ? row : null
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
  // players table does not have a name_lower column; use case-insensitive match on name.
  // NOTE: no wildcards; exact match but case-insensitive.
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
    // state is jsonb; historically we stored clan id as state.clan.id on the client.
    // PostgREST json contains (cs) is not always reliable for nested objects across versions,
    // so we try cs-filter first, and if it returns empty fall back to json-path operator.

    // Primary attempt: json contains: {"clan":{"id":"CLN..."}}
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

    // Fallback: json-path filter. IMPORTANT: clan id in state might have inconsistent casing,
    // so use case-insensitive match.
    const url2 = projectUrl.replace(/\/$/, "") +
      `/rest/v1/players?state->clan->>id=ilike.${encodeURIComponent(clanId)}&select=tg_id,name&limit=200`
    const resp2 = await fetch(url2, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    const rows2 = resp2.ok ? await resp2.json().catch(() => []) : []
    const out2 = Array.isArray(rows2) ? rows2 : []
    if (out2.length) return out2

    // Extra fallback: if PostgREST treats ilike as pattern, try lowercase variant.
    const url3 = projectUrl.replace(/\/$/, "") +
      `/rest/v1/players?state->clan->>id=ilike.${encodeURIComponent(String(clanId || "").toLowerCase())}&select=tg_id,name&limit=200`
    const resp3 = await fetch(url3, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp3.ok) return []
    const rows3 = await resp3.json().catch(() => [])
    return Array.isArray(rows3) ? rows3 : []
  } catch (_e) {
    return []
  }
}

async function postgrestInsertEvents(
  projectUrl: string,
  serviceKey: string,
  rows: any[],
): Promise<Response> {
  const url = projectUrl.replace(/\/$/, "") + "/rest/v1/boss_help_events"
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

async function postgrestGetUsedHelpDamage(
  projectUrl: string,
  serviceKey: string,
  toTgId: string,
  fromTgId: string,
  bossId: number,
  sinceIso: string,
): Promise<number> {
  try {
    const url = projectUrl.replace(/\/$/, "") +
      `/rest/v1/boss_help_events?to_tg_id=eq.${encodeURIComponent(toTgId)}` +
      `&from_tg_id=eq.${encodeURIComponent(fromTgId)}` +
      `&boss_id=eq.${encodeURIComponent(String(bossId))}` +
      `&created_at=gte.${encodeURIComponent(sinceIso)}` +
      `&select=dmg&limit=1000`
    const resp = await fetch(url, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp.ok) return 0
    const rows = await resp.json().catch(() => [])
    if (!Array.isArray(rows) || !rows.length) return 0
    let sum = 0
    for (const r of rows) {
      const v = safeInt((r as any)?.dmg, 0)
      if (v > 0) sum += v
    }
    return Math.max(0, Math.floor(sum))
  } catch (_e) {
    return 0
  }
}

async function postgrestApplyBossDamage(
  projectUrl: string,
  serviceKey: string,
  ownerTgId: string,
  bossId: number,
  dmg: number,
  maxHp: number,
  expiresAt: string,
  source?: string,
  fromTgId?: string,
  fromName?: string,
  clanId?: string,
): Promise<{ ok: boolean; status?: number; statusText?: string; body?: string }> {
  try {
    const url = projectUrl.replace(/\/$/, "") + "/rest/v1/rpc/apply_boss_damage_v2"
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
      },
      body: JSON.stringify({
        p_owner_tg_id: ownerTgId,
        p_boss_id: bossId,
        p_dmg: dmg,
        p_max_hp: maxHp,
        p_expires_at: expiresAt,
        p_source: source || "hit",
        p_from_tg_id: fromTgId || null,
        p_from_name: fromName || null,
        p_clan_id: clanId || null,
      }),
    })
    const text = await resp.text().catch(() => "")
    return {
      ok: resp.ok,
      status: resp.status,
      statusText: resp.statusText,
      body: text ? text.slice(0, 900) : "",
    }
  } catch (_e) {
    return { ok: false, status: 0, statusText: "fetch_error", body: "" }
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

  const senderTgId = String(verified.user.id)
  const bossId = Math.max(1, safeInt(body?.boss_id ?? body?.bossId, 0))
  const dmg = Math.max(0, Math.min(1_000_000_000, safeInt(body?.dmg, 0)))
  let clanId = sanitizeClanId(body?.clan_id ?? body?.clanId)
  const fromName = sanitizeName(body?.from_name ?? body?.from ?? body?.member_name ?? body?.name)

  const def = bossDef(bossId)
  if (!def) {
    return new Response(JSON.stringify({ ok: false, error: "bad_boss_id" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  if (!bossId) {
    return new Response(JSON.stringify({ ok: false, error: "bad_boss_id" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }
  if (!dmg) {
    return new Response(JSON.stringify({ ok: true, inserted: 0 }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const senderRow = await postgrestGetPlayer(projectUrl, serviceKey, senderTgId)
  if (!senderRow) {
    return new Response(JSON.stringify({ ok: false, error: "sender_not_found" }), {
      status: 404,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  // If client did not send clan_id (or sent invalid), infer it from sender stored state.
  if (!clanId) {
    try {
      const st = senderRow?.state
      let cid = ""
      if (st && typeof st === "object") {
        // Preferred: state.clan.id
        try { cid = String((st as any)?.clan?.id || "").trim() } catch (_e0) {}

        // Some clients store clan id flat
        if (!cid) {
          try { cid = String((st as any)?.clanId || "").trim() } catch (_e1) {}
        }
        if (!cid) {
          try { cid = String((st as any)?.clan_id || "").trim() } catch (_e2) {}
        }

        // Some clients store state.clan as string or as {id:...}
        if (!cid) {
          try {
            const c = (st as any)?.clan
            if (typeof c === "string") cid = String(c || "").trim()
            else if (c && typeof c === "object") cid = String((c as any).id || "").trim()
          } catch (_e3) {}
        }
      }
      clanId = sanitizeClanId(String(cid || "").trim().toUpperCase())
    } catch (_e) {
      // ignore
    }
  }

  const senderName = String(senderRow?.name || fromName || "Player").trim().slice(0, 18) || "Player"
  const senderNameLower = senderName.toLowerCase()

  const recipients = new Set<string>()

  // friends recipients from sender state
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
          if (!tid2) continue
          if (tid2 === senderTgId) continue
          recipients.add(tid2)
          resolvedByName += 1
        } catch (_e0) {}
        continue
      }

      const tid = String((it && typeof it === "object" ? ((it as any).tg_id || (it as any).tgId) : "") || "").trim()
      if (tid) {
        if (tid === senderTgId) continue
        recipients.add(tid)
        continue
      }

      // Fallback: some clients store friends without tg_id (only name/online).
      // Resolve friend tg_id by name to avoid losing help delivery.
      const nmRaw = String(it?.name || "").trim()
      const nm = nmRaw.slice(0, 18)
      if (!nm) continue
      const nl = nm.toLowerCase()
      if (nl === senderNameLower) continue
      if (resolvedByName >= 20) continue
      try {
        const pr = await postgrestFindPlayerByNameLower(projectUrl, serviceKey, nl)
        const tid2 = String(pr?.tg_id || "").trim()
        if (!tid2) continue
        if (tid2 === senderTgId) continue
        recipients.add(tid2)
        resolvedByName += 1
      } catch (_e0) {
        // ignore
      }
    }
  } catch (_) {}

  // clan recipients by clan members names -> resolve to tg_id
  if (clanId) {
    // Primary (reliable): list by clan id from player state
    const rows = await postgrestListPlayersByClanId(projectUrl, serviceKey, clanId)
    for (const r of rows) {
      const tid = String((r as any)?.tg_id || "").trim()
      if (!tid) continue
      if (tid === senderTgId) continue
      recipients.add(tid)
    }

    // Fallback (best-effort): resolve by names stored in clan row
    try {
      const clanRow = await postgrestGetClan(projectUrl, serviceKey, clanId)
      const clanObj = (clanRow && typeof clanRow === "object") ? (clanRow as any).data : null
      const c = clanObj && typeof clanObj === "object" ? clanObj : {}
      const mem = Array.isArray((c as any).members) ? (c as any).members : []
      const leaderRaw = (c as any).leader
      const deputyRaw = (c as any).deputy
      const leader = (leaderRaw && typeof leaderRaw === "object") ? String((leaderRaw as any).name || "").trim() : String(leaderRaw || "").trim()
      const deputy = (deputyRaw && typeof deputyRaw === "object") ? String((deputyRaw as any).name || "").trim() : String(deputyRaw || "").trim()

      // If clan row has owner_tg_id, always include it.
      try {
        const ownerTid = String((clanRow as any)?.owner_tg_id || "").trim()
        if (ownerTid && ownerTid !== senderTgId) recipients.add(ownerTid)
      } catch (_e0) {}

      const names: string[] = []
      if (leader) names.push(leader)
      if (deputy) names.push(deputy)
      for (const m of mem) {
        if (!m) continue
        if (typeof m === "string") {
          names.push(String(m || "").trim())
          continue
        }
        if (typeof m === "object") {
          const tid = String((m as any).tg_id || (m as any).tgId || "").trim()
          if (tid) {
            if (tid !== senderTgId) recipients.add(tid)
          }
          const nm = String((m as any).name || (m as any).nm || "").trim()
          if (nm) names.push(nm)
          continue
        }
        names.push(String(m).trim())
      }

      const uniq = new Set<string>()
      for (const n of names) {
        const nn = String(n || "").trim()
        if (!nn) continue
        const nl = nn.toLowerCase()
        if (nl === senderNameLower) continue
        uniq.add(nl)
        if (uniq.size >= 25) break
      }

      for (const nl of uniq) {
        const pr = await postgrestFindPlayerByNameLower(projectUrl, serviceKey, nl)
        const tid = String(pr?.tg_id || "").trim()
        if (!tid) continue
        if (tid === senderTgId) continue
        recipients.add(tid)
      }
    } catch (_) {}
  }

  const recArr = Array.from(recipients)
  if (!recArr.length) {
    return new Response(JSON.stringify({
      ok: true,
      inserted: 0,
      debug: {
        reason: "no_recipients",
        recipients: 0,
        boss_id: bossId,
        clan_id: clanId || null,
        from_name: senderName,
      },
    }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  let clanLvl = 1
  if (clanId) {
    try {
      const clanRow = await postgrestGetClan(projectUrl, serviceKey, clanId)
      const clanObj = (clanRow && typeof clanRow === "object") ? (clanRow as any).data : null
      const c = clanObj && typeof clanObj === "object" ? clanObj : {}
      clanLvl = clampInt(safeInt((c as any).lvl, 1), 1, 10)
    } catch (_e) {
      clanLvl = 1
    }
  }

  const ts = new Date().toISOString()
  const expiresAt = new Date(Date.now() + 8 * 60 * 60 * 1000).toISOString()
  const windowStartIso = new Date(Date.now() - 8 * 60 * 60 * 1000).toISOString()

  let rpcOkCount = 0
  let rpcFailCount = 0
  let rpcFirstErr: any = null
  let skippedCapCount = 0
  let skippedOtherCount = 0
  let skippedNoActiveCount = 0

  // Apply damage server-side immediately so recipient boss HP updates even if offline
  // Best-effort: failure here should not block logging events.
  const rows0 = await Promise.all(
    recArr.map(async (toTgId) => {
      let applied = 0
      let targetBossId = bossId
      let targetDef = def
      try {
        const pr = await postgrestGetPlayer(projectUrl, serviceKey, String(toTgId))
        const st = pr && typeof pr === "object" ? (pr as any).state : null

        // New rule: help is only allowed from recipient's bros list.
        // Bros list = recipient friends + clanmates.
        if (!recipientAllowsSenderHelp(st, senderTgId, senderNameLower, clanId || "")) {
          skippedOtherCount += 1
          return null
        }

        // used-help scope: for clan help we reset the per-sender cap when recipient starts a new fight.
        // We do that by counting used help since the active fight's fight_started_at (stable anchor).
        let usedSinceIso = windowStartIso
        let hasActiveFight = false

        try {
          const abm = await postgrestFindActiveBossFightMeta(projectUrl, serviceKey, String(toTgId))
          const ab = abm && abm.boss_id ? abm.boss_id : 0
          const dd = ab ? bossDef(ab) : null
          if (dd) {
            targetBossId = ab
            targetDef = dd
            if (abm && abm.fight_started_at) usedSinceIso = String(abm.fight_started_at)
            hasActiveFight = true
          }
        } catch (_e1) {
          // ignore
        }

        // Requirement: help should be delivered ONLY to recipients who already have an active boss fight.
        // Do NOT start a new fight via apply_boss_damage_v2.
        if (!hasActiveFight) {
          skippedNoActiveCount += 1
          return null
        }

        if (clanId) {
          const lvlRaw = st && typeof st === "object" ? (st as any).friendHelpLvl : 0
          const lvl = clampInt(safeInt(lvlRaw, 0), 0, 10)
          const cap = (Number(targetBossId) === 1)
            ? 500
            : Math.max(0, Math.floor(targetDef.max_hp * (0.05 + lvl * 0.005)))
          const used = await postgrestGetUsedHelpDamage(projectUrl, serviceKey, String(toTgId), senderTgId, targetBossId, usedSinceIso)
          const remain = Math.max(0, cap - Math.max(0, used))
          applied = Math.max(0, Math.min(dmg, remain))
        } else {
          const lvlRaw = st && typeof st === "object" ? (st as any).friendHelpLvl : 0
          const lvl = clampInt(safeInt(lvlRaw, 0), 0, 10)
          const cap = (Number(targetBossId) === 1)
            ? 500
            : Math.max(0, Math.floor(targetDef.max_hp * (0.05 + lvl * 0.005)))
          applied = Math.max(0, Math.min(dmg, cap))
        }
      } catch (_e) {
        const cap = (Number(targetBossId) === 1)
          ? 500
          : Math.max(0, Math.floor(targetDef.max_hp * 0.05))
        if (clanId) {
          const used = await postgrestGetUsedHelpDamage(projectUrl, serviceKey, String(toTgId), senderTgId, targetBossId, windowStartIso)
          const remain = Math.max(0, cap - Math.max(0, used))
          applied = Math.max(0, Math.min(dmg, remain))
        } else {
          applied = Math.max(0, Math.min(dmg, cap))
        }
      }

      try {
        if (applied > 0) {
          const r = await postgrestApplyBossDamage(
            projectUrl,
            serviceKey,
            String(toTgId),
            targetBossId,
            applied,
            targetDef.max_hp,
            expiresAt,
            "help",
            senderTgId,
            senderName,
            clanId || undefined,
          )
          if (r && r.ok) {
            rpcOkCount += 1
          } else {
            rpcFailCount += 1
            if (!rpcFirstErr) {
              rpcFirstErr = {
                step: "rpc_apply_boss_damage_v2",
                to_tg_id: String(toTgId),
                boss_id: targetBossId,
                status: r?.status,
                status_text: r?.statusText,
                body: r?.body,
              }
            }
            applied = 0
          }
        }
      } catch (_e2) {
        rpcFailCount += 1
        if (!rpcFirstErr) {
          rpcFirstErr = {
            step: "rpc_apply_boss_damage_v2",
            to_tg_id: String(toTgId),
            status: 0,
            status_text: "exception",
            body: "",
          }
        }
        applied = 0
      }

      if (applied > 0) {
        return {
          to_tg_id: String(toTgId),
          from_tg_id: String(senderTgId),
          boss_id: targetBossId,
          dmg: applied,
          clan_id: clanId || null,
          from_name: senderName,
          created_at: ts,
        }
      } else {
        if (dmg > 0) {
          // Best-effort classify 0-applied.
          // If we didn't even attempt RPC (applied computed as 0) => cap reached.
          // If RPC failed, it's counted separately above.
          if (rpcFailCount > 0) skippedOtherCount += 1
          else skippedCapCount += 1
        }
        return null
      }
    }),
  )

  const rows = (Array.isArray(rows0) ? rows0 : []).filter((x) => !!x)
  if (!rows.length) {
    const reason = (rpcFailCount > 0)
      ? "rpc_failed"
      : (skippedCapCount > 0 ? "cap_reached" : "no_effect")
    return new Response(JSON.stringify({
      ok: true,
      inserted: 0,
      debug: {
        reason,
        recipients: recArr.length,
        skipped_no_active: skippedNoActiveCount,
        skipped_cap: skippedCapCount,
        skipped_other: skippedOtherCount,
        rpc_ok: rpcOkCount,
        rpc_fail: rpcFailCount,
        rpc_first_err: rpcFirstErr,
      },
    }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

const ins = await postgrestInsertEvents(projectUrl, serviceKey, rows)
if (!ins.ok) {
  const text = await ins.text().catch(() => "")
  return new Response(
    JSON.stringify({
      ok: false,
      error: "supabase_error",
      details: text.slice(0, 1500),
      debug: {
        step: "insert_boss_help_events",
        status: ins.status,
        status_text: ins.statusText,
        recipients: recArr.length,
        rows: rows.length,
        sample_to: rows && rows.length ? String((rows[0] as any)?.to_tg_id || "") : "",
        sample_boss_id: rows && rows.length ? Number((rows[0] as any)?.boss_id || 0) : 0,
      },
    }),
    {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    },
  )
}

return new Response(
  JSON.stringify({
    ok: true,
    inserted: rows.length,
    debug: {
      recipients: recArr.length,
      skipped_no_active: skippedNoActiveCount,
      rpc_ok: rpcOkCount,
      rpc_fail: rpcFailCount,
      rpc_first_err: rpcFirstErr,
    },
  }),
  {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  },
)
})
