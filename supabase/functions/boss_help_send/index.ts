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

function sanitizeClanId(id: unknown): string {
  const s = String(id || "").trim().toUpperCase()
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

function safeInt(v: unknown, def = 0): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return def
  return Math.floor(n)
}

function clampInt(n: number, lo: number, hi: number): number {
  if (!Number.isFinite(n)) return lo
  return Math.max(lo, Math.min(hi, Math.floor(n)))
}

function bossDef(bossId: number): { boss_id: number; max_hp: number } | null {
  const defs: Record<number, number> = {
    1: 1000,
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
    `/rest/v1/clans?id=eq.${encodeURIComponent(clanId)}&select=id,data&limit=1`
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
  const url = projectUrl.replace(/\/$/, "") +
    `/rest/v1/players?name_lower=eq.${encodeURIComponent(nameLower)}&select=tg_id,name&limit=1`
  const resp = await fetch(url, {
    method: "GET",
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
  })
  if (!resp.ok) return null
  const rows = await resp.json().catch(() => [])
  const row = Array.isArray(rows) && rows.length ? rows[0] : null
  return row && typeof row === "object" ? row : null
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

    // Fallback: json-path filter: state->clan->>id = 'CLN...'
    const url2 = projectUrl.replace(/\/$/, "") +
      `/rest/v1/players?state->clan->>id=eq.${encodeURIComponent(clanId)}&select=tg_id,name&limit=200`
    const resp2 = await fetch(url2, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp2.ok) return []
    const rows2 = await resp2.json().catch(() => [])
    return Array.isArray(rows2) ? rows2 : []
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
): Promise<boolean> {
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
      }),
    })
    return resp.ok
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

  const senderTgId = String(verified.user.id)
  const bossId = Math.max(1, safeInt(body?.boss_id ?? body?.bossId, 0))
  const dmg = Math.max(0, Math.min(1_000_000_000, safeInt(body?.dmg, 0)))
  const clanId = sanitizeClanId(body?.clan_id ?? body?.clanId)
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

  const senderName = String(senderRow?.name || fromName || "Player").trim().slice(0, 18) || "Player"
  const senderNameLower = senderName.toLowerCase()

  const recipients = new Set<string>()

  // friends recipients from sender state
  try {
    const st = senderRow?.state
    const fr = st && typeof st === "object" ? (st as any).friends : null
    const list: any[] = Array.isArray(fr) ? fr : []
    for (const it of list.slice(0, 80)) {
      const tid = String(it?.tg_id || "").trim()
      if (!tid) continue
      if (tid === senderTgId) continue
      recipients.add(tid)
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
      const leader = String((c as any).leader || "").trim()
      const deputy = String((c as any).deputy || "").trim()

      const names: string[] = []
      if (leader) names.push(leader)
      if (deputy) names.push(deputy)
      for (const m of mem) names.push(String(m || "").trim())

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
    return new Response(JSON.stringify({ ok: true, inserted: 0 }), {
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

  // Apply damage server-side immediately so recipient boss HP updates even if offline
  // Best-effort: failure here should not block logging events.
  const rows = await Promise.all(
    recArr.map(async (toTgId) => {
      let applied = 0
      try {
        const pr = await postgrestGetPlayer(projectUrl, serviceKey, String(toTgId))
        const st = pr && typeof pr === "object" ? (pr as any).state : null
        if (clanId) {
          const lvlRaw = st && typeof st === "object" ? (st as any).friendHelpLvl : 0
          const lvl = clampInt(safeInt(lvlRaw, 0), 0, 10)
          const capPct = 0.10 + lvl * 0.01
          const cap = Math.max(0, Math.floor(def.max_hp * capPct))
          const used = await postgrestGetUsedHelpDamage(projectUrl, serviceKey, String(toTgId), senderTgId, bossId, windowStartIso)
          const remain = Math.max(0, cap - Math.max(0, used))
          applied = Math.max(0, Math.min(dmg, remain))
        } else {
          const lvlRaw = st && typeof st === "object" ? (st as any).friendHelpLvl : 0
          const lvl = clampInt(safeInt(lvlRaw, 0), 0, 10)
          const capPct = 0.10 + lvl * 0.01
          const cap = Math.max(0, Math.floor(def.max_hp * capPct))
          applied = Math.max(0, Math.min(dmg, cap))
        }
      } catch (_e) {
        const capPct = 0.10
        const cap = Math.max(0, Math.floor(def.max_hp * capPct))
        if (clanId) {
          const used = await postgrestGetUsedHelpDamage(projectUrl, serviceKey, String(toTgId), senderTgId, bossId, windowStartIso)
          const remain = Math.max(0, cap - Math.max(0, used))
          applied = Math.max(0, Math.min(dmg, remain))
        } else {
          applied = Math.max(0, Math.min(dmg, cap))
        }
      }

      try {
        if (applied > 0) {
          await postgrestApplyBossDamage(projectUrl, serviceKey, String(toTgId), bossId, applied, def.max_hp, expiresAt)
        }
      } catch (_e2) {
        // ignore
      }

      return {
        to_tg_id: toTgId,
        from_tg_id: senderTgId,
        from_name: senderName,
        boss_id: bossId,
        dmg: applied,
        clan_id: clanId || null,
        created_at: ts,
        consumed: false,
      }
    }),
  )

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

  return new Response(JSON.stringify({ ok: true, inserted: rows.length, debug: { recipients: recArr.length } }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})
