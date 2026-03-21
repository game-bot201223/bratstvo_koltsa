// Follow this setup guide to integrate the Deno language server with your editor:
// https://deno.land/manual/getting_started/setup_your_environment
// This enables autocomplete, go to definition, etc.

// Setup type definitions for built-in Supabase Runtime APIs
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
  // Telegram WebApp validation: secret_key = HMAC_SHA256("WebAppData", bot_token)
  // (Note: login widget uses SHA256(bot_token), but WebApp initData uses this HMAC derivation.)
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

async function postgrestUpsertPlayer(projectUrl: string, serviceKey: string, payload: Record<string, unknown>): Promise<Response> {
  const url = projectUrl.replace(/\/$/, "") + "/rest/v1/players?on_conflict=tg_id"
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

async function postgrestGetPlayerState(projectUrl: string, serviceKey: string, tgId: string): Promise<any | null> {
  const url = projectUrl.replace(/\/$/, "") + `/rest/v1/players?tg_id=eq.${encodeURIComponent(tgId)}&select=state&limit=1`
  const resp = await fetch(url, {
    method: "GET",
    headers: {
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
    },
  })
  if (!resp.ok) return null
  const rows = await resp.json().catch(() => [])
  const row = Array.isArray(rows) && rows.length ? rows[0] : null
  return row && typeof row === "object" ? (row as any).state : null
}

function deepMergeState(base: any, patch: any): any {
  if (!patch || typeof patch !== "object" || Array.isArray(patch)) return patch
  const out: any = (base && typeof base === "object" && !Array.isArray(base)) ? { ...base } : {}
  for (const k of Object.keys(patch)) {
    const pv = (patch as any)[k]
    if (typeof pv === "undefined") continue
    const bv = out[k]
    if (pv && typeof pv === "object" && !Array.isArray(pv)) {
      out[k] = deepMergeState(bv, pv)
    } else {
      out[k] = pv
    }
  }
  return out
}

function deriveDistrictFearTotal(state: any): number {
  try {
    const src = state && typeof state === "object" ? (state as any).districtFear : null
    if (!src || typeof src !== "object") return 0
    let total = 0
    for (const k of Object.keys(src)) {
      const v = safeNonNegInt((src as any)[k])
      total += v
      if (total > 2147483647) return 2147483647
    }
    return total
  } catch (_e) {
    return 0
  }
}

function derivePersistedMetrics(body: any, state: any): { xp: number; gold: number; tooth: number; districtFearTotal: number } {
  const src = state && typeof state === "object" ? state : {}
  return {
    xp: safeNonNegInt(body?.xp ?? (body as any)?.total_xp ?? (body as any)?.totalXp ?? (src as any).totalXp ?? (src as any).xp),
    gold: safeNonNegInt(body?.gold ?? (src as any).gold),
    tooth: safeNonNegInt(body?.tooth ?? (src as any).tooth),
    districtFearTotal: safeNonNegInt(body?.district_fear_total ?? (body as any)?.districtFearTotal ?? deriveDistrictFearTotal(src)),
  }
}

async function postgrestGetPlayerSession(projectUrl: string, serviceKey: string, tgId: string): Promise<{ sid: string; updatedAt: string; deviceId: string }> {
  try {
    const url = projectUrl.replace(/\/$/, "") +
      `/rest/v1/players?tg_id=eq.${encodeURIComponent(tgId)}&select=active_session_id,active_session_updated_at,active_device_id&limit=1`
    const resp = await fetch(url, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp.ok) return { sid: "", updatedAt: "", deviceId: "" }
    const rows = await resp.json().catch(() => [])
    const row = Array.isArray(rows) && rows.length ? rows[0] : null
    const sid = row && typeof row === "object" ? String((row as any).active_session_id || "").trim() : ""
    const updatedAt = row && typeof row === "object" ? String((row as any).active_session_updated_at || "").trim() : ""
    const deviceId = row && typeof row === "object" ? String((row as any).active_device_id || "").trim() : ""
    return { sid, updatedAt, deviceId }
  } catch (_e) {
    return { sid: "", updatedAt: "", deviceId: "" }
  }
}

async function postgrestSetPlayerSession(projectUrl: string, serviceKey: string, tgId: string, sessionId: string): Promise<void> {
  try {
    if (!sessionId) return
    const url = projectUrl.replace(/\/$/, "") + `/rest/v1/players?tg_id=eq.${encodeURIComponent(tgId)}`
    await fetch(url, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        Prefer: "return=minimal",
      },
      body: JSON.stringify({ active_session_id: sessionId, active_session_updated_at: new Date().toISOString(), updated_at: new Date().toISOString() }),
    }).catch(() => {})
  } catch (_e) {}
}

async function postgrestGetPlayerName(projectUrl: string, serviceKey: string, tgId: string): Promise<string> {
  try {
    const url = projectUrl.replace(/\/$/, "") +
      `/rest/v1/players?tg_id=eq.${encodeURIComponent(tgId)}&select=name&limit=1`
    const resp = await fetch(url, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp.ok) return ""
    const rows = await resp.json().catch(() => [])
    const row = Array.isArray(rows) && rows.length ? rows[0] : null
    const nm = row && typeof row === "object" ? String((row as any).name || "").trim() : ""
    return nm.slice(0, 18)
  } catch (_e) {
    return ""
  }
}

async function postgrestGetClan(projectUrl: string, serviceKey: string, clanId: string): Promise<any | null> {
  try {
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
  } catch (_e) {
    return null
  }
}

function sanitizeClanId(s: unknown): string {
  const t = String(s || "").trim().toUpperCase().slice(0, 24)
  if (!/^CLN\d{1,20}$/.test(t)) return ""
  return t
}

function clanDataRename(data: any, prevN: string, nextN: string): { data: any; touched: boolean } {
  try {
    if (!data || typeof data !== "object") return { data, touched: false }
    let touched = false
    // leader / deputy
    try {
      if (normName((data as any).leader) === prevN) { (data as any).leader = nextN; touched = true }
      if (normName((data as any).deputy) === prevN) { (data as any).deputy = nextN; touched = true }
    } catch (_e) {}

    // members/apps arrays
    try {
      const m0 = Array.isArray((data as any).members) ? (data as any).members : []
      const a0 = Array.isArray((data as any).apps) ? (data as any).apps : []
      const m1 = m0.map((x: any) => (normName(x) === prevN ? nextN : x))
      const a1 = a0.map((x: any) => (normName(x) === prevN ? nextN : x))
      const uniq = (arr: any[]) => {
        const seen = new Set<string>()
        const out: any[] = []
        for (const it of arr) {
          const k = normName(it)
          if (!k) continue
          if (seen.has(k)) continue
          seen.add(k)
          out.push(it)
        }
        return out
      }
      ;(data as any).members = uniq(m1)
      ;(data as any).apps = uniq(a1)
      if (m0.some((x: any) => normName(x) === prevN) || a0.some((x: any) => normName(x) === prevN)) touched = true
    } catch (_e) {}
    return { data, touched }
  } catch (_e) {
    return { data, touched: false }
  }
}

async function postgrestListClans(projectUrl: string, serviceKey: string, limit: number): Promise<any[]> {
  try {
    const lim = Math.max(1, Math.min(300, Math.floor(limit || 200)))
    const url = projectUrl.replace(/\/$/, "") +
      `/rest/v1/clans?select=id,data,updated_at&order=updated_at.desc&limit=${lim}`
    const resp = await fetch(url, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp.ok) return []
    const rows = await resp.json().catch(() => [])
    return Array.isArray(rows) ? rows : []
  } catch (_e) {
    return []
  }
}

async function postgrestPatchClanData(projectUrl: string, serviceKey: string, clanId: string, data: any): Promise<boolean> {
  try {
    const url = projectUrl.replace(/\/$/, "") + `/rest/v1/clans?id=eq.${encodeURIComponent(clanId)}`
    const resp = await fetch(url, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        Prefer: "return=minimal",
      },
      body: JSON.stringify({ data, updated_at: new Date().toISOString() }),
    })
    return resp.ok
  } catch (_e) {
    return false
  }
}

function normName(name: unknown): string {
  return String(name || "").trim().toUpperCase()
}

async function postgrestUpdateBossLastWinnerName(projectUrl: string, serviceKey: string, tgId: string, name: string, photoUrl: string): Promise<void> {
  try {
    const url = projectUrl.replace(/\/$/, "") + `/rest/v1/boss_last_winners?tg_id=eq.${encodeURIComponent(tgId)}`
    await fetch(url, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        Prefer: "return=minimal",
      },
      body: JSON.stringify({ name, photo_url: photoUrl, updated_at: new Date().toISOString() }),
    }).catch(() => {})
  } catch (_e) {}
}

async function postgrestUpdateDistrictLeaderName(projectUrl: string, serviceKey: string, tgId: string, name: string, photoUrl: string): Promise<void> {
  try {
    const url = projectUrl.replace(/\/$/, "") + `/rest/v1/district_leaders?tg_id=eq.${encodeURIComponent(tgId)}`
    await fetch(url, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        Prefer: "return=minimal",
      },
      body: JSON.stringify({ name, photo_url: photoUrl, updated_at: new Date().toISOString() }),
    }).catch(() => {})
  } catch (_e) {}
}

async function bestEffortPropagateRename(
  projectUrl: string,
  serviceKey: string,
  tgId: string,
  prevName: string,
  nextName: string,
  photoUrl: string,
): Promise<void> {
  try {
    const prevN = normName(prevName)
    const nextN = String(nextName || "").trim().slice(0, 18)
    if (!prevN || !nextN) return

    // First: patch the player's current clan directly (authoritative).
    try {
      const st = await postgrestGetPlayerState(projectUrl, serviceKey, tgId)
      const cid = st && typeof st === "object" ? sanitizeClanId((st as any)?.clan?.id) : ""
      if (cid) {
        const clanRow = await postgrestGetClan(projectUrl, serviceKey, cid)
        const data0 = clanRow && typeof clanRow === "object" ? (clanRow as any).data : null
        if (data0 && typeof data0 === "object") {
          const rr = clanDataRename(data0, prevN, nextN)
          if (rr.touched) {
            await postgrestPatchClanData(projectUrl, serviceKey, cid, rr.data).catch(() => false)
          }
        }
      }
    } catch (_e) {}

    // Update clans data (best-effort, recent N clans).
    try {
      const clans = await postgrestListClans(projectUrl, serviceKey, 300)
      for (const row of clans) {
        const id = String((row as any)?.id || "").trim()
        const data = (row as any)?.data
        if (!id || !data || typeof data !== "object") continue

        const rr = clanDataRename(data, prevN, nextN)
        if (rr.touched) await postgrestPatchClanData(projectUrl, serviceKey, id, rr.data).catch(() => false)
      }
    } catch (_e) {}

    // Update other tables that store name.
    try { await postgrestUpdateBossLastWinnerName(projectUrl, serviceKey, tgId, nextN, photoUrl) } catch (_e) {}
    try { await postgrestUpdateDistrictLeaderName(projectUrl, serviceKey, tgId, nextN, photoUrl) } catch (_e) {}
  } catch (_e) {}
}

async function postgrestFindNameOwner(projectUrl: string, serviceKey: string, name: string): Promise<string> {
  try {
    const nm = sanitizeName(name)
    if (!nm) return ""
    const nmNorm = normName(nm)
    const likeExact = escapeLikeExact(nm)
    const url = projectUrl.replace(/\/$/, "") +
      `/rest/v1/players?select=tg_id,name&name=ilike.${encodeURIComponent(likeExact)}&limit=5`
    const resp = await fetch(url, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp.ok) return ""
    const rows = await resp.json().catch(() => [])
    const arr = Array.isArray(rows) ? rows : []
    for (const row of arr) {
      if (!row || typeof row !== "object") continue
      const n2 = String((row as any).name || "").trim()
      if (!n2) continue
      if (normName(n2) !== nmNorm) continue
      const tid = String((row as any).tg_id || "").trim()
      if (tid) return tid
    }
    return ""
  } catch (_e) {
    return ""
  }
}

async function creditReferrerGold(projectUrl: string, serviceKey: string, refTgId: string, amount: number): Promise<boolean> {
  const st = await postgrestGetPlayerState(projectUrl, serviceKey, refTgId)
  const stateObj = (st && typeof st === "object") ? st as Record<string, unknown> : {}
  const curGold = Number((stateObj as any).gold)
  const nextGold = (Number.isFinite(curGold) ? curGold : 0) + amount
  ;(stateObj as any).gold = Math.floor(nextGold)
  ;(stateObj as any)._invBonusTs = new Date().toISOString()

  const resp = await postgrestUpsertPlayer(projectUrl, serviceKey, {
    tg_id: refTgId,
    state: stateObj,
    updated_at: new Date().toISOString(),
  })
  return resp.ok
}

async function addFriendToReferrer(
  projectUrl: string,
  serviceKey: string,
  refTgId: string,
  friend: { tg_id: string; name: string; lvl: number; power: number },
): Promise<boolean> {
  const st = await postgrestGetPlayerState(projectUrl, serviceKey, refTgId)
  const stateObj = (st && typeof st === "object") ? st as Record<string, unknown> : {}
  const fr0 = (stateObj as any).friends
  const list: any[] = Array.isArray(fr0) ? fr0.slice() : []

  const fid = String(friend.tg_id || "").trim()
  const nm = String(friend.name || "Player").trim().slice(0, 18) || "Player"
  const lvl = Math.max(1, safeNonNegInt(friend.lvl))
  const power = safeNonNegInt(friend.power)

  // idempotent: replace by tg_id if exists; else add
  let replaced = false
  for (let i = 0; i < list.length; i++) {
    const it = list[i]
    if (it && typeof it === "object" && String((it as any).tg_id || "") === fid) {
      list[i] = { ...(it as any), tg_id: fid, name: nm, lvl, power, online: true, updated_at: new Date().toISOString() }
      replaced = true
      break
    }
  }
  if (!replaced) {
    list.push({ tg_id: fid, name: nm, lvl, power, online: true, updated_at: new Date().toISOString() })
  }

  ;(stateObj as any).friends = list.slice(-200)

  const resp = await postgrestUpsertPlayer(projectUrl, serviceKey, {
    tg_id: refTgId,
    state: stateObj,
    updated_at: new Date().toISOString(),
  })
  return resp.ok
}

function safeNonNegInt(v: unknown, def = 0): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return def
  if (n < 0) return def
  return Math.floor(n)
}

function sanitizeName(s: unknown): string {
  return String(s || "").trim().slice(0, 18)
}

function escapeLikeExact(s: string): string {
  // Escape LIKE wildcards so user names containing %/_ won't match other rows.
  return String(s || "").replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_")
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

function isJwtLike(s: string): boolean {
  try {
    const t = String(s || "").trim()
    // Supabase anon/service_role keys are JWT-like strings starting with 'eyJ' and containing 2 dots.
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

  const actorTgId = String(verified.user.id)

  // Admin mode: allow updating arbitrary player state by tg_id.
  // Uses SERVICE_ROLE_KEY so it works even when RLS is enabled on players.
  try {
    const adminIdsRaw = String(Deno.env.get("ADMIN_TG_IDS") || Deno.env.get("ADMIN_TG_ID") || "8794843839").trim()
    const adminIds = adminIdsRaw.split(/[\s,;]+/g).map((x) => x.trim()).filter((x) => x)
    const isAdmin = adminIds.includes(actorTgId)
    const targetTgId = String(body?.target_tg_id ?? (body as any)?.targetTgId ?? "").trim()
    const hasState = !!(body && typeof body === "object" && (body as any).state && typeof (body as any).state === "object")
    if (isAdmin && targetTgId && hasState) {
      const state0: unknown = (body as any).state
      try {
        const size = JSON.stringify(state0).length
        if (size > 420000) {
          return new Response(JSON.stringify({ ok: false, error: "state_too_large", size }), {
            status: 413,
            headers: { ...corsHeaders, "Content-Type": "application/json" },
          })
        }
      } catch (_eSize) {}

      // Merge partial admin patch with existing state to avoid progress resets.
      let mergedState: unknown = state0
      try {
        const prevState = await postgrestGetPlayerState(projectUrl, serviceKey, targetTgId).catch(() => null)
        if (prevState && typeof prevState === "object" && state0 && typeof state0 === "object") {
          mergedState = deepMergeState(prevState, state0)
        }
      } catch (_eMerge) {
        mergedState = state0
      }

      const resp = await postgrestUpsertPlayer(projectUrl, serviceKey, {
        tg_id: targetTgId,
        state: mergedState,
        updated_at: new Date().toISOString(),
      })
      if (!resp.ok) {
        const text = await resp.text().catch(() => "")
        return new Response(JSON.stringify({ ok: false, error: "supabase_error", details: text.slice(0, 1500) }), {
          status: 500,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        })
      }
      return new Response(JSON.stringify({ ok: true, tg_id: targetTgId }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      })
    }
  } catch (_eAdmin) {}

  const tgId = actorTgId

  // Soft rate limit: cloud saves can be frequent; cap to a few per second.
  try {
    const rlKey = `upsert_player:${tgId}`
    const rl = await postgrestRateLimitAllow(projectUrl, serviceKey, rlKey, 2500)
    if (rl.ok && !rl.allowed) {
      return new Response(JSON.stringify({ ok: true, skipped: true, reason: "rate_limited", next_allow_at: rl.next_allow_at || null }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      })
    }
  } catch (_e) {}

  const prevName = await postgrestGetPlayerName(projectUrl, serviceKey, tgId).catch(() => "")
  const name = sanitizeName(body?.name || verified.user?.first_name || verified.user?.username || "Player") || "Player"
  // Prefer verified Telegram avatar; client may be stale.
  const photoUrl = String(verified.user?.photo_url || body?.photo_url || "")

  // Enforce unique nickname (case-insensitive) at the app level for better UX.
  try {
    const owner = await postgrestFindNameOwner(projectUrl, serviceKey, name)
    if (owner && owner !== tgId) {
      return new Response(JSON.stringify({ ok: false, error: "name_taken" }), {
        status: 409,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      })
    }
  } catch (_e) {}

  let state: unknown = null
  try {
    state = (body && typeof body === "object") ? (body as any).state : null
    if (state && typeof state === "object") {
      const size = JSON.stringify(state).length
      if (size > 420000) {
        return new Response(JSON.stringify({ ok: false, error: "state_too_large", size }), {
          status: 413,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        })
      }
    } else {
      state = null
    }
  } catch (_) {
    state = null
  }

  // Single-device, but allow multiple sessions on SAME device.
  // - If incoming session_id differs and device differs => session_conflict.
  // - If device matches => allow and "take over" active_session_id to newest session.
  try {
    const incomingSid = String(body?.session_id ?? (body as any)?.sessionId ?? "").trim()
    const incomingDeviceId = String(body?.device_id ?? (body as any)?.deviceId ?? "").trim()
    const cur = await postgrestGetPlayerSession(projectUrl, serviceKey, tgId).catch(() => ({ sid: "", updatedAt: "", deviceId: "" }))
    const currentSid = String(cur.sid || "").trim()
    const currentDeviceId = String(cur.deviceId || "").trim()
    if (currentSid && incomingSid && incomingSid !== currentSid) {
      if (currentDeviceId && incomingDeviceId && incomingDeviceId === currentDeviceId) {
        await postgrestSetPlayerSession(projectUrl, serviceKey, tgId, incomingSid)
      } else {
      return new Response(JSON.stringify({ ok: false, error: "session_conflict" }), {
        status: 409,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      })
      }
    }
    // Optional heartbeat for active session
    if (incomingSid && incomingSid === currentSid) {
      await postgrestSetPlayerSession(projectUrl, serviceKey, tgId, incomingSid)
    } else if (!currentSid && incomingSid) {
      await postgrestSetPlayerSession(projectUrl, serviceKey, tgId, incomingSid)
    }
  } catch (_e) {}

  // invite bonus: 10 gold to referrer when invitee reaches level 5 (once per invitee)
  // also ensure both sides have each other in братки: referrer already has invitee (addFriendToReferrer);
  // add referrer to invitee's state.friends so invitee sees referrer in братки
  try {
    const refTgId = String(body?.referrer_tg_id ?? (body as any)?.referrerTgId ?? "").trim()
    const inviteeLevel = Math.max(1, safeNonNegInt(body?.level))
    if (refTgId && refTgId !== tgId && state && typeof state === "object") {
      const so = state as any
      let frList: any[] = Array.isArray(so.friends) ? so.friends.slice() : []
      const hasRef = frList.some((f: any) => f && String(f.tg_id || f.tgId || "").trim() === refTgId)
      if (!hasRef) {
        const refName = await postgrestGetPlayerName(projectUrl, serviceKey, refTgId).catch(() => "") || "Браток"
        frList.push({ tg_id: refTgId, name: refName.trim().slice(0, 18) || "Браток", lvl: 1, power: 0 })
        so.friends = frList.slice(-200)
      }

      if (inviteeLevel >= 5) {
        const alreadyPaid = !!so._invBonusPaid
        if (!alreadyPaid) {
          so._invBonusPaid = true
          so._invBonusBy = refTgId
          so._invBonusAt = new Date().toISOString()
          await creditReferrerGold(projectUrl, serviceKey, refTgId, 10).catch(() => false)
        }
      }

      const friendName = String(body?.name || verified.user?.first_name || verified.user?.username || "Player").trim() || "Player"
      const friendLvl = Math.max(1, safeNonNegInt(body?.level))
      const friendPower = safeNonNegInt(body?.stats_sum ?? (body as any)?.statsSum)
      await addFriendToReferrer(projectUrl, serviceKey, refTgId, {
        tg_id: tgId,
        name: friendName,
        lvl: friendLvl,
        power: friendPower,
      }).catch(() => false)
    }
  } catch (_) {}

  const persistedMetrics = derivePersistedMetrics(body, state)

  const payload: Record<string, unknown> = {
    tg_id: tgId,
    name,
    photo_url: photoUrl,
    arena_power: safeNonNegInt(body?.arena_power ?? (body as any)?.arenaPower),
    arena_wins: safeNonNegInt(body?.arena_wins ?? (body as any)?.arenaWins),
    arena_losses: safeNonNegInt(body?.arena_losses ?? (body as any)?.arenaLosses),
    level: Math.max(1, safeNonNegInt(body?.level)),
    stats_sum: safeNonNegInt(body?.stats_sum ?? (body as any)?.statsSum),
    boss_wins: safeNonNegInt(body?.boss_wins ?? (body as any)?.bossWins),
    xp: persistedMetrics.xp,
    gold: persistedMetrics.gold,
    tooth: persistedMetrics.tooth,
    district_fear_total: persistedMetrics.districtFearTotal,
    state,
    updated_at: new Date().toISOString(),
  }

  const resp = await postgrestUpsertPlayer(projectUrl, serviceKey, payload)
  if (!resp.ok) {
    const text = await resp.text().catch(() => "")
    try {
      if (/duplicate key value/i.test(text) && /(players_name_lower_uniq|player name|name_lower)/i.test(text)) {
        return new Response(JSON.stringify({ ok: false, error: "name_taken" }), {
          status: 409,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        })
      }
    } catch (_e) {}
    return new Response(JSON.stringify({
      ok: false,
      error: "supabase_error",
      details: text.slice(0, 1500),
      debug: {
        step: "postgrest_upsert_players",
        status: resp.status,
        status_text: resp.statusText,
        has_state: !!state,
        state_size: (()=>{ try { return state && typeof state === 'object' ? JSON.stringify(state).length : 0; } catch (_e) { return -1; } })(),
      }
    }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  // Best-effort: if nickname changed, propagate to places that store name by string.
  try {
    const prevN = normName(prevName)
    const nextN = normName(name)
    if (prevN && nextN && prevN !== nextN) {
      bestEffortPropagateRename(projectUrl, serviceKey, tgId, prevName, name, photoUrl).catch(() => {})
    }
  } catch (_e) {}

  return new Response(JSON.stringify({ ok: true, tg_id: tgId }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})

 export {}

/* To invoke locally:

  1. Run `supabase start` (see: https://supabase.com/docs/reference/cli/supabase-start)
  2. Make an HTTP request:

  curl -i --location --request POST 'http://127.0.0.1:54321/functions/v1/upsert_player' \
    --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0' \
    --header 'Content-Type: application/json' \
    --data '{"name":"Functions"}'

*/
