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

function safeNonNegInt(v: unknown): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return 0
  return Math.max(0, Math.floor(n))
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

  const tgId = String(verified.user.id)

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

  const name = String(body?.name || verified.user?.first_name || verified.user?.username || "Player").trim() || "Player"
  const photoUrl = String(body?.photo_url || verified.user?.photo_url || "")

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

  // invite bonus (idempotent per invitee by storing a flag in invitee state)
  try {
    const refTgId = String(body?.referrer_tg_id ?? (body as any)?.referrerTgId ?? "").trim()
    if (refTgId && refTgId !== tgId && state && typeof state === "object") {
      const so = state as any
      const alreadyPaid = !!so._invBonusPaid
      if (!alreadyPaid) {
        so._invBonusPaid = true
        so._invBonusBy = refTgId
        so._invBonusAt = new Date().toISOString()
        // best-effort credit; don't block save if referrer doesn't exist yet
        await creditReferrerGold(projectUrl, serviceKey, refTgId, 10).catch(() => false)
      }

      // best-effort: ensure invitee appears in referrer's friends list
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

  return new Response(JSON.stringify({ ok: true, tg_id: tgId }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})

/* To invoke locally:

  1. Run `supabase start` (see: https://supabase.com/docs/reference/cli/supabase-start)
  2. Make an HTTP request:

  curl -i --location --request POST 'http://127.0.0.1:54321/functions/v1/upsert_player' \
    --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0' \
    --header 'Content-Type: application/json' \
    --data '{"name":"Functions"}'

*/
