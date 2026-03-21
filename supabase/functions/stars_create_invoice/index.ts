const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-telegram-init-data",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
}

const PRODUCTS: Record<string, { gold: number; stars: number; name: string }> = {
  stars_20_10: { gold: 20, stars: 10, name: "20 золотых монет" },
  stars_200_100: { gold: 200, stars: 100, name: "200 золотых монет" },
  stars_500_250: { gold: 500, stars: 250, name: "500 золотых монет" },
  stars_700_350: { gold: 700, stars: 350, name: "700 золотых монет" },
  stars_1000_500: { gold: 1000, stars: 500, name: "1000 золотых монет" },
  stars_3000_1500: { gold: 3000, stars: 1500, name: "3000 золотых монет" },
  stars_5000_2500: { gold: 5000, stars: 2500, name: "5000 золотых монет" },
  stars_10000_5000: { gold: 10000, stars: 5000, name: "10000 золотых монет" },
  stars_20000_10000: { gold: 20000, stars: 10000, name: "20000 золотых монет" },
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
  const key = await crypto.subtle.importKey("raw", new Uint8Array(keyBytes), { name: "HMAC", hash: "SHA-256" }, false, ["sign"])
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
  } catch (_e) {
    user = undefined
  }
  return { ok: true, user }
}

async function telegramApi(botToken: string, method: string, body: URLSearchParams): Promise<any> {
  const resp = await fetch(`https://api.telegram.org/bot${botToken}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  })
  const json = await resp.json().catch(() => null)
  if (!resp.ok || !json?.ok) {
    throw new Error(json?.description || `telegram_http_${resp.status}`)
  }
  return json.result
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
  if (!botToken) {
    return new Response(JSON.stringify({ ok: false, error: "missing_secrets" }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  let body: any = {}
  try {
    body = await req.json()
  } catch (_e) {
    body = {}
  }

  if (body?.warm) {
    return new Response(JSON.stringify({ ok: true, warm: true }), {
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
    return new Response(JSON.stringify({ ok: false, error: "unauthorized" }), {
      status: 401,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const productId = String(body?.productId || body?.product_id || "").trim()
  const product = PRODUCTS[productId]
  if (!product) {
    return new Response(JSON.stringify({ ok: false, error: "bad_product_id" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const tgId = String(verified.user.id)
  const title = product.name
  const description = `${product.name} за Telegram Stars`
  const payload = `stars:${productId}:${tgId}:${Date.now()}`
  const prices = JSON.stringify([{ label: product.name, amount: product.stars }])

  try {
    const params = new URLSearchParams()
    params.set("title", title)
    params.set("description", description)
    params.set("payload", payload)
    params.set("currency", "XTR")
    params.set("prices", prices)

    const invoiceLink = await telegramApi(botToken, "createInvoiceLink", params)
    return new Response(JSON.stringify({ ok: true, invoice_link: invoiceLink, productId, stars: product.stars, gold: product.gold }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  } catch (e) {
    return new Response(JSON.stringify({ ok: false, error: "telegram_api_error", reason: String((e as Error)?.message || e || "err") }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }
})
