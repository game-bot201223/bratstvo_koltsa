import "jsr:@supabase/functions-js/edge-runtime.d.ts"

const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-telegram-bot-api-secret-token",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
}

async function postgrestGetPlayerState(projectUrl: string, serviceKey: string, tgId: string): Promise<any | null> {
  try {
    const url = projectUrl.replace(/\/$/, "") + `/rest/v1/players?tg_id=eq.${encodeURIComponent(tgId)}&select=state&limit=1`
    const resp = await fetch(url, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (!resp.ok) return null
    const rows = await resp.json().catch(() => [])
    const row = Array.isArray(rows) && rows.length ? rows[0] : null
    return row && typeof row === "object" ? (row as any).state : null
  } catch (_e) {
    return null
  }
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

function pickPreCheckout(update: any): { id: string; fromId: string; payload: string; currency: string; amount: number } | null {
  try {
    const pq = update?.pre_checkout_query
    if (!pq) return null
    const id = String(pq.id || "").trim()
    const fromId = String(pq?.from?.id || "").trim()
    const payload = String(pq.invoice_payload || "").trim()
    const currency = String(pq.currency || "").trim().toUpperCase()
    const amount = safeInt(pq.total_amount, 0)
    if (!id) return null
    return { id, fromId, payload, currency, amount }
  } catch (_e) {
    return null
  }
}

function pickSuccessfulPayment(update: any): { chatId: number | null; tgId: string; payload: string; currency: string; amount: number; telegramPaymentChargeId: string; providerPaymentChargeId: string } | null {
  try {
    const sp = update?.message?.successful_payment
    if (!sp) return null
    const tgId = String(update?.message?.from?.id || "").trim()
    const chatRaw = update?.message?.chat?.id
    let chatId: number | null = null
    if (typeof chatRaw === "number") chatId = chatRaw
    else if (typeof chatRaw === "string" && chatRaw.trim()) {
      const n = Number(chatRaw)
      if (Number.isFinite(n)) chatId = n
    }
    return {
      chatId,
      tgId,
      payload: String(sp.invoice_payload || "").trim(),
      currency: String(sp.currency || "").trim().toUpperCase(),
      amount: safeInt(sp.total_amount, 0),
      telegramPaymentChargeId: String(sp.telegram_payment_charge_id || "").trim(),
      providerPaymentChargeId: String(sp.provider_payment_charge_id || "").trim(),
    }
  } catch (_e) {
    return null
  }
}

function parseStarsPayload(payload: string): { productId: string; tgId: string } | null {
  try {
    const parts = String(payload || "").trim().split(":")
    if (parts.length < 3) return null
    if (parts[0] !== "stars") return null
    const productId = String(parts[1] || "").trim()
    const tgId = String(parts[2] || "").trim()
    if (!productId || !tgId) return null
    return { productId, tgId }
  } catch (_e) {
    return null
  }
}

async function creditStarsPurchase(projectUrl: string, serviceKey: string, tgId: string, productId: string, chargeId: string): Promise<{ ok: boolean; gold: number }> {
  try {
    const product = STARS_PRODUCTS[productId]
    if (!product) return { ok: false, gold: 0 }
    const st = await postgrestGetPlayerState(projectUrl, serviceKey, tgId)
    const stateObj = (st && typeof st === "object") ? st as Record<string, unknown> : {}
    const payments0 = (stateObj as any)._starsPayments
    const payments = (payments0 && typeof payments0 === "object") ? payments0 as Record<string, unknown> : {}
    if (chargeId && payments[chargeId]) return { ok: true, gold: product.gold }

    // Donation stats for admin: total gold credited from Stars purchases.
    try {
      const curTot = Number((stateObj as any)._donationsGoldTotal)
      const curCnt = Number((stateObj as any)._donationsCount)
      ;(stateObj as any)._donationsGoldTotal = (Number.isFinite(curTot) ? curTot : 0) + product.gold
      ;(stateObj as any)._donationsCount = (Number.isFinite(curCnt) ? curCnt : 0) + 1
      const lst0 = (stateObj as any)._donationsLast
      const lst: any[] = Array.isArray(lst0) ? lst0.slice() : []
      lst.unshift({
        productId,
        gold: product.gold,
        stars: product.stars,
        ts: new Date().toISOString(),
        charge_id: chargeId,
      })
      ;(stateObj as any)._donationsLast = lst.slice(0, 20)
    } catch (_eDon) {}

    const curGold = Number((stateObj as any).gold)
    const nextGold = (Number.isFinite(curGold) ? curGold : 0) + product.gold
    ;(stateObj as any).gold = Math.floor(nextGold)
    ;(stateObj as any)._starsLastPurchase = {
      productId,
      gold: product.gold,
      ts: new Date().toISOString(),
      charge_id: chargeId,
    }
    if (chargeId) {
      payments[chargeId] = { productId, gold: product.gold, ts: new Date().toISOString() }
      ;(stateObj as any)._starsPayments = payments
    }
    const resp = await postgrestUpsertPlayer(projectUrl, serviceKey, {
      tg_id: tgId,
      state: stateObj,
      updated_at: new Date().toISOString(),
    })
    return { ok: resp.ok, gold: product.gold }
  } catch (_e) {
    return { ok: false, gold: 0 }
  }
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
}

function getEnv(name: string): string {
  return String(Deno.env.get(name) || "").trim()
}

function safeInt(v: unknown, def = 0): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return def
  return Math.floor(n)
}

const STARS_PRODUCTS: Record<string, { gold: number; stars: number; name: string }> = {
  stars_2_1: { gold: 2, stars: 1, name: "2 золотых монеты (тест)" },
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

async function postgrestDedupeUpdate(
  projectUrl: string,
  serviceKey: string,
  updateId: number,
  body: any,
): Promise<{ ok: boolean; duplicate: boolean }>{
  try {
    if (!updateId) return { ok: true, duplicate: false }
    const base = projectUrl.replace(/\/$/, "")

    // 1) check if already processed
    const urlGet = base +
      `/rest/v1/telegram_webhook_updates?update_id=eq.${encodeURIComponent(String(updateId))}&select=update_id&limit=1`
    const g = await fetch(urlGet, {
      method: "GET",
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    })
    if (g.ok) {
      const rows = await g.json().catch(() => [])
      if (Array.isArray(rows) && rows.length) return { ok: true, duplicate: true }
    }

    // 2) insert marker (best-effort). If conflict happens, treat as duplicate.
    const urlIns = base + "/rest/v1/telegram_webhook_updates"
    const ins = await fetch(urlIns, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        Prefer: "return=minimal",
      },
      body: JSON.stringify([{ update_id: updateId, body: body ?? null }]),
    })
    if (ins.ok) return { ok: true, duplicate: false }
    const txt = await ins.text().catch(() => "")
    if (/duplicate key value violates unique constraint/i.test(txt)) return { ok: true, duplicate: true }
    return { ok: false, duplicate: false }
  } catch (_e) {
    return { ok: false, duplicate: false }
  }
}

async function tgCall(botToken: string, method: string, payload: any): Promise<any> {
  const url = `https://api.telegram.org/bot${botToken}/${method}`
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload ?? {}),
  })
  const text = await resp.text().catch(() => "")
  let j: any = null
  try {
    j = text ? JSON.parse(text) : null
  } catch (_e) {
    j = null
  }
  if (!resp.ok || !j || j.ok === false) {
    throw new Error(`telegram_api_error:${resp.status}:${String(text).slice(0, 250)}`)
  }
  return j
}

function pickText(update: any): string {
  try {
    const t = update?.message?.text
    return typeof t === "string" ? t : ""
  } catch (_e) {
    return ""
  }
}

function pickChatId(update: any): number | null {
  const id = update?.message?.chat?.id
  if (typeof id === "number") return id
  if (typeof id === "string" && id.trim()) {
    const n = Number(id)
    if (Number.isFinite(n)) return n
  }
  return null
}

function pickCallback(update: any): { id: string; data: string; chatId: number | null } | null {
  try {
    const cq = update?.callback_query
    if (!cq) return null
    const id = String(cq?.id || "").trim()
    const data = String(cq?.data || "").trim()
    let chatId: number | null = null
    const mid = cq?.message?.chat?.id
    if (typeof mid === "number") chatId = mid
    else if (typeof mid === "string" && mid.trim()) {
      const n = Number(mid)
      if (Number.isFinite(n)) chatId = n
    }
    if (!id) return null
    return { id, data, chatId }
  } catch (_e) {
    return null
  }
}

function cmdName(text: string): string {
  const t = String(text || "").trim()
  if (!t.startsWith("/")) return ""
  const first = t.split(/\s+/)[0] || ""
  const base = first.split("@")[0] || ""
  return base.toLowerCase()
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders })
  if (req.method !== "POST") return json({ ok: false, error: "method_not_allowed" }, 405)

  const botToken = getEnv("TELEGRAM_BOT_TOKEN") || getEnv("BOT_TOKEN")
  const webhookSecret = getEnv("TELEGRAM_WEBHOOK_SECRET") || getEnv("WEBHOOK_SECRET_TOKEN")
  const webappUrl = getEnv("WEBAPP_URL") || "https://game-bot201223.github.io/bratstvo_koltsa/"
  const projectUrl = getEnv("PROJECT_URL")
  const serviceKey = getEnv("SERVICE_ROLE_KEY")

  if (!botToken) return json({ ok: false, error: "missing_bot_token" }, 500)

  // Verify webhook secret token if configured
  if (webhookSecret) {
    const provided = String(req.headers.get("x-telegram-bot-api-secret-token") || "").trim()
    if (!provided || provided !== webhookSecret) {
      return json({ ok: false, error: "unauthorized" }, 401)
    }
  }

  let update: any = null
  try {
    update = await req.json()
  } catch (_e) {
    return json({ ok: false, error: "invalid_json" }, 400)
  }

  // Idempotency: dedupe by update_id (Telegram may retry delivery)
  try {
    const updateId = safeInt(update?.update_id, 0)
    if (updateId && projectUrl && serviceKey) {
      const dd = await postgrestDedupeUpdate(projectUrl, serviceKey, updateId, update)
      if (dd.ok && dd.duplicate) return json({ ok: true, duplicate: true })
    }
  } catch (_e) {
    // ignore
  }

  // callback_query
  const cq = pickCallback(update)
  if (cq) {
    try {
      await tgCall(botToken, "answerCallbackQuery", {
        callback_query_id: cq.id,
        text: cq.data ? `OK: ${cq.data}` : "OK",
        show_alert: false,
      })
    } catch (_e) {
      // ignore telegram errors to keep webhook stable
    }
    return json({ ok: true })
  }

  const preCheckout = pickPreCheckout(update)
  if (preCheckout) {
    try {
      const parsed = parseStarsPayload(preCheckout.payload)
      const product = parsed ? STARS_PRODUCTS[parsed.productId] : null
      const ok = !!(parsed && product && preCheckout.currency === "XTR" && preCheckout.amount === safeInt(product.stars, 0))
      await tgCall(botToken, "answerPreCheckoutQuery", {
        pre_checkout_query_id: preCheckout.id,
        ok,
        error_message: ok ? undefined : "Платёж не прошёл проверку. Попробуй ещё раз.",
      })
    } catch (_e) {
      try {
        await tgCall(botToken, "answerPreCheckoutQuery", {
          pre_checkout_query_id: preCheckout.id,
          ok: false,
          error_message: "Временная ошибка оплаты. Попробуй ещё раз.",
        })
      } catch (_e2) {}
    }
    return json({ ok: true })
  }

  const successfulPayment = pickSuccessfulPayment(update)
  if (successfulPayment) {
    try {
      const parsed = parseStarsPayload(successfulPayment.payload)
      const product = parsed ? STARS_PRODUCTS[parsed.productId] : null
      const targetTgId = parsed ? parsed.tgId : successfulPayment.tgId
      const valid = !!(
        parsed &&
        product &&
        targetTgId &&
        successfulPayment.currency === "XTR" &&
        successfulPayment.amount === safeInt(product.stars, 0)
      )
      if (valid && projectUrl && serviceKey) {
        await creditStarsPurchase(projectUrl, serviceKey, targetTgId, parsed!.productId, successfulPayment.telegramPaymentChargeId)
      }
      if (successfulPayment.chatId) {
        try {
          await tgCall(botToken, "sendMessage", {
            chat_id: successfulPayment.chatId,
            text: valid && product
              ? `Оплата прошла. Начислено ${product.gold} золотых монет.`
              : "Оплата прошла, но награда не была распознана. Напиши /help если что-то пошло не так.",
          })
        } catch (_eMsg) {}
      }
    } catch (_e) {
      // ignore to keep webhook stable
    }
    return json({ ok: true })
  }

  // message (commands)
  const text = pickText(update)
  const chatId = pickChatId(update)
  const cmd = cmdName(text)

  if (chatId && cmd) {
    try {
      if (cmd === "/start") {
        await tgCall(botToken, "sendMessage", {
          chat_id: chatId,
          text:
            "Запускай игру кнопкой ниже (WebApp).\n\nЕсли что-то глючит — закрой WebApp и открой заново.",
          reply_markup: {
            inline_keyboard: [
              [
                {
                  text: "Играть",
                  web_app: { url: webappUrl },
                },
              ],
            ],
          },
        })
      } else if (cmd === "/ping") {
        await tgCall(botToken, "sendMessage", { chat_id: chatId, text: "pong" })
      } else if (cmd === "/help") {
        await tgCall(botToken, "sendMessage", {
          chat_id: chatId,
          text: "Команды:\n/start — открыть игру\n/ping — проверка\n/help — помощь",
        })
      } else {
        // Unknown command: ignore silently to reduce noise
      }
    } catch (_e) {
      // ignore
    }
  }

  return json({ ok: true })
})
