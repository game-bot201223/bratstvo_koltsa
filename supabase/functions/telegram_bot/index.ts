import "jsr:@supabase/functions-js/edge-runtime.d.ts"

const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-telegram-bot-api-secret-token",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
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
  const webappUrl = getEnv("WEBAPP_URL") || "https://bratstvo-koltsa.vercel.app"

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
