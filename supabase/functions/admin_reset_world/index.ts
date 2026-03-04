const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-admin-reset-token",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
}

async function postgrestCallWorldReset(projectUrl: string, serviceKey: string): Promise<any> {
  const url = projectUrl.replace(/\/$/, "") + "/rest/v1/rpc/admin_world_reset"
  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
    },
    body: JSON.stringify({}),
  })

  const text = await resp.text().catch(() => "")
  let j: any = null
  try {
    j = text ? JSON.parse(text) : null
  } catch (_e) {
    j = null
  }

  if (!resp.ok) {
    return { ok: false, status: resp.status, statusText: resp.statusText, body: text.slice(0, 1500) }
  }

  return j && typeof j === "object" ? j : { ok: true }
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders })
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ ok: false, error: "method_not_allowed" }), {
      status: 405,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const adminToken = String(Deno.env.get("ADMIN_RESET_TOKEN") || "").trim()
  const projectUrl = String(Deno.env.get("PROJECT_URL") || "").trim()
  const serviceKey = String(Deno.env.get("SERVICE_ROLE_KEY") || "").trim()
  if (!adminToken || !projectUrl || !serviceKey) {
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

  const tok = String(req.headers.get("x-admin-reset-token") || body?.token || "").trim()
  if (!tok || tok !== adminToken) {
    return new Response(JSON.stringify({ ok: false, error: "unauthorized" }), {
      status: 401,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    })
  }

  const r = await postgrestCallWorldReset(projectUrl, serviceKey)
  return new Response(JSON.stringify(r), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
})
