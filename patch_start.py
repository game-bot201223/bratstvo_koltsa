import sys

with open('/opt/gameapi/functions/boss_fight_start.ts', 'r') as f:
    content = f.read()

patched = False

# 1. Add Redis import if not present
if 'redis.ts' not in content:
    content = 'import { redisBossSetHp, redisRateLimit } from "../redis.ts";\n' + content
    patched = True
    print('Added Redis import')

# 2. Replace PostgREST rate limit with Redis rate limit
old_rl = """  try {
    const rlKey = `boss_start:${ownerTgId}`
    const rl = await postgrestRateLimitAllow(projectUrl, serviceKey, rlKey, 650)
    if (rl.ok && !rl.allowed) {
      return new Response(JSON.stringify({ ok: true, skipped: true, reason: "rate_limited", next_allow_at: rl.next_allow_at || null }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      })
    }
  } catch (_e) {}"""

new_rl = """  try {
    const rlKey = `boss_start:${ownerTgId}`
    const rlAllowed = await redisRateLimit(rlKey, 650);
    if (!rlAllowed) {
      return new Response(JSON.stringify({ ok: true, skipped: true, reason: "rate_limited" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      })
    }
  } catch (_e) {}"""

if old_rl in content:
    content = content.replace(old_rl, new_rl, 1)
    patched = True
    print('Replaced rate limit with Redis')

# 3. Add Redis caching after successful fight start
old_return = '  return new Response(JSON.stringify({ ok: true, fight, boss: def }), {'
new_return = """  // Cache initial boss state in Redis
  try {
    const fHp = parseInt(String(fight?.hp ?? fight?.current_hp ?? def.max_hp), 10) || def.max_hp;
    const fMaxHp = parseInt(String(fight?.max_hp ?? def.max_hp), 10) || def.max_hp;
    const fExpires = String(fight?.expires_at || expiresAt || "");
    redisBossSetHp(ownerTgId, bossId, fHp, fMaxHp, fExpires).catch(() => {});
  } catch (_rc) {}

  return new Response(JSON.stringify({ ok: true, fight, boss: def }), {"""

if old_return in content and 'Cache initial boss state' not in content:
    content = content.replace(old_return, new_return, 1)
    patched = True
    print('Added Redis caching on fight start')

if patched:
    with open('/opt/gameapi/functions/boss_fight_start.ts', 'w') as f:
        f.write(content)
    print('OK: boss_fight_start.ts patched')
else:
    print('SKIP: already patched or no match')
