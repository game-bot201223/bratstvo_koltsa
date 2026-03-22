#!/bin/bash
# Patch boss_fight_hit.ts to add Redis caching after successful hits

# 1. Add Redis import at the top of the file
head -1 /opt/gameapi/functions/boss_fight_hit.ts | grep -q 'redis.ts' || {
  sed -i '1i\import { redisBossSetHp, redisBossDmgLogPush, redisBossTopAdd } from "../redis.ts";' /opt/gameapi/functions/boss_fight_hit.ts
}

# 2. After the successful response line, add Redis caching
# Find the line: return new Response(JSON.stringify({ ok: true, fight: ar.fight, boss: def }),
# and add Redis caching before it
grep -q 'redisBossSetHp' /opt/gameapi/functions/boss_fight_hit.ts || {
  sed -i '/return new Response(JSON.stringify({ ok: true, fight: ar.fight, boss: def })/{
i\  // Cache in Redis for real-time reads\
  try {\
    const fightData = ar.fight;\
    const fHp = parseInt(String(fightData?.hp ?? fightData?.current_hp ?? 0), 10) || 0;\
    const fMaxHp = parseInt(String(fightData?.max_hp ?? def.max_hp ?? 0), 10) || 0;\
    const fExpires = String(fightData?.expires_at || expiresAt || "");\
    redisBossSetHp(ownerTgId, bossId, fHp, fMaxHp, fExpires).catch(() => {});\
    if (fromName) {\
      redisBossDmgLogPush(ownerTgId, bossId, fromName, dmg).catch(() => {});\
      redisBossTopAdd(ownerTgId, bossId, fromName, dmg).catch(() => {});\
    }\
  } catch (_rc) {}
}' /opt/gameapi/functions/boss_fight_hit.ts
}

echo "boss_fight_hit.ts patched"
head -3 /opt/gameapi/functions/boss_fight_hit.ts
echo "---"
grep -n 'redisBoss' /opt/gameapi/functions/boss_fight_hit.ts
