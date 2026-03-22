import sys

with open('/opt/gameapi/functions/boss_fight_hit.ts', 'r') as f:
    content = f.read()

old = '  return new Response(JSON.stringify({ ok: true, fight: ar.fight, boss: def }), {'
new = """  // Cache in Redis for real-time reads
  try {
    const fightData = ar.fight;
    const fHp = parseInt(String(fightData?.hp ?? fightData?.current_hp ?? 0), 10) || 0;
    const fMaxHp = parseInt(String(fightData?.max_hp ?? def.max_hp ?? 0), 10) || 0;
    const fExpires = String(fightData?.expires_at || expiresAt || "");
    redisBossSetHp(ownerTgId, bossId, fHp, fMaxHp, fExpires).catch(() => {});
    if (fromName) {
      redisBossDmgLogPush(ownerTgId, bossId, fromName, dmg).catch(() => {});
      redisBossTopAdd(ownerTgId, bossId, fromName, dmg).catch(() => {});
    }
  } catch (_rc) {}

  return new Response(JSON.stringify({ ok: true, fight: ar.fight, boss: def }), {"""

if old in content:
    content = content.replace(old, new, 1)
    with open('/opt/gameapi/functions/boss_fight_hit.ts', 'w') as f:
        f.write(content)
    print('OK: patched boss_fight_hit.ts')
else:
    print('SKIP: target line not found (already patched?)')
