import { redisSet, redisGet, redisBossSetHp, redisBossGetHp, redisRateLimit, redisBossDmgLogPush, redisBossDmgLogGet, redisBossTopAdd, redisBossTopGet } from './redis.ts';

console.log('=== Redis Module Test ===');

// Basic SET/GET
await redisSet('test:ping', 'pong', 60);
const v = await redisGet('test:ping');
console.log('SET/GET:', v === 'pong' ? 'OK' : 'FAIL (got: ' + v + ')');

// Boss HP
await redisBossSetHp('12345', 1, 1500, 2500, new Date(Date.now() + 3600000).toISOString());
const boss = await redisBossGetHp('12345', 1);
console.log('Boss HP:', boss && boss.hp === 1500 && boss.max_hp === 2500 ? 'OK' : 'FAIL', JSON.stringify(boss));

// Rate limit
const r1 = await redisRateLimit('test:rl:' + Date.now(), 2000);
const r2 = await redisRateLimit('test:rl:' + (Date.now() - 1), 2000);
console.log('Rate limit:', r1 === true ? 'OK (allowed)' : 'FAIL', r2 === true ? 'OK (allowed)' : 'FAIL');

// Damage log
await redisBossDmgLogPush('12345', 1, 'Player1', 500);
await redisBossDmgLogPush('12345', 1, 'Player2', 300);
const dmgLog = await redisBossDmgLogGet('12345', 1, 10);
console.log('Dmg log:', dmgLog.length >= 2 ? 'OK' : 'FAIL', JSON.stringify(dmgLog.slice(0, 3)));

// Top damage
await redisBossTopAdd('12345', 1, 'Player1', 500);
await redisBossTopAdd('12345', 1, 'Player2', 300);
await redisBossTopAdd('12345', 1, 'Player1', 200);
const top = await redisBossTopGet('12345', 1, 5);
console.log('Top dmg:', top.length >= 2 && top[0].who === 'Player1' ? 'OK' : 'FAIL', JSON.stringify(top));

console.log('=== All tests done ===');
Deno.exit(0);
