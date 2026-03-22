import { redisSetOnline, redisRemoveOnline, redisPublish } from "./redis.ts";

export interface WsClient {
  ws: WebSocket;
  tgId: string;
  clanId: string;
}

export const wsClients = new Map<string, WsClient>();

export function wsOnlineCount(): number {
  let n = 0;
  for (const [, c] of wsClients) if (c.ws.readyState === WebSocket.OPEN) n++;
  return n;
}

export function wsBroadcastAll(evt: unknown): void {
  const msg = JSON.stringify(evt);
  for (const [, c] of wsClients) {
    if (c.ws.readyState === WebSocket.OPEN) {
      try { c.ws.send(msg); } catch (_) {}
    }
  }
  // Also publish to Redis for multi-instance scaling in the future
  try { redisPublish("game:broadcast", msg).catch(() => {}); } catch (_) {}
}

export function wsBroadcastToClan(clanId: string, evt: unknown): void {
  if (!clanId) return;
  const msg = JSON.stringify(evt);
  for (const [, c] of wsClients) {
    if (c.ws.readyState === WebSocket.OPEN && c.clanId === clanId) {
      try { c.ws.send(msg); } catch (_) {}
    }
  }
  try { redisPublish("game:clan:" + clanId, msg).catch(() => {}); } catch (_) {}
}

export function wsRegister(tgId: string, ws: WebSocket, clanId = ""): void {
  wsClients.set(tgId, { ws, tgId, clanId });
  wsBroadcastAll({ type: "online_count", count: wsOnlineCount() });
  // Track in Redis with 2min TTL (refreshed on heartbeat)
  try { redisSetOnline(tgId, 120).catch(() => {}); } catch (_) {}
}

export function wsUnregister(tgId: string): void {
  wsClients.delete(tgId);
  wsBroadcastAll({ type: "online_count", count: wsOnlineCount() });
  try { redisRemoveOnline(tgId).catch(() => {}); } catch (_) {}
}

export function wsUpdateClan(tgId: string, clanId: string): void {
  const c = wsClients.get(tgId);
  if (c) c.clanId = clanId;
}

// Refresh online TTL on heartbeat (called from server.ts on pong)
export function wsHeartbeat(tgId: string): void {
  try { redisSetOnline(tgId, 120).catch(() => {}); } catch (_) {}
}
