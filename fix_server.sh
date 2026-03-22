#!/bin/bash
# Patch server.ts: add wsHeartbeat import and pong handler
sed -i 's/wsOnlineCount } from ".\/ws.ts";/wsOnlineCount, wsHeartbeat } from ".\/ws.ts";/' /opt/gameapi/server.ts

# Add heartbeat call on pong message
sed -i 's/} else if (msg.type === "pong") {}/} else if (msg.type === "pong") { if (tgId) wsHeartbeat(tgId); }/' /opt/gameapi/server.ts

echo "server.ts patched"
head -1 /opt/gameapi/server.ts
grep 'pong' /opt/gameapi/server.ts
