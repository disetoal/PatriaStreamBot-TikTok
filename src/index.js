import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import process from "node:process";
import { TikTokLiveConnection, WebcastEvent, ControlEvent } from "tiktok-live-connector";

loadDotEnv();

const VERSION = "0.8.0";
const BASE_URL = String(process.env.PATRIABOT_URL || "").replace(/\/+$/, "");
const TOKEN = String(process.env.TIKTOK_BRIDGE_TOKEN || "");
const POLL_SECONDS = clamp(Number(process.env.POLL_SECONDS || 60), 30, 600);
const SYNC_SECONDS = clamp(Number(process.env.SYNC_SECONDS || 60), 30, 600);
const SAMPLE_SECONDS = clamp(Number(process.env.SAMPLE_SECONDS || 30), 15, 300);
const PORT = clamp(Number(process.env.PORT || 8788), 1, 65535);
const HOST_LABEL = String(process.env.HOST_LABEL || process.env.RENDER_SERVICE_NAME || process.env.RENDER_INSTANCE_ID || os.hostname()).slice(0, 120);
const DEPLOYMENT_LABEL = String(process.env.DEPLOYMENT_LABEL || "render").slice(0, 40);
const CHECK_CONCURRENCY = clamp(Number(process.env.CHECK_CONCURRENCY || 5), 1, 20);
const PUBLIC_URL = process.env.RENDER_EXTERNAL_HOSTNAME ? `https://${process.env.RENDER_EXTERNAL_HOSTNAME}` : null;

if (!BASE_URL || !/^https?:\/\//i.test(BASE_URL)) fatal("Falta PATRIABOT_URL en .env");
if (!TOKEN) fatal("Falta TIKTOK_BRIDGE_TOKEN en .env");

const monitors = new Map();
const recentErrors = [];
let lastSyncAt = 0;
let shuttingDown = false;
let loopRunning = false;

function log(...args) { console.log(new Date().toISOString(), ...args); }
function warn(...args) { console.warn(new Date().toISOString(), ...args); }
function rememberError(message) {
  const text = String(message || "error").slice(0, 240);
  recentErrors.push(text);
  while (recentErrors.length > 5) recentErrors.shift();
  warn(text);
}

async function api(path, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const response = await fetch(`${BASE_URL}${path}`, {
      ...options,
      headers: {
        Authorization: `Bearer ${TOKEN}`,
        "content-type": "application/json",
        ...(options.headers || {})
      },
      signal: controller.signal
    });
    let body = null;
    try { body = await response.json(); } catch {}
    if (!response.ok) throw new Error(`${path} HTTP ${response.status}: ${body?.error || body?.message || response.statusText}`);
    return body || {};
  } finally {
    clearTimeout(timeout);
  }
}

async function syncStreamers(force = false) {
  if (!force && Date.now() - lastSyncAt < SYNC_SECONDS * 1000) return;
  const data = await api("/api/internal/tiktok/streamers");
  const incoming = new Map((data.streamers || []).map((s) => [cleanUsername(s.username), s]).filter(([u]) => u));

  for (const [username, monitor] of monitors) {
    if (!incoming.has(username)) {
      log(`Quitando @${username} del bridge`);
      await sendEvent({ type: "offline", username }).catch(() => {});
      await disconnectMonitor(monitor);
      monitors.delete(username);
    }
  }

  for (const [username, streamer] of incoming) {
    const existing = monitors.get(username);
    if (existing) existing.streamer = streamer;
    else monitors.set(username, createMonitor(username, streamer));
  }

  lastSyncAt = Date.now();
  log(`Sincronizados ${monitors.size} streamers TikTok`);
}

function createMonitor(username, streamer) {
  return {
    username,
    streamer,
    connection: null,
    roomId: null,
    live: false,
    connecting: false,
    endedByEvent: false,
    lastSampleAt: 0,
    lastViewerCount: 0,
    lastCheckAt: 0,
    consecutiveOffline: 0,
    lastTitle: null,
    lastStartedAt: null,
    lastThumbnail: null
  };
}

async function checkMonitor(monitor) {
  if (monitor.connecting || shuttingDown) return;
  monitor.lastCheckAt = Date.now();

  if (monitor.connection && monitor.live) {
    if (Date.now() - monitor.lastSampleAt > SAMPLE_SECONDS * 1000) {
      try {
        const info = await monitor.connection.fetchRoomInfo(monitor.roomId || undefined);
        const meta = extractRoomMeta(info);
        if (meta.viewers !== null) monitor.lastViewerCount = meta.viewers;
        monitor.lastTitle = meta.title || monitor.lastTitle;
        monitor.lastStartedAt = meta.startedAt || monitor.lastStartedAt;
        monitor.lastThumbnail = meta.thumbnail || monitor.lastThumbnail;
        await sendLiveSample(monitor, "sample");
      } catch (error) {
        rememberError(`@${monitor.username}: no se pudo refrescar roomInfo: ${error?.message || error}`);
      }
    }
    return;
  }

  monitor.connecting = true;
  try {
    const connection = new TikTokLiveConnection(monitor.username, {
      processInitialData: false,
      fetchRoomInfoOnConnect: true,
      enableExtendedGiftInfo: false,
      authenticateWs: false
    });

    const isLive = await connection.fetchIsLive(monitor.username);
    if (!isLive) {
      monitor.consecutiveOffline++;
      if (monitor.live || monitor.roomId) {
        if (monitor.consecutiveOffline >= 2) await markOffline(monitor, "TikTok indica OFFLINE");
      }
      return;
    }

    monitor.consecutiveOffline = 0;
    const roomId = String(await connection.fetchRoomId(monitor.username));
    monitor.roomId = roomId;

    let roomInfo = null;
    try { roomInfo = await connection.fetchRoomInfo(roomId); } catch {}
    const meta = extractRoomMeta(roomInfo);
    monitor.lastViewerCount = meta.viewers ?? monitor.lastViewerCount ?? 0;
    monitor.lastTitle = meta.title || monitor.lastTitle || `@${monitor.username} está en directo`;
    monitor.lastStartedAt = meta.startedAt || monitor.lastStartedAt || new Date().toISOString();
    monitor.lastThumbnail = meta.thumbnail || monitor.lastThumbnail || null;

    attachConnectionEvents(monitor, connection);
    monitor.connection = connection;
    monitor.live = true;
    monitor.endedByEvent = false;

    await sendLiveSample(monitor, "live");

    try {
      const state = await connection.connect(roomId);
      const stateMeta = extractRoomMeta(state?.roomInfo);
      monitor.roomId = String(state?.roomId || roomId);
      if (stateMeta.viewers !== null) monitor.lastViewerCount = stateMeta.viewers;
      monitor.lastTitle = stateMeta.title || monitor.lastTitle;
      monitor.lastStartedAt = stateMeta.startedAt || monitor.lastStartedAt;
      monitor.lastThumbnail = stateMeta.thumbnail || monitor.lastThumbnail;
      await sendLiveSample(monitor, "sample");
      log(`🔴 @${monitor.username} LIVE · room ${monitor.roomId}`);
    } catch (error) {
      // Detecting the room is already enough to notify. A failed websocket only means
      // viewer updates will temporarily fall back to polling on the next cycle.
      rememberError(`@${monitor.username}: LIVE detectado, websocket no conectado: ${error?.message || error}`);
      monitor.connection = null;
    }
  } catch (error) {
    rememberError(`@${monitor.username}: ${error?.message || error}`);
  } finally {
    monitor.connecting = false;
  }
}

function attachConnectionEvents(monitor, connection) {
  connection.on(WebcastEvent.ROOM_USER, (data) => {
    const viewers = safeInt(data?.viewerCount ?? data?.userCount ?? data?.memberCount, 0);
    monitor.lastViewerCount = viewers;
    if (Date.now() - monitor.lastSampleAt >= SAMPLE_SECONDS * 1000) {
      sendLiveSample(monitor, "sample").catch((e) => rememberError(`@${monitor.username}: sample: ${e?.message || e}`));
    }
  });

  connection.on(WebcastEvent.STREAM_END, () => {
    monitor.endedByEvent = true;
    markOffline(monitor, "STREAM_END recibido").catch((e) => rememberError(`@${monitor.username}: offline: ${e?.message || e}`));
  });

  connection.on(ControlEvent.DISCONNECTED, ({ code, reason } = {}) => {
    if (monitor.connection === connection) monitor.connection = null;
    if (!monitor.endedByEvent) log(`⚠ @${monitor.username} websocket desconectado (${code ?? "?"}) ${reason || ""}`);
  });

  connection.on(ControlEvent.ERROR, (event) => {
    const message = event?.exception?.message || event?.info || event?.message || "error de conexión";
    rememberError(`@${monitor.username}: ${message}`);
  });
}

async function sendLiveSample(monitor, type) {
  const payload = {
    type,
    username: monitor.username,
    roomId: monitor.roomId,
    viewers: safeInt(monitor.lastViewerCount, 0),
    title: monitor.lastTitle || `@${monitor.username} está en directo`,
    startedAt: monitor.lastStartedAt || new Date().toISOString(),
    thumbnail: monitor.lastThumbnail || null
  };
  await sendEvent(payload);
  monitor.lastSampleAt = Date.now();
}

async function markOffline(monitor, reason) {
  if (!monitor.live && !monitor.roomId) return;
  log(`⚫ @${monitor.username} OFFLINE · ${reason}`);
  await sendEvent({ type: "offline", username: monitor.username, roomId: monitor.roomId });
  monitor.live = false;
  monitor.roomId = null;
  monitor.consecutiveOffline = 0;
  monitor.lastViewerCount = 0;
  monitor.lastSampleAt = 0;
  monitor.lastTitle = null;
  monitor.lastStartedAt = null;
  monitor.lastThumbnail = null;
  const connection = monitor.connection;
  monitor.connection = null;
  if (connection) {
    try { await connection.disconnect(); } catch {}
  }
}

async function disconnectMonitor(monitor) {
  const connection = monitor.connection;
  monitor.connection = null;
  monitor.live = false;
  if (connection) {
    try { await connection.disconnect(); } catch {}
  }
}

async function sendEvent(event) {
  return api("/api/internal/tiktok/event", { method: "POST", body: JSON.stringify(event) });
}

async function sendHeartbeat() {
  const values = [...monitors.values()];
  const live = values.filter((m) => m.live).length;
  const activeConnections = values.filter((m) => m.connection && m.live).length;
  try {
    await api("/api/internal/tiktok/heartbeat", {
      method: "POST",
      body: JSON.stringify({
        version: VERSION,
        deployment: DEPLOYMENT_LABEL,
        monitored: values.length,
        live,
        activeConnections,
        pollSeconds: POLL_SECONDS,
        checkConcurrency: CHECK_CONCURRENCY,
        errors: recentErrors,
        host: HOST_LABEL,
        publicUrl: PUBLIC_URL,
        uptimeSeconds: Math.round(process.uptime()),
        rssMb: Math.round(process.memoryUsage().rss / 1024 / 1024),
        heapMb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
        load1: Number(os.loadavg()[0]?.toFixed(2) || 0),
        platform: `${os.platform()} ${os.arch()}`,
        node: process.version
      })
    });
  } catch (error) {
    rememberError(`heartbeat: ${error?.message || error}`);
  }
}

async function cycle() {
  if (loopRunning || shuttingDown) return;
  loopRunning = true;
  try {
    await syncStreamers();
    const list = [...monitors.values()];
    for (let i = 0; i < list.length; i += CHECK_CONCURRENCY) {
      await Promise.all(list.slice(i, i + CHECK_CONCURRENCY).map(checkMonitor));
    }
    await sendHeartbeat();
  } catch (error) {
    rememberError(`ciclo: ${error?.message || error}`);
  } finally {
    loopRunning = false;
  }
}

function extractRoomMeta(info) {
  const room = info?.roomInfo || info?.room || info || {};
  const viewers = firstNumber(
    room?.stats?.userCount,
    room?.stats?.totalUser,
    room?.stats?.totalUserCount,
    room?.userCount,
    room?.viewerCount,
    info?.stats?.userCount,
    info?.userCount
  );
  const title = firstString(room?.title, room?.description, info?.title);
  const createTime = firstNumber(room?.createTime, room?.create_time, room?.startTime, room?.start_time, info?.createTime);
  const startedAt = createTime ? new Date(createTime < 1e12 ? createTime * 1000 : createTime).toISOString() : null;
  const thumbnail = firstUrl(
    room?.cover?.urlList,
    room?.cover?.url_list,
    room?.coverUrl,
    room?.cover_url,
    room?.owner?.avatarThumb?.urlList,
    info?.cover?.urlList
  );
  return { viewers, title, startedAt, thumbnail };
}

function firstNumber(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n) && n >= 0) return Math.round(n);
  }
  return null;
}
function firstString(...values) {
  for (const value of values) if (typeof value === "string" && value.trim()) return value.trim().slice(0, 300);
  return null;
}
function firstUrl(...values) {
  for (const value of values) {
    const candidates = Array.isArray(value) ? value : [value];
    for (const c of candidates) {
      if (typeof c !== "string") continue;
      try { const u = new URL(c); if (u.protocol === "http:" || u.protocol === "https:") return u.toString(); } catch {}
    }
  }
  return null;
}
function safeInt(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) && n >= 0 ? Math.round(n) : fallback;
}
function cleanUsername(value) {
  return String(value || "").trim().replace(/^@/, "").split(/[/?#]/)[0].toLowerCase();
}
function clamp(n, min, max) {
  return Number.isFinite(n) ? Math.min(max, Math.max(min, Math.round(n))) : min;
}

function loadDotEnv() {
  try {
    if (!fs.existsSync(".env")) return;
    const text = fs.readFileSync(".env", "utf8");
    for (const raw of text.split(/\r?\n/)) {
      const line = raw.trim();
      if (!line || line.startsWith("#")) continue;
      const idx = line.indexOf("=");
      if (idx < 1) continue;
      const key = line.slice(0, idx).trim();
      let value = line.slice(idx + 1).trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
      if (process.env[key] === undefined) process.env[key] = value;
    }
  } catch (error) { warn(`No pude leer .env: ${error?.message || error}`); }
}

function fatal(message) {
  console.error(message);
  process.exit(1);
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    const values = [...monitors.values()];
    res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({
      ok: true,
      service: "PatriaStreamBot TikTok Bridge",
      version: VERSION,
      monitored: values.length,
      live: values.filter((m) => m.live).length,
      activeConnections: values.filter((m) => m.connection && m.live).length,
      host: HOST_LABEL,
      deployment: DEPLOYMENT_LABEL,
      uptimeSeconds: Math.round(process.uptime()),
      rssMb: Math.round(process.memoryUsage().rss / 1024 / 1024),
      heapMb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      load1: Number(os.loadavg()[0]?.toFixed(2) || 0),
      recentErrors
    }));
    return;
  }
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end("PatriaStreamBot TikTok Bridge\n");
});

server.listen(PORT, "0.0.0.0", () => log(`TikTok Bridge v${VERSION} escuchando en :${PORT}`));

await syncStreamers(true).catch((e) => rememberError(`sync inicial: ${e?.message || e}`));
await sendHeartbeat();
await cycle();
const cycleTimer = setInterval(cycle, POLL_SECONDS * 1000);
const heartbeatTimer = setInterval(sendHeartbeat, 30000);

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`${signal}: cerrando TikTok Bridge...`);
  clearInterval(cycleTimer);
  clearInterval(heartbeatTimer);
  await Promise.all([...monitors.values()].map(disconnectMonitor));
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 4000).unref();
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
