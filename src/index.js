import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import process from "node:process";
import { TikTokLiveConnection, WebcastEvent, ControlEvent, RouteConfig } from "tiktok-live-connector";
import {
  Client,
  Events,
  GatewayIntentBits,
  Partials,
  PermissionFlagsBits
} from "discord.js";

loadDotEnv();

const VERSION = "0.9.2";
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
const DISCORD_BOT_TOKEN = String(process.env.DISCORD_BOT_TOKEN || "");

if (!BASE_URL || !/^https?:\/\//i.test(BASE_URL)) fatal("Falta PATRIABOT_URL en .env");
if (!TOKEN) fatal("Falta TIKTOK_BRIDGE_TOKEN en .env");

const monitors = new Map();
const recentErrors = [];
let lastSyncAt = 0;
let shuttingDown = false;
let loopRunning = false;
let discordClient = null;
let discordConfig = { purchaseChannelId: "", approvalEmoji: "✅", approvalMode: "any", allowedRoleIds: [] };
let discordLastEventAt = null;
let discordConfigAt = 0;
const discordErrors = [];


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
    offlineReported: false,
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

  // If we already consider the streamer live, verify the room status on every
  // metadata refresh. TikTok often keeps the last room payload after a LIVE ends
  // (status 4), so a room id/title alone must never be treated as proof of LIVE.
  if (monitor.connection && monitor.live) {
    if (Date.now() - monitor.lastSampleAt > SAMPLE_SECONDS * 1000) {
      try {
        const info = await monitor.connection.fetchRoomInfo(monitor.roomId || undefined);
        const status = extractRoomStatus(info);
        if (status !== null && status !== 2) {
          await markOffline(monitor, `room status ${status} (2 = LIVE)`);
          return;
        }
        applyRoomMeta(monitor, info);
        await sendLiveSample(monitor, "sample");
      } catch (error) {
        rememberError(`@${monitor.username}: no se pudo refrescar roomInfo: ${error?.message || error}`);
      }
    }
    return;
  }

  monitor.connecting = true;
  let connection = null;
  try {
    connection = new TikTokLiveConnection(monitor.username, {
      processInitialData: false,
      fetchRoomInfoOnConnect: true,
      enableExtendedGiftInfo: false,
      authenticateWs: false
    });

    // First ask TikTok's own HTML state directly. This is intentionally stricter
    // than fetchIsLive(), whose composite fallbacks may occasionally resolve an
    // old/stale room after the broadcast has already ended.
    const htmlState = await fetchHtmlLiveState(connection, monitor.username);
    if (htmlState.known && !htmlState.live) {
      await markOffline(monitor, `HTML confirma OFFLINE${htmlState.status !== null ? ` (status ${htmlState.status})` : ""}`);
      return;
    }

    const isLive = await connection.fetchIsLive(monitor.username);
    if (!isLive) {
      monitor.consecutiveOffline++;
      if (monitor.consecutiveOffline >= 1) await markOffline(monitor, "TikTok indica OFFLINE");
      return;
    }

    monitor.consecutiveOffline = 0;
    const roomId = String(htmlState.roomId || await connection.fetchRoomId(monitor.username));
    monitor.roomId = roomId;

    let roomInfo = null;
    try { roomInfo = await connection.fetchRoomInfo(roomId); } catch (error) {
      rememberError(`@${monitor.username}: roomInfo previo no disponible: ${error?.message || error}`);
    }

    const roomStatus = extractRoomStatus(roomInfo);
    if (roomStatus !== null && roomStatus !== 2) {
      await markOffline(monitor, `roomInfo confirma OFFLINE (status ${roomStatus})`);
      return;
    }
    applyRoomMeta(monitor, roomInfo);

    attachConnectionEvents(monitor, connection);
    monitor.connection = connection;
    monitor.endedByEvent = false;

    // Strong confirmation before the websocket: a TikTok room status of 2 means
    // currently live. Status 4 is an ended/stale room and is rejected above.
    const preConfirmed = htmlState.live || roomStatus === 2;

    try {
      const state = await connection.connect(roomId);
      monitor.roomId = String(state?.roomId || roomId);
      const stateStatus = extractRoomStatus(state?.roomInfo);
      if (stateStatus !== null && stateStatus !== 2) {
        await markOffline(monitor, `connect devolvió room status ${stateStatus}`);
        return;
      }
      applyRoomMeta(monitor, state?.roomInfo);

      if (preConfirmed || stateStatus === 2) {
        await confirmLive(monitor, "status 2 / conexión confirmada");
        await sendLiveSample(monitor, "sample");
      } else {
        // Some TikTok responses omit status entirely. In that case, do not send
        // a false LIVE alert. We keep the socket open and wait for a real room
        // event (ROOM_USER) before confirming.
        log(`🟡 @${monitor.username} room ${monitor.roomId} conectado; esperando evidencia LIVE`);
      }
    } catch (error) {
      monitor.connection = null;
      if (preConfirmed) {
        // Explicit status 2 is stronger than a websocket/signing failure.
        await confirmLive(monitor, "status 2 confirmado; websocket no disponible");
        rememberError(`@${monitor.username}: LIVE status=2, websocket no conectado: ${error?.message || error}`);
      } else {
        await markOffline(monitor, `sin confirmación LIVE: ${error?.message || error}`);
      }
    }
  } catch (error) {
    if (connection && monitor.connection !== connection) {
      try { await connection.disconnect(); } catch {}
    }
    rememberError(`@${monitor.username}: ${error?.message || error}`);
  } finally {
    monitor.connecting = false;
  }
}

async function fetchHtmlLiveState(connection, username) {
  try {
    const result = await RouteConfig.fetchRoomInfoFromHtml({
      webClient: connection.webClient,
      uniqueId: username
    });
    const liveRoom = result?.liveRoom ?? null;
    if (!liveRoom) return { known: true, live: false, status: null, roomId: null };
    const status = extractRoomStatus(liveRoom);
    const roomId = extractRoomId(liveRoom);
    if (status !== null) return { known: true, live: status === 2, status, roomId };
    return { known: false, live: false, status: null, roomId };
  } catch (error) {
    // HTML may be challenged/rate-limited from a datacenter. That is not enough
    // to call someone offline; fall back to the webcast checks below.
    rememberError(`@${username}: HTML check no concluyente: ${error?.message || error}`);
    return { known: false, live: false, status: null, roomId: null };
  }
}

function applyRoomMeta(monitor, info) {
  const meta = extractRoomMeta(info);
  if (meta.viewers !== null) monitor.lastViewerCount = meta.viewers;
  monitor.lastTitle = meta.title || monitor.lastTitle || `@${monitor.username} está en directo`;
  monitor.lastStartedAt = meta.startedAt || monitor.lastStartedAt || new Date().toISOString();
  monitor.lastThumbnail = meta.thumbnail || monitor.lastThumbnail || null;
}

async function confirmLive(monitor, reason) {
  if (monitor.live) return;
  monitor.live = true;
  monitor.offlineReported = false;
  await sendLiveSample(monitor, "live");
  log(`🔴 @${monitor.username} LIVE · room ${monitor.roomId || "?"} · ${reason}`);
}

function attachConnectionEvents(monitor, connection) {
  connection.on(WebcastEvent.ROOM_USER, (data) => {
    const viewers = safeInt(data?.viewerCount ?? data?.userCount ?? data?.memberCount, 0);
    monitor.lastViewerCount = viewers;
    if (!monitor.live) {
      confirmLive(monitor, "ROOM_USER recibido").catch((e) => rememberError(`@${monitor.username}: confirmación: ${e?.message || e}`));
      return;
    }
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
  const shouldReport = monitor.live || !monitor.offlineReported;
  if (shouldReport) {
    log(`⚫ @${monitor.username} OFFLINE · ${reason}`);
    await sendEvent({ type: "offline", username: monitor.username, roomId: monitor.roomId });
    monitor.offlineReported = true;
  }
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

function roomObject(info) {
  return info?.roomInfo || info?.room || info?.data?.room || info?.data || info || {};
}

function extractRoomStatus(info) {
  const room = roomObject(info);
  const candidates = [room?.status, room?.roomStatus, room?.room_status, room?.liveStatus, room?.live_status];
  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n)) return Math.round(n);
  }
  return null;
}

function extractRoomId(info) {
  const room = roomObject(info);
  const value = room?.idStr ?? room?.id_str ?? room?.id ?? room?.roomId ?? room?.room_id ?? null;
  return value === null || value === undefined ? null : String(value);
}

function extractRoomMeta(info) {
  const room = roomObject(info);
  const viewers = firstNumber(
    room?.stats?.userCount,
    room?.stats?.totalUser,
    room?.stats?.totalUserCount,
    room?.userCount,
    room?.viewerCount,
    room?.user_count,
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


function rememberDiscordError(message) {
  const text = String(message || "error").slice(0, 240);
  discordErrors.push(text);
  while (discordErrors.length > 5) discordErrors.shift();
  warn(`Discord: ${text}`);
}

async function syncDiscordConfig(force = false) {
  if (!force && Date.now() - discordConfigAt < 60000) return discordConfig;
  try {
    const data = await api("/api/internal/discord/config");
    discordConfig = {
      purchaseChannelId: String(data.purchaseChannelId || ""),
      approvalEmoji: String(data.approvalEmoji || "✅"),
      approvalMode: data.approvalMode === "admin" ? "admin" : "any",
      allowedRoleIds: Array.isArray(data.allowedRoleIds) ? data.allowedRoleIds.map(String) : []
    };
    discordConfigAt = Date.now();
  } catch (error) {
    rememberDiscordError(`config: ${error?.message || error}`);
  }
  return discordConfig;
}

function reactionKey(reaction) {
  const emoji = reaction?.emoji;
  if (!emoji) return "";
  return emoji.id || emoji.name || String(emoji);
}

function reactionMatches(reaction, configured) {
  const wanted = String(configured || "✅").trim();
  const emoji = reaction?.emoji;
  if (!emoji) return false;
  return wanted === String(emoji.name || "") || wanted === String(emoji.id || "") || wanted === String(emoji);
}

async function reactorAuthorized(message, user) {
  await syncDiscordConfig();
  if (discordConfig.approvalMode === "any" && !discordConfig.allowedRoleIds.length) return true;
  if (!message.guild) return false;
  try {
    const member = await message.guild.members.fetch(user.id);
    if (member.permissions.has(PermissionFlagsBits.Administrator) || member.permissions.has(PermissionFlagsBits.ManageGuild)) return true;
    if (discordConfig.allowedRoleIds.some((id) => member.roles.cache.has(id))) return true;
    return discordConfig.approvalMode === "any";
  } catch (error) {
    rememberDiscordError(`no pude verificar permisos de ${user.id}: ${error?.message || error}`);
    return false;
  }
}

function normalizeLabel(value) {
  return String(value || "").normalize("NFD").replace(/\p{Diacritic}/gu, "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function fieldValue(embed, ...names) {
  const wanted = names.map(normalizeLabel);
  for (const f of embed?.fields || []) {
    if (wanted.includes(normalizeLabel(f?.name))) return String(f?.value || "").trim();
  }
  return "";
}

function parsePurchaseMessage(message) {
  const embeds = message?.embeds || [];
  const embed = embeds.find((e) => /nueva solicitud de compra/i.test(String(e?.title || "")))
    || embeds.find((e) => /patriacraft/i.test(`${e?.title || ""} ${e?.description || ""} ${e?.footer?.text || ""}`));
  if (!embed) return null;

  const product = fieldValue(embed, "Producto");
  const purchaseType = fieldValue(embed, "Tipo");
  const duration = fieldValue(embed, "Duración", "Duracion");
  const quantityRaw = fieldValue(embed, "Cantidad");
  const price = fieldValue(embed, "Precio total", "Total", "Precio");
  const minecraft = fieldValue(embed, "Minecraft");
  const discord = fieldValue(embed, "Discord");

  if (!price || (!product && !minecraft)) return null;
  const quantity = Math.max(1, Number.parseInt(String(quantityRaw).replace(/\D/g, ""), 10) || 1);
  return { product, purchaseType, duration, quantity, price, minecraft, discord };
}

async function postPurchaseApproval(message, reaction, user, purchase) {
  const response = await api("/api/internal/discord/purchase-approved", {
    method: "POST",
    body: JSON.stringify({
      messageId: message.id,
      channelId: message.channelId,
      guildId: message.guildId,
      reaction: reactionKey(reaction),
      approvedById: user.id,
      approvedByName: user.globalName || user.username || user.id,
      ...purchase
    })
  });
  if (!response.duplicate) {
    log(`💰 Compra aprobada: ${purchase.product || "Compra"} · ${purchase.price} · total Bs ${response.summary?.totalBs || "?"}`);
    try { await message.react("💰"); } catch {}
  }
  return response;
}

async function sendDiscordHeartbeat() {
  if (!discordClient) return;
  let purchaseChannelReady = false;
  try {
    await syncDiscordConfig();
    if (discordClient.isReady() && discordConfig.purchaseChannelId) {
      const channel = await discordClient.channels.fetch(discordConfig.purchaseChannelId).catch(() => null);
      purchaseChannelReady = !!channel;
    }
    await api("/api/internal/discord/heartbeat", {
      method: "POST",
      body: JSON.stringify({
        connected: discordClient.isReady(),
        user: discordClient.user ? `${discordClient.user.username}#${discordClient.user.discriminator}` : null,
        guilds: discordClient.guilds?.cache?.size || 0,
        purchaseChannelReady,
        lastEventAt: discordLastEventAt,
        errors: discordErrors
      })
    });
  } catch (error) {
    rememberDiscordError(`heartbeat: ${error?.message || error}`);
  }
}

async function startDiscordBot() {
  if (!DISCORD_BOT_TOKEN) {
    warn("DISCORD_BOT_TOKEN no configurado: control de compras por reacción desactivado.");
    return;
  }

  discordClient = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.GuildMessageReactions,
      GatewayIntentBits.MessageContent
    ],
    partials: [Partials.Message, Partials.Channel, Partials.Reaction, Partials.User]
  });

  discordClient.once(Events.ClientReady, async (client) => {
    log(`🤖 Discord conectado como ${client.user.tag}`);
    await syncDiscordConfig(true);
    await sendDiscordHeartbeat();
  });

  discordClient.on(Events.MessageReactionAdd, async (reaction, user) => {
    if (user?.bot) return;
    try {
      if (reaction.partial) await reaction.fetch();
      const message = reaction.message.partial ? await reaction.message.fetch() : reaction.message;
      await syncDiscordConfig();
      if (!message || message.channelId !== discordConfig.purchaseChannelId) return;
      if (!reactionMatches(reaction, discordConfig.approvalEmoji)) return;
      if (!(await reactorAuthorized(message, user))) return;
      const purchase = parsePurchaseMessage(message);
      if (!purchase) return;
      discordLastEventAt = new Date().toISOString();
      await postPurchaseApproval(message, reaction, user, purchase);
      await sendDiscordHeartbeat();
    } catch (error) {
      rememberDiscordError(`reacción: ${error?.message || error}`);
    }
  });

  discordClient.on("error", (error) => rememberDiscordError(error?.message || error));
  try {
    await discordClient.login(DISCORD_BOT_TOKEN);
  } catch (error) {
    rememberDiscordError(`login: ${error?.message || error}`);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    const values = [...monitors.values()];
    res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({
      ok: true,
      service: "PatriaStreamBot Render Bridge",
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
      recentErrors,
      discord: {
        configured: Boolean(DISCORD_BOT_TOKEN),
        connected: Boolean(discordClient?.isReady()),
        user: discordClient?.user?.tag || null,
        guilds: discordClient?.guilds?.cache?.size || 0,
        purchaseChannelId: discordConfig.purchaseChannelId || null,
        lastEventAt: discordLastEventAt,
        errors: discordErrors
      }
    }));
    return;
  }
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end("PatriaStreamBot Render Bridge\n");
});

server.listen(PORT, "0.0.0.0", () => log(`PatriaStreamBot Bridge v${VERSION} escuchando en :${PORT}`));

await startDiscordBot();
await syncStreamers(true).catch((e) => rememberError(`sync inicial: ${e?.message || e}`));
await sendHeartbeat();
await cycle();
const cycleTimer = setInterval(cycle, POLL_SECONDS * 1000);
const heartbeatTimer = setInterval(sendHeartbeat, 30000);
const discordHeartbeatTimer = setInterval(sendDiscordHeartbeat, 30000);

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`${signal}: cerrando TikTok Bridge...`);
  clearInterval(cycleTimer);
  clearInterval(heartbeatTimer);
  clearInterval(discordHeartbeatTimer);
  await Promise.all([...monitors.values()].map(disconnectMonitor));
  if (discordClient) {
    try { discordClient.destroy(); } catch {}
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 4000).unref();
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
