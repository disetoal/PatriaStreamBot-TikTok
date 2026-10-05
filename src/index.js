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
  PermissionFlagsBits,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  EmbedBuilder
} from "discord.js";

loadDotEnv();

const VERSION = "1.0.3";
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
let snapshotReady = false;
let discordClient = null;
let discordConfig = { purchaseChannelId: "", announcementChannelId: "", supportChannelIds: [], approvalEmoji: "✅", approvalMode: "any", allowedRoleIds: [], assistantEnabled: true, assistantPrefix: "!patria", staffRoleIds: [] };
const pendingStaffDrafts = new Map();
let discordLastEventAt = null;
let discordConfigAt = 0;
let discordChannelHealth = { purchaseChannelReady:false, announcementChannelReady:false, supportChannelsReady:0, supportChannelsExpected:0 };
let discordChannelIssues = [];
const discordErrors = [];


function log(...args) { console.log(new Date().toISOString(), ...args); }
function warn(...args) { console.warn(new Date().toISOString(), ...args); }
function rememberError(message) {
  const text = String(message || "error").slice(0, 240);
  recentErrors.push(text);
  while (recentErrors.length > 5) recentErrors.shift();
  warn(text);
}

function clearRecentErrors(prefix) {
  for (let i = recentErrors.length - 1; i >= 0; i--) {
    if (String(recentErrors[i] || "").startsWith(prefix)) recentErrors.splice(i, 1);
  }
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
    if (!response.ok) throw new Error(`${path} HTTP ${response.status}: ${body?.message || body?.error || response.statusText}`);
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
    lastThumbnail: null,
    lastPositiveAt: 0,
    lastRoomUserAt: 0
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
      let positive = false;
      try {
        // Revalidación estricta mientras ya está marcado LIVE. TikTok puede dejar
        // el websocket o roomInfo viejo abierto después de finalizar el directo.
        const htmlState = await fetchHtmlLiveState(monitor.connection, monitor.username);
        if (htmlState.known && !htmlState.live) {
          await markOffline(monitor, `HTML confirma OFFLINE${htmlState.status !== null ? ` (status ${htmlState.status})` : ""}`);
          return;
        }
        if (htmlState.known && htmlState.live) {
          positive = true;
          monitor.lastPositiveAt = Date.now();
          if (htmlState.roomId) monitor.roomId = String(htmlState.roomId);
        }

        const info = await monitor.connection.fetchRoomInfo(monitor.roomId || undefined);
        const status = extractRoomStatus(info);
        if (status !== null && status !== 2) {
          await markOffline(monitor, `room status ${status} (2 = LIVE)`);
          return;
        }
        if (status === 2) {
          positive = true;
          monitor.lastPositiveAt = Date.now();
        }
        applyRoomMeta(monitor, info);

        // Si durante varios minutos no existe ninguna prueba positiva nueva
        // (status 2, HTML LIVE o ROOM_USER), cerramos la sesión antes de dejar
        // un falso LIVE eterno en la web.
        const freshestPositive = Math.max(monitor.lastPositiveAt || 0, monitor.lastRoomUserAt || 0);
        if (!positive && freshestPositive && Date.now() - freshestPositive > 180000) {
          await markOffline(monitor, "sin confirmación LIVE fresca durante 3 minutos");
          return;
        }

        await sendLiveSample(monitor, "sample");
      } catch (error) {
        rememberError(`@${monitor.username}: no se pudo refrescar LIVE: ${error?.message || error}`);
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
  monitor.lastPositiveAt = Date.now();
  monitor.offlineReported = false;
  await sendLiveSample(monitor, "live");
  log(`🔴 @${monitor.username} LIVE · room ${monitor.roomId || "?"} · ${reason}`);
}

function attachConnectionEvents(monitor, connection) {
  connection.on(WebcastEvent.ROOM_USER, (data) => {
    const viewers = safeInt(data?.viewerCount ?? data?.userCount ?? data?.memberCount, 0);
    monitor.lastViewerCount = viewers;
    monitor.lastRoomUserAt = Date.now();
    monitor.lastPositiveAt = Date.now();
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
  const result = await sendEvent(payload);
  if (type === "live") log(`🌐 @${monitor.username} sincronizado con la página · sesión ${result?.sessionId || "?"}`);
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
  monitor.lastPositiveAt = 0;
  monitor.lastRoomUserAt = 0;
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

function activeStreamSnapshot(values = [...monitors.values()]) {
  return values.filter((m) => m.live).map((m) => ({
    username: m.username,
    roomId: m.roomId || null,
    viewers: safeInt(m.lastViewerCount, 0),
    title: m.lastTitle || `@${m.username} está en directo`,
    startedAt: m.lastStartedAt || null,
    thumbnail: m.lastThumbnail || null
  }));
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
        snapshotReady,
        activeStreams: activeStreamSnapshot(values),
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
    clearRecentErrors("heartbeat:");
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
    snapshotReady = true;
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

function clearDiscordErrors(prefix) {
  for (let i = discordErrors.length - 1; i >= 0; i--) {
    if (String(discordErrors[i] || "").startsWith(prefix)) discordErrors.splice(i, 1);
  }
}

async function syncDiscordConfig(force = false) {
  if (!force && Date.now() - discordConfigAt < 60000) return discordConfig;
  try {
    const data = await api("/api/internal/discord/config");
    discordConfig = {
      purchaseChannelId: String(data.purchaseChannelId || ""),
      announcementChannelId: String(data.announcementChannelId || ""),
      supportChannelIds: Array.isArray(data.supportChannelIds) ? data.supportChannelIds.map(String) : [],
      approvalEmoji: String(data.approvalEmoji || "✅"),
      approvalMode: data.approvalMode === "admin" ? "admin" : "any",
      allowedRoleIds: Array.isArray(data.allowedRoleIds) ? data.allowedRoleIds.map(String) : [],
      assistantEnabled: data.assistantEnabled !== false,
      assistantPrefix: String(data.assistantPrefix || "!patria"),
      staffRoleIds: Array.isArray(data.staffRoleIds) ? data.staffRoleIds.map(String) : []
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
  let announcementChannelReady = false;
  let supportChannelsReady = 0;
  const channelIssues = [];
  const inspectChannel = async (id, label) => {
    if (!id) { channelIssues.push(`${label}: ID no configurado`); return false; }
    try {
      const channel = await discordClient.channels.fetch(String(id));
      if (!channel) { channelIssues.push(`${label} ${id}: no encontrado`); return false; }
      if (typeof channel.isTextBased === "function" && !channel.isTextBased()) {
        channelIssues.push(`${label} ${id}: no es un canal de texto`);
        return false;
      }
      const me = channel.guild?.members?.me || null;
      const perms = me && typeof channel.permissionsFor === "function" ? channel.permissionsFor(me) : null;
      if (perms && !perms.has(PermissionFlagsBits.ViewChannel)) {
        channelIssues.push(`${label} ${id}: falta View Channel`);
        return false;
      }
      if (perms && !perms.has(PermissionFlagsBits.SendMessages)) {
        channelIssues.push(`${label} ${id}: falta Send Messages`);
        return false;
      }
      return true;
    } catch (error) {
      channelIssues.push(`${label} ${id}: ${String(error?.message || error).slice(0,120)}`);
      return false;
    }
  };
  try {
    await syncDiscordConfig();
    if (discordClient.isReady()) {
      purchaseChannelReady = await inspectChannel(discordConfig.purchaseChannelId, "Compras");
      announcementChannelReady = await inspectChannel(discordConfig.announcementChannelId, "Anuncios");
      for (const id of discordConfig.supportChannelIds || []) {
        if (await inspectChannel(id, "Soporte")) supportChannelsReady++;
      }
    }
    discordChannelHealth={purchaseChannelReady,announcementChannelReady,supportChannelsReady,supportChannelsExpected:(discordConfig.supportChannelIds||[]).length};
    discordChannelIssues = [...channelIssues].slice(-5);
    await api("/api/internal/discord/heartbeat", {
      method: "POST",
      body: JSON.stringify({
        connected: discordClient.isReady(),
        user: discordClient.user ? `${discordClient.user.username}#${discordClient.user.discriminator}` : null,
        guilds: discordClient.guilds?.cache?.size || 0,
        purchaseChannelReady,
        announcementChannelReady,
        supportChannelsReady,
        supportChannelsExpected: (discordConfig.supportChannelIds || []).length,
        lastEventAt: discordLastEventAt,
        errors: [...discordErrors, ...channelIssues].slice(-5)
      })
    });
    clearDiscordErrors("heartbeat:");
  } catch (error) {
    rememberDiscordError(`heartbeat: ${error?.message || error}`);
  }
}


async function memberIsStaff(messageOrInteraction, userId) {
  await syncDiscordConfig();
  const guild = messageOrInteraction.guild;
  if (!guild) return false;
  try {
    const member = await guild.members.fetch(userId);
    if (member.permissions.has(PermissionFlagsBits.Administrator) || member.permissions.has(PermissionFlagsBits.ManageGuild)) return true;
    return discordConfig.staffRoleIds.some((id) => member.roles.cache.has(id));
  } catch { return false; }
}

function cleanAssistantCommand(message) {
  let text = String(message.content || "").trim();
  if (discordClient?.user) text = text.replace(new RegExp(`<@!?${discordClient.user.id}>`, "g"), "").trim();
  const prefix = String(discordConfig.assistantPrefix || "!patria");
  if (text.toLowerCase().startsWith(prefix.toLowerCase())) text = text.slice(prefix.length).trim();
  return text;
}

async function handleAssistantMessage(message) {
  if (message.author?.bot || !message.guild || !discordClient?.isReady()) return;
  await syncDiscordConfig();
  if (!discordConfig.assistantEnabled) return;
  const prefix = String(discordConfig.assistantPrefix || "!patria");
  const mentioned = message.mentions?.users?.has(discordClient.user.id);
  const prefixed = String(message.content || "").trim().toLowerCase().startsWith(prefix.toLowerCase());
  if (!mentioned && !prefixed) return;

  const command = cleanAssistantCommand(message);
  const lower = command.toLowerCase();
  const isStaffCommand = lower.startsWith("anuncio ") || lower.startsWith("evento ");
  const inSupport = (discordConfig.supportChannelIds || []).includes(String(message.channelId));

  if (isStaffCommand) {
    if (!(await memberIsStaff(message, message.author.id))) return void message.reply("🔒 Ese comando es solo para staff autorizado.");
    const kind = lower.startsWith("evento ") ? "evento" : "anuncio";
    const text = command.slice(kind.length).trim().slice(0, 3500);
    if (!text) return void message.reply("Falta el contenido del borrador.");
    const id = `${Date.now().toString(36)}${Math.random().toString(36).slice(2,7)}`;
    pendingStaffDrafts.set(id, { userId: message.author.id, channelId: message.channelId, guildId: message.guildId, kind, text, createdAt: Date.now() });
    const destination = discordConfig.announcementChannelId ? `<#${discordConfig.announcementChannelId}>` : "el canal actual";
    const embed = new EmbedBuilder().setColor(kind === "evento" ? 0x007934 : 0xF9E300)
      .setTitle(kind === "evento" ? "🏆 Vista previa de evento" : "📢 Vista previa de anuncio")
      .setDescription(text)
      .addFields({name:"Destino",value:destination,inline:false})
      .setFooter({ text: "Mi Patria Craft · Requiere confirmación" }).setTimestamp();
    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId(`patria_confirm_${id}`).setLabel("Publicar").setStyle(ButtonStyle.Success).setEmoji("✅"),
      new ButtonBuilder().setCustomId(`patria_cancel_${id}`).setLabel("Cancelar").setStyle(ButtonStyle.Secondary).setEmoji("✖️")
    );
    return void message.reply({ embeds:[embed], components:[row] });
  }

  if (!inSupport) {
    if (mentioned || prefixed) {
      const channels=(discordConfig.supportChannelIds||[]).map(id=>`<#${id}>`).join(" o ");
      return void message.reply(`🛟 El Asistente Patria atiende soporte en ${channels || "los canales oficiales de soporte"}.`);
    }
    return;
  }

  if (!command) return void message.reply(`🇧🇴 Hola. Pregúntame sobre Mi Patria Craft. Si necesitas abrir un caso usa \`${prefix} ticket <tu problema>\`.`);

  if (lower.startsWith("ticket ")) {
    const description=command.slice("ticket".length).trim().slice(0,3500);
    if(!description)return void message.reply(`Usa \`${prefix} ticket <describe tu problema>\`.`);
    try {
      const r=await api("/api/internal/discord/support-ticket",{method:"POST",body:JSON.stringify({
        channelId:message.channelId,guildId:message.guildId,messageId:message.id,
        userId:message.author.id,username:message.author.globalName||message.author.username,
        subject:`Soporte Discord · ${message.author.globalName||message.author.username}`,description
      })});
      discordLastEventAt=new Date().toISOString();
      return void message.reply(`🎫 **Ticket #${r.ticketId} creado.** El staff ya puede verlo en PatriaNetwork.`);
    } catch(error){rememberDiscordError(`ticket: ${error?.message||error}`);return void message.reply("No pude crear el ticket. Avisa a un miembro del staff.");}
  }

  try {
    const r = await api("/api/internal/discord/assistant", { method:"POST", body:JSON.stringify({ text: command, userId: message.author.id, username: message.author.username, channelId: message.channelId }) });
    await message.reply({ content: `🇧🇴 **Asistente Patria**\n${String(r.answer || "No encontré respuesta.").slice(0,1900)}\n\nSi necesitas seguimiento usa \`${prefix} ticket <tu problema>\`.`, allowedMentions:{ repliedUser:false } });
    discordLastEventAt = new Date().toISOString();
  } catch (error) { rememberDiscordError(`asistente: ${error?.message || error}`); }
}

async function handleStaffDraftInteraction(interaction) {
  if (!interaction.isButton()) return;
  const m = interaction.customId.match(/^patria_(confirm|cancel)_([a-z0-9]+)$/i); if (!m) return;
  const draft = pendingStaffDrafts.get(m[2]);
  if (!draft) return void interaction.reply({ content:"Este borrador expiró.", ephemeral:true });
  if (draft.userId !== interaction.user.id || !(await memberIsStaff(interaction, interaction.user.id))) return void interaction.reply({ content:"No puedes confirmar este borrador.", ephemeral:true });
  pendingStaffDrafts.delete(m[2]);
  if (m[1] === "cancel") return void interaction.update({ content:"Borrador cancelado.", embeds:[], components:[] });

  await syncDiscordConfig(true);
  const targetId=discordConfig.announcementChannelId || draft.channelId;
  const target=await discordClient.channels.fetch(targetId).catch(()=>null);
  if(!target || !target.isTextBased()) return void interaction.reply({content:"No puedo acceder al canal oficial de anuncios.",ephemeral:true});

  const isEvent=draft.kind==="evento";
  const embed = new EmbedBuilder().setColor(isEvent?0x007934:0xF9E300)
    .setTitle(isEvent?"🏆 EVENTO · MI PATRIA CRAFT":"🇧🇴 COMUNICADO · MI PATRIA CRAFT")
    .setDescription(draft.text)
    .setFooter({text:"Mi Patria Craft • Staff oficial"}).setTimestamp();
  await target.send({embeds:[embed]});
  await interaction.update({ content:`✅ Publicado en <#${targetId}> por ${interaction.user}.`, embeds:[], components:[] });
  discordLastEventAt=new Date().toISOString();
  await api("/api/internal/discord/staff-action", { method:"POST", body:JSON.stringify({ action:isEvent?"event.publish":"announcement.publish",actor:interaction.user.globalName||interaction.user.username,summary:draft.text.slice(0,500),messageId:interaction.message.id }) }).catch(()=>{});
}

async function pollDiscordOutbox() {
  if (!discordClient?.isReady()) return;
  try {
    await syncDiscordConfig();
    const r=await api("/api/internal/discord/outbox");
    for(const item of r.items||[]){
      try{
        const channel=await discordClient.channels.fetch(String(item.targetChannelId||discordConfig.announcementChannelId||"")).catch(()=>null);
        if(!channel || !channel.isTextBased())throw new Error("canal_destino_no_disponible");
        const p=item.payload||{};
        const embed=new EmbedBuilder()
          .setColor(Number(p.color||0xF9E300))
          .setTitle(String(p.title||item.title||"🇧🇴 MI PATRIA CRAFT").slice(0,256))
          .setDescription(String(p.body||item.body||"").slice(0,4000))
          .setFooter({text:String(p.footer||"Mi Patria Craft • Comunicado oficial").slice(0,2048)})
          .setTimestamp();
        if(p.thumbnail){try{embed.setThumbnail(String(p.thumbnail))}catch{}}
        await channel.send({embeds:[embed]});
        await api("/api/internal/discord/outbox/ack",{method:"POST",body:JSON.stringify({id:item.id,ok:true})});
        discordLastEventAt=new Date().toISOString();
        log(`📢 Outbox #${item.id} publicado en ${channel.id}`);
      }catch(error){
        await api("/api/internal/discord/outbox/ack",{method:"POST",body:JSON.stringify({id:item.id,ok:false,error:String(error?.message||error)})}).catch(()=>{});
        rememberDiscordError(`outbox #${item.id}: ${error?.message||error}`);
      }
    }
  }catch(error){rememberDiscordError(`outbox: ${error?.message||error}`);}
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
    await pollDiscordOutbox();
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

  discordClient.on(Events.MessageCreate, handleAssistantMessage);
  discordClient.on(Events.InteractionCreate, handleStaffDraftInteraction);

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
      snapshotReady,
      activeStreams: activeStreamSnapshot(values),
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
        announcementChannelId: discordConfig.announcementChannelId || null,
        announcementChannelReady: discordChannelHealth.announcementChannelReady,
        supportChannelIds: discordConfig.supportChannelIds || [],
        supportChannelsExpected: discordChannelHealth.supportChannelsExpected,
        supportChannelsReady: discordChannelHealth.supportChannelsReady,
        lastEventAt: discordLastEventAt,
        errors: [...discordErrors, ...discordChannelIssues].slice(-5)
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
await cycle();
const cycleTimer = setInterval(cycle, POLL_SECONDS * 1000);
const heartbeatTimer = setInterval(sendHeartbeat, 30000);
const discordHeartbeatTimer = setInterval(sendDiscordHeartbeat, 30000);
const discordOutboxTimer = setInterval(pollDiscordOutbox, 10000);

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`${signal}: cerrando TikTok Bridge...`);
  clearInterval(cycleTimer);
  clearInterval(heartbeatTimer);
  clearInterval(discordHeartbeatTimer);
  clearInterval(discordOutboxTimer);
  await Promise.all([...monitors.values()].map(disconnectMonitor));
  if (discordClient) {
    try { discordClient.destroy(); } catch {}
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 4000).unref();
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
