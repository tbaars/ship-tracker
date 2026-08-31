import http from "node:http";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import WebSocket from "ws";
import timezoneLookup from "@photostructure/tz-lookup";

const MMSI = "245464000";
const SHIP_NAME = "ROTTERDAM";
const PORT = Number(process.env.PORT || 3000);
const SAVE_INTERVAL_MS = Number(process.env.SAVE_INTERVAL_MINUTES || 30) * 60_000;
const DATA_FILE = resolve(process.env.DATA_FILE || "data/positions.json");
const AIS_URL = process.env.AISSTREAM_URL || "wss://stream.aisstream.io/v0/stream";
const API_KEY = process.env.AISSTREAM_API_KEY;
const AIS_DISABLED = process.env.AISSTREAM_DISABLED === "true";
const FEED_STALE_MS = 15 * 60_000;
const SHIP_SILENT_MS = 45 * 60_000;
const RECONNECT_DELAYS_MS = [60_000, 5 * 60_000, 15 * 60_000, 30 * 60_000, 60 * 60_000];

let positions = [];
let shuttingDown = false;
let watchdogTimer;

const channels = {
  ship: createChannel("ship"),
  nearby: createChannel("nearby")
};

function createChannel(name) {
  return {
    name,
    socket: undefined,
    reconnectTimer: undefined,
    reconnectAttempt: 0,
    state: AIS_DISABLED ? "disabled" : "starting",
    connectedAt: null,
    lastMessageAt: null,
    lastPositionAt: null,
    lastError: null
  };
}

function parseAisTimestamp(value) {
  if (!value) return null;
  const normalized = value
    .replace(" ", "T")
    .replace(/(\.\d{3})\d+/, "$1")
    .replace(/ \+0000 UTC$/, "Z")
    .replace(/\+0000 UTC$/, "Z");
  const timestamp = new Date(normalized);
  return Number.isNaN(timestamp.getTime()) ? null : timestamp;
}

async function loadPositions() {
  try {
    const saved = JSON.parse(await readFile(DATA_FILE, "utf8"));
    positions = Array.isArray(saved)
      ? saved.map((position) => ({
          ...position,
          timezone: position.timezone || timezoneLookup(
            Number(position.latitude),
            Number(position.longitude)
          )
        }))
      : [];
    console.log(`Loaded ${positions.length} saved position(s)`);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    positions = [];
  }
}

async function persistPositions() {
  await mkdir(dirname(DATA_FILE), { recursive: true });
  const temporaryFile = `${DATA_FILE}.tmp`;
  await writeFile(temporaryFile, `${JSON.stringify(positions, null, 2)}\n`);
  await rename(temporaryFile, DATA_FILE);
}

async function savePosition(message) {
  const metadata = message.MetaData || {};
  const report = message.Message?.PositionReport || {};
  const timestamp = parseAisTimestamp(metadata.time_utc);
  const latitude = Number(metadata.latitude ?? report.Latitude);
  const longitude = Number(metadata.longitude ?? report.Longitude);

  if (!timestamp || !Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    console.warn("Ignored a position with missing coordinates or AIS timestamp");
    return;
  }

  const last = positions.at(-1);
  const lastTime = last ? new Date(last.timestamp).getTime() : 0;
  if (last && timestamp.getTime() - lastTime < SAVE_INTERVAL_MS) return;

  const position = {
    mmsi: MMSI,
    shipName: SHIP_NAME,
    latitude,
    longitude,
    timestamp: timestamp.toISOString(),
    speedKnots: Number.isFinite(Number(report.Sog)) ? Number(report.Sog) : null,
    course: Number.isFinite(Number(report.Cog)) ? Number(report.Cog) : null,
    heading: Number.isFinite(Number(report.TrueHeading)) ? Number(report.TrueHeading) : null,
    timezone: timezoneLookup(latitude, longitude)
  };

  positions.push(position);
  await persistPositions();
  console.log(`Saved position ${position.latitude}, ${position.longitude} at ${position.timestamp}`);
}

function controlBoundingBox() {
  const latest = positions.at(-1);
  const latitude = Number(latest?.latitude ?? 53.77);
  const longitude = Number(latest?.longitude ?? 4.92);
  const radius = 0.2;
  return [[
    [Math.max(-90, latitude - radius), Math.max(-180, longitude - radius)],
    [Math.min(90, latitude + radius), Math.min(180, longitude + radius)]
  ]];
}

function scheduleReconnect(channel) {
  if (shuttingDown || AIS_DISABLED) return;
  const delay = RECONNECT_DELAYS_MS[
    Math.min(channel.reconnectAttempt, RECONNECT_DELAYS_MS.length - 1)
  ];
  channel.reconnectAttempt += 1;
  channel.state = "reconnecting";
  console.log(`Reconnecting ${channel.name} AIS feed in ${Math.round(delay / 60_000)} minute(s)`);
  clearTimeout(channel.reconnectTimer);
  channel.reconnectTimer = setTimeout(() => connectToAisStream(channel), delay);
}

function connectToAisStream(channel) {
  if (!API_KEY) {
    channel.state = "missing_api_key";
    channel.lastError = "AISSTREAM_API_KEY is not set";
    console.error(channel.lastError);
    return;
  }

  channel.state = "connecting";
  channel.socket = new WebSocket(AIS_URL, { perMessageDeflate: true });

  channel.socket.on("open", () => {
    channel.state = "connected";
    channel.connectedAt = new Date().toISOString();
    channel.lastError = null;
    const subscription = {
      APIKey: API_KEY,
      BoundingBoxes: channel.name === "ship"
        ? [[[-90, -180], [90, 180]]]
        : controlBoundingBox(),
      FilterMessageTypes: ["PositionReport"]
    };
    if (channel.name === "ship") subscription.FiltersShipMMSI = [MMSI];
    channel.socket.send(JSON.stringify(subscription));
    console.log(`Connected ${channel.name} AIS feed`);
  });

  channel.socket.on("message", async (data) => {
    channel.lastMessageAt = new Date().toISOString();
    try {
      const message = JSON.parse(data.toString());
      if (message.MessageType !== "PositionReport") return;
      channel.lastPositionAt = new Date().toISOString();
      channel.reconnectAttempt = 0;
      if (channel.name === "ship") await savePosition(message);
    } catch (error) {
      channel.lastError = error.message;
      console.error(`Could not process ${channel.name} AIS message:`, error.message);
    }
  });

  channel.socket.on("error", (error) => {
    channel.lastError = error.message;
    console.error(`${channel.name} AIS feed error:`, error.message);
  });

  channel.socket.on("close", (code, reason) => {
    channel.socket = undefined;
    if (!shuttingDown) {
      console.log(`${channel.name} AIS feed disconnected (${code}${reason.length ? `: ${reason}` : ""})`);
      scheduleReconnect(channel);
    }
  });
}

function ageSince(timestamp, fallback) {
  const value = timestamp || fallback;
  return value ? Date.now() - new Date(value).getTime() : Infinity;
}

function restartStaleFeeds() {
  const nearby = channels.nearby;
  if (nearby.state !== "connected") return;
  if (ageSince(nearby.lastPositionAt, nearby.connectedAt) <= FEED_STALE_MS) return;

  console.warn("Nearby AIS control feed is stale; reconnecting both feeds");
  for (const channel of Object.values(channels)) {
    if (channel.state === "connected") {
      channel.state = "stale";
      channel.socket?.terminate();
    }
  }
}

function sendJson(response, status, body) {
  response.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Cache-Control": "no-store"
  });
  response.end(`${JSON.stringify(body)}\n`);
}

const server = http.createServer((request, response) => {
  const path = new URL(request.url, `http://${request.headers.host || "localhost"}`).pathname;

  if (request.method === "GET" && path === "/positions") {
    return sendJson(response, 200, positions);
  }

  if (request.method === "GET" && path === "/health") {
    const ship = channels.ship;
    const nearby = channels.nearby;
    const nearbyAge = ageSince(nearby.lastPositionAt, nearby.connectedAt);
    const shipAge = ageSince(ship.lastPositionAt, ship.connectedAt);
    const disabled = AIS_DISABLED;
    const socketsConnected = ship.state === "connected" && nearby.state === "connected";

    let status = "ok";
    let diagnosis = "receiving_rotterdam";
    if (disabled) {
      diagnosis = "ais_disabled";
    } else if (!socketsConnected || nearbyAge > FEED_STALE_MS) {
      status = "degraded";
      diagnosis = "ais_feed_stale";
    } else if (!nearby.lastPositionAt) {
      status = "starting";
      diagnosis = "checking_ais_feed";
    } else if (!ship.lastPositionAt || shipAge > SHIP_SILENT_MS) {
      status = "warning";
      diagnosis = "no_rotterdam_reports";
    }

    const healthy = status !== "degraded";
    return sendJson(response, healthy ? 200 : 503, {
      status,
      diagnosis,
      ais: disabled ? "disabled" : ship.state,
      nearbyAis: disabled ? "disabled" : nearby.state,
      ship: SHIP_NAME,
      mmsi: MMSI,
      savedPositions: positions.length,
      lastPositionAt: positions.at(-1)?.timestamp || null,
      lastRotterdamReportAt: ship.lastPositionAt,
      lastNearbyShipReportAt: nearby.lastPositionAt,
      lastError: ship.lastError || nearby.lastError
    });
  }

  return sendJson(response, 404, { error: "Not found", endpoints: ["/health", "/positions"] });
});

async function start() {
  await loadPositions();
  server.listen(PORT, "0.0.0.0", () => {
    console.log(`HTTP service listening on port ${PORT}`);
    if (!AIS_DISABLED) {
      connectToAisStream(channels.ship);
      connectToAisStream(channels.nearby);
      watchdogTimer = setInterval(restartStaleFeeds, 5 * 60_000);
    }
  });
}

function stop(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`${signal} received; shutting down`);
  clearInterval(watchdogTimer);
  for (const channel of Object.values(channels)) {
    clearTimeout(channel.reconnectTimer);
    channel.socket?.close(1000, "Service shutting down");
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5_000).unref();
}

process.on("SIGINT", () => stop("SIGINT"));
process.on("SIGTERM", () => stop("SIGTERM"));

start().catch((error) => {
  console.error("Service failed to start:", error);
  process.exit(1);
});
