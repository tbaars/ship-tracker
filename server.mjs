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

let positions = [];
let socket;
let reconnectTimer;
let reconnectAttempt = 0;
let shuttingDown = false;
let lastMessageAt = null;
let lastError = null;
let connectionState = AIS_DISABLED ? "disabled" : "starting";

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

function scheduleReconnect() {
  if (shuttingDown || AIS_DISABLED) return;
  const delay = Math.min(60_000, 1_000 * 2 ** reconnectAttempt);
  reconnectAttempt += 1;
  connectionState = "reconnecting";
  console.log(`Reconnecting to AISStream in ${Math.round(delay / 1000)} second(s)`);
  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(connectToAisStream, delay);
}

function connectToAisStream() {
  if (!API_KEY) {
    connectionState = "missing_api_key";
    lastError = "AISSTREAM_API_KEY is not set";
    console.error(lastError);
    return;
  }

  connectionState = "connecting";
  socket = new WebSocket(AIS_URL, { perMessageDeflate: true });

  socket.on("open", () => {
    reconnectAttempt = 0;
    connectionState = "connected";
    lastError = null;
    socket.send(JSON.stringify({
      APIKey: API_KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FiltersShipMMSI: [MMSI],
      FilterMessageTypes: ["PositionReport"]
    }));
    console.log(`Connected to AISStream for ${SHIP_NAME} (${MMSI})`);
  });

  socket.on("message", async (data) => {
    lastMessageAt = new Date().toISOString();
    try {
      const message = JSON.parse(data.toString());
      if (message.MessageType === "PositionReport") await savePosition(message);
    } catch (error) {
      lastError = error.message;
      console.error("Could not process AIS message:", error.message);
    }
  });

  socket.on("error", (error) => {
    lastError = error.message;
    console.error("AISStream error:", error.message);
  });

  socket.on("close", (code, reason) => {
    socket = undefined;
    if (!shuttingDown) {
      console.log(`AISStream disconnected (${code}${reason.length ? `: ${reason}` : ""})`);
      scheduleReconnect();
    }
  });
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
    const healthy = connectionState === "connected" || connectionState === "disabled";
    return sendJson(response, healthy ? 200 : 503, {
      status: healthy ? "ok" : "degraded",
      ais: connectionState,
      ship: SHIP_NAME,
      mmsi: MMSI,
      savedPositions: positions.length,
      lastPositionAt: positions.at(-1)?.timestamp || null,
      lastMessageAt,
      lastError
    });
  }

  return sendJson(response, 404, { error: "Not found", endpoints: ["/health", "/positions"] });
});

async function start() {
  await loadPositions();
  server.listen(PORT, "0.0.0.0", () => {
    console.log(`HTTP service listening on port ${PORT}`);
    if (!AIS_DISABLED) connectToAisStream();
  });
}

function stop(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`${signal} received; shutting down`);
  clearTimeout(reconnectTimer);
  socket?.close(1000, "Service shutting down");
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5_000).unref();
}

process.on("SIGINT", () => stop("SIGINT"));
process.on("SIGTERM", () => stop("SIGTERM"));

start().catch((error) => {
  console.error("Service failed to start:", error);
  process.exit(1);
});
