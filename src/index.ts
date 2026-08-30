import { DurableObject } from "cloudflare:workers";

const ROTTERDAM_MMSI = "245464000";

export class MyDurableObject extends DurableObject {
  private socket: WebSocket | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // Create our little database the first time the tracker runs.
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        lat REAL NOT NULL,
        lon REAL NOT NULL,
        sog REAL,
        cog REAL
      )
    `);
  }

  async start(): Promise<string> {
    // Don't create a second connection if we're already connected.
    if (
      this.socket &&
      (this.socket.readyState === WebSocket.OPEN ||
        this.socket.readyState === WebSocket.CONNECTING)
    ) {
      return "already connected";
    }

    console.log("Connecting to AISStream...");

    const socket = new WebSocket(
      "wss://stream.aisstream.io/v0/stream"
    );

socket.binaryType = "arraybuffer";

    this.socket = socket;

    socket.addEventListener("open", () => {
      console.log("Connected to AISStream");

console.log("Sending AISStream subscription");

      socket.send(
        JSON.stringify({
          APIKey: this.env.AISSTREAM_API_KEY,

          BoundingBoxes: [
            [
              [-90, -180],
              [90, 180]
            ]
          ],

          FiltersShipMMSI: [ROTTERDAM_MMSI],
          FilterMessageTypes: ["PositionReport"]
        })
      );
    });

    socket.addEventListener("message", async (event) => {
      try {
        let text: string;

if (typeof event.data === "string") {
  text = event.data;
} else if (event.data instanceof ArrayBuffer) {
  text = new TextDecoder().decode(event.data);
} else if (event.data instanceof Blob) {
  text = await event.data.text();
} else {
  console.log("Unknown AIS message type");
  return;
}

const message = JSON.parse(text);

console.log("AIS message type:", message.MessageType);

        if (message.MessageType !== "PositionReport") {
          return;
        }

        const lat = message.MetaData?.Latitude;
        const lon = message.MetaData?.Longitude;

        if (typeof lat !== "number" || typeof lon !== "number") {
          return;
        }

        const report = message.Message?.PositionReport ?? {};

        const latest = [
          ...this.ctx.storage.sql.exec<{ timestamp: string }>(`
            SELECT timestamp
            FROM positions
            ORDER BY id DESC
            LIMIT 1
          `)
        ][0];

        const now = new Date();

        // Keep roughly one location every 30 minutes.
        if (latest) {
          const lastTime = new Date(latest.timestamp);

          const minutesSince =
            (now.getTime() - lastTime.getTime()) / 1000 / 60;

          if (minutesSince < 30) {
            return;
          }
        }

        this.ctx.storage.sql.exec(
          `
          INSERT INTO positions
            (timestamp, lat, lon, sog, cog)
          VALUES (?, ?, ?, ?, ?)
          `,
          now.toISOString(),
          lat,
          lon,
          report.Sog ?? null,
          report.Cog ?? null
        );

        console.log(
          `Saved Rotterdam position: ${lat}, ${lon}`
        );
      } catch (error) {
        console.error("AIS message error:", error);
      }
    });

   socket.addEventListener("close", (event) => {
  console.log(
    "AISStream disconnected",
    "code:", event.code,
    "reason:", event.reason,
    "clean:", event.wasClean
  );

  this.socket = null;

  this.ctx.storage.setAlarm(Date.now() + 60_000);
});

    socket.addEventListener("error", (error) => {
      console.error("AISStream WebSocket error:", error);
    });

    return "connecting";
  }

  async alarm() {
    await this.start();
  }

  async getPositions() {
    return [
      ...this.ctx.storage.sql.exec(`
        SELECT timestamp, lat, lon, sog, cog
        FROM positions
        ORDER BY id ASC
      `)
    ];
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    const id =
      env.MY_DURABLE_OBJECT.idFromName("rotterdam");

    const tracker =
      env.MY_DURABLE_OBJECT.get(id);

    if (url.pathname === "/start") {
      const status = await tracker.start();

      return Response.json(
        { status },
        {
          headers: {
            "Access-Control-Allow-Origin": "*"
          }
        }
      );
    }

    if (url.pathname === "/positions") {
      const positions = await tracker.getPositions();

      return Response.json(positions, {
        headers: {
          "Access-Control-Allow-Origin": "*"
        }
      });
    }

    return new Response(
`🚢 Nana and Opa Tracker

Rotterdam MMSI: ${ROTTERDAM_MMSI}

/start       Start the AIS collector
/positions   See the saved route`
    );
  }
};