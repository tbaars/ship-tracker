# Nana and Opa Cruise Tracker

This repository contains the public ship-tracker map and the Node collector
that listens for AIS position reports from **Rotterdam** (MMSI `245464000`).
The collector saves at most one point every 30 minutes using the ship's AIS
timestamp and makes the saved route available over HTTP.

## Run locally

1. Copy `.env.example` to `.env`.
2. Put your AISStream key after `AISSTREAM_API_KEY=` in `.env`.
3. Run `npm start`.
4. Open `http://localhost:3000/health` and
   `http://localhost:3000/positions`.

The `.env` file and saved `data/` folder are ignored by Git.

## HTTP endpoints

- `GET /health` reports the AIS connection and most recently saved point.
- `GET /positions` returns all saved positions as JSON.

## Railway preparation

The app reads Railway's `PORT` automatically. Set `AISSTREAM_API_KEY` as a
Railway variable. For storage that survives deployments, mount a Railway
volume at `/data` and set `DATA_FILE=/data/positions.json`.
