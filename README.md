# Reversal API (Render-ready)

Production Node.js service that collects intraday bars via Yahoo, detects reversal patterns, stores them in Postgres, and exposes REST endpoints. Aligned with `reversal_schema_v2.sql`.

## Quick Deploy (Render)

**Prereqs**: A Render PostgreSQL instance (use `reversal_schema_v2.sql` to initialize).

1. Create a new **Web Service** from this repo or zip upload.
2. Set **Build Command**: _none needed_ (Render auto-installs dependencies)
3. Set **Start Command**: `node reversal_api_server.js`
4. Set **Environment** -> **Environment Variables**:
   - `DATABASE_URL` (from Render Postgres)
   - `SYMBOLS` e.g. `ES=F,NQ=F`
   - `TIMEFRAMES` e.g. `1m,5m`
   - `RETENTION_DAYS` e.g. `120`
   - `SLICE_HOURS` e.g. `6`
   - `YAHOO_RATE_LIMIT_MS` e.g. `1000`
   - `TZ` e.g. `America/New_York`
   - `PORT` (Render sets automatically; optional override)

> Note: Node 18+ is required (Render default is fine).

## Endpoints

- `GET /health`
- `POST /api/process` — body: `{ symbol, timeframe, startDate, endDate }`
- `POST /api/label` — body: `{ daysBack?: number }`
- `POST /api/admin/daily_collect`
- `POST /api/admin/housekeeping`
- `GET /api/status/:runId`
- `GET /api/jobs?limit=20&jobType=...&status=...`
- `GET /api/stats?symbol=...&timeframe=...&days=7`
- `GET /api/reversals?symbol=...&timeframe=...&outcome=...&trend=...&minQuality=...&limit=50`
- `GET /api/training` *(501 unless you add the optional view)*
- `GET /api/neighbors` *(501 unless you add the optional function)*

## Database

Run `reversal_schema_v2.sql` on your Postgres (Render → PSQL).  
This creates tables, indexes, triggers, and housekeeping functions.

### Optional (to enable /api/training and /api/neighbors)

Create:
- Materialized view `training_fingerprints`
- SQL function `get_neighbor_patterns(...)`

You can add these later; the API will return 501 until present.

## Local Dev

```bash
cp .env.example .env
npm install
npm run dev
```

Then visit `http://localhost:${PORT || 3000}/health`.

## Notes

- Cron jobs run **only in production** and use `TZ` to schedule.
- Yahoo Finance limits: tune `YAHOO_RATE_LIMIT_MS` if you hit throttling.
- The service aligns with the schema fields (`base_points`, `total_points`, `point_tier` etc.).

