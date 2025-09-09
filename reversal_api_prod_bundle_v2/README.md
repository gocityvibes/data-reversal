# Reversal Data Collection API — Production Bundle (Points-Enabled)

This bundle includes:
- `server.js` — hardened production server (Node 18+) with API key auth, validation, rate limiting, CORS allow-list,
  Yahoo retry/backoff, concurrency control, ET RTH windows, and **points** computed and persisted.
- `migrations/001_points_and_archive.sql` — adds `base_points`, `total_points`, `point_tier` to live & archive and updates the archive function.
- `.env.sample` — copy to `.env` and fill values.

## Install
```
npm install express cors helmet express-rate-limit pg yahoo-finance2 node-cron uuid luxon dotenv
```

## Migrate DB
Apply your base schema first, then run:
```
psql "$DATABASE_URL" -f migrations/001_points_and_archive.sql
```

## Run
```
NODE_ENV=production node server.js
```

## Auth
All `/api/*` routes require `x-api-key: <ADMIN_API_KEY>`. Health check `/health` is public.
