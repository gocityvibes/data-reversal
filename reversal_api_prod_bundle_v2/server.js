// (server.js) — Production-hardened Reversal Data Collection API
// Includes: API key auth, validation, rate limiting, CORS allow-list, retry/backoff, concurrency control,
// indicator alignment with null safety, points computation & persistence, ET RTH windowing via Luxon.
// Requires: Node 18+
// NOTE: This file is intended to work with the SQL migration included in this bundle.

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { Pool } = require('pg');
const yahooFinance = require('yahoo-finance2').default;
const cron = require('node-cron');
const { v4: uuidv4 } = require('uuid');
const { DateTime } = require('luxon');
require('dotenv').config();

if (!process.env.DATABASE_URL) { console.error('[FATAL] DATABASE_URL is required'); process.exit(1); }
if (!process.env.ADMIN_API_KEY) { console.error('[FATAL] ADMIN_API_KEY is required'); process.exit(1); }
if (typeof fetch !== 'function') { console.error('[FATAL] Node 18+ is required'); process.exit(1); }

const app = express();
const port = process.env.PORT || 3000;

const config = {
  symbols: (process.env.SYMBOLS || 'ES=F').split(',').map(s => s.trim()).filter(Boolean),
  timeframes: (process.env.TIMEFRAMES || '1m,5m').split(',').map(t => t.trim()).filter(Boolean),
  retentionDays: parseInt(process.env.RETENTION_DAYS || '120', 10),
  sliceHours: parseInt(process.env.SLICE_HOURS || '6', 10),
  sliceOverlapMinutes: parseInt(process.env.SLICE_OVERLAP_MIN || '2', 10),
  yahooRateLimit: parseInt(process.env.YAHOO_RATE_LIMIT_MS || '1000', 10),
  yahooMaxRetries: parseInt(process.env.YAHOO_MAX_RETRIES || '2', 10),
  yahooBackoffMs: parseInt(process.env.YAHOO_BACKOFF_MS || '500', 10),
  timezone: process.env.TZ || 'America/New_York',
  maxConcurrency: parseInt(process.env.MAX_CONCURRENCY || '3', 10),
  corsOrigins: (process.env.CORS_ORIGINS || '').split(',').map(o => o.trim()).filter(Boolean),
};

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

app.set('trust proxy', 1);
app.use(helmet());

if (config.corsOrigins.length) {
  app.use(cors({
    origin: (origin, cb) => {
      if (!origin) return cb(null, true);
      if (config.corsOrigins.includes(origin)) return cb(null, true);
      return cb(new Error('Not allowed by CORS'));
    },
  }));
}

const limiter = rateLimit({
  windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS || '60000', 10),
  max: parseInt(process.env.RATE_LIMIT_MAX || '120', 10),
  standardHeaders: true,
  legacyHeaders: false,
});
app.use('/api', limiter);
app.use(express.json({ limit: '512kb' }));

function requireApiKey(req, res, next) {
  const key = req.headers['x-api-key'] || req.query.api_key;
  if (!key || key !== process.env.ADMIN_API_KEY) return res.status(401).json({ error: 'Unauthorized' });
  return next();
}

function assertSymbol(s) { return typeof s === 'string' && config.symbols.includes(s); }
function assertTimeframe(tf) { return typeof tf === 'string' && config.timeframes.includes(tf); }
function parseDateSafe(d) { const dt = new Date(d); return isNaN(dt.getTime()) ? null : dt; }
function clampInt(n, lo, hi, def) { const v = parseInt(n, 10); return Number.isNaN(v) ? def : Math.min(hi, Math.max(lo, v)); }

class TechnicalIndicators {
  static sma(values, period) {
    const len = values.length, res = Array(len).fill(null);
    for (let i = period - 1; i < len; i++) {
      let sum = 0; for (let j = i - period + 1; j <= i; j++) sum += values[j];
      res[i] = sum / period;
    }
    return res;
  }
  static ema(values, period) {
    const len = values.length, res = Array(len).fill(null);
    if (len < period) return res;
    const k = 2 / (period + 1);
    let sma = 0; for (let i = 0; i < period; i++) sma += values[i]; sma /= period;
    res[period - 1] = sma;
    for (let i = period; i < len; i++) res[i] = (values[i] - res[i - 1]) * k + res[i - 1];
    return res;
  }
  static rsi(values, period = 14) {
    const len = values.length, res = Array(len).fill(null);
    if (len < period) return res;
    const gains = Array(len).fill(0), losses = Array(len).fill(0);
    for (let i = 1; i < len; i++) { const ch = values[i] - values[i - 1]; gains[i] = ch > 0 ? ch : 0; losses[i] = ch < 0 ? -ch : 0; }
    for (let i = period; i < len; i++) {
      let avgGain = 0, avgLoss = 0;
      for (let j = i - period + 1; j <= i; j++) { avgGain += gains[j]; avgLoss += losses[j]; }
      avgGain /= period; avgLoss /= period;
      const rs = avgLoss === 0 ? (avgGain === 0 ? 0 : 1000) : avgGain / avgLoss;
      res[i] = 100 - (100 / (1 + rs));
    }
    return res;
  }
  static atr(high, low, close, period = 14) {
    const len = close.length, res = Array(len).fill(null);
    if (len < period + 1) return res;
    const tr = Array(len).fill(null);
    for (let i = 1; i < len; i++) {
      const tr1 = high[i] - low[i];
      const tr2 = Math.abs(high[i] - close[i - 1]);
      const tr3 = Math.abs(low[i] - close[i - 1]);
      tr[i] = Math.max(tr1, tr2, tr3);
    }
    for (let i = period; i < len; i++) {
      let sum = 0; for (let j = i - period + 1; j <= i; j++) sum += tr[j] ?? 0;
      res[i] = sum / period;
    }
    return res;
  }
  static vwap(high, low, close, volume) {
    const len = close.length, res = Array(len).fill(null);
    let cpv = 0, cvol = 0;
    for (let i = 0; i < len; i++) {
      const tp = (high[i] + low[i] + close[i]) / 3;
      cpv += tp * (volume[i] || 0);
      cvol += (volume[i] || 0);
      res[i] = cvol > 0 ? cpv / cvol : close[i];
    }
    return res;
  }
}

class ReversalDetector {
  constructor(tf = '1m') { this.timeframe = tf; this.config = this.getConfig(tf); }
  getConfig(tf) {
    const base = { rsiOverbought: 70, rsiOversold: 30, atrStretchMin: 1.0, followThroughAtrMin: 1.0 };
    if (tf === '1m') return { ...base, relVolumeMin: 2.0, followThroughBarsMin: 5, confirmationBarsMax: 5 };
    if (tf === '5m') return { ...base, atrStretchMin: 0.8, relVolumeMin: 1.5, followThroughBarsMin: 3, confirmationBarsMax: 3 };
    return base;
  }
  detectSwingPoints(high, low, lookback = 3) {
    const higherHighs = [], lowerLows = [], higherLows = [], lowerHighs = [];
    for (let i = lookback; i < high.length - lookback; i++) {
      if (high.slice(i - lookback, i).every(h => h < high[i]) && high.slice(i + 1, i + lookback + 1).every(h => h < high[i])) higherHighs.push({ index: i, price: high[i] });
      if (low.slice(i - lookback, i).every(l => l > low[i]) && low.slice(i + 1, i + lookback + 1).every(l => l > low[i])) lowerLows.push({ index: i, price: low[i] });
      if (low.slice(i - lookback, i).some(l => l < low[i]) && low.slice(i + 1, i + lookback + 1).every(l => l > low[i])) higherLows.push({ index: i, price: low[i] });
      if (high.slice(i - lookback, i).some(h => h > high[i]) && high.slice(i + 1, i + lookback + 1).every(h => h < high[i])) lowerHighs.push({ index: i, price: high[i] });
    }
    return { higherHighs, lowerLows, higherLows, lowerHighs };
  }
  calculatePoints(rev, exhaustion, structureBreak, confirmation, followThrough) {
    const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
    let trend_points = 5;
    if (rev.bars_since_peak_trough <= 10) trend_points = 15;
    else if (rev.bars_since_peak_trough <= 20) trend_points = 10;

    let exhaustion_points = 0;
    if (rev.rsi >= 75 || rev.rsi <= 25) exhaustion_points += 8;
    else if (rev.rsi >= 70 || rev.rsi <= 30) exhaustion_points += 5;
    if (rev.move_vs_atr >= 2.0) exhaustion_points += 7;
    else if (rev.move_vs_atr >= 1.5) exhaustion_points += 4;
    if (rev.vol_climax_flag) exhaustion_points += 5;
    exhaustion_points = Math.min(20, exhaustion_points);

    const structure_points = (structureBreak.brokeHigherLow || structureBreak.brokeLowerHigh) ? 15 : 5;

    let confirmation_points = 0;
    if (confirmation.emaCross !== 'none') confirmation_points += 5;
    if (confirmation.rsiCross !== 'none') confirmation_points += 5;
    if (confirmation.vwapRetest === 'accept') confirmation_points += 5;
    confirmation_points = Math.max(5, confirmation_points);

    let follow_points = 5;
    if (followThrough.atr >= 1.5) follow_points = 15;
    else if (followThrough.atr >= 1.0) follow_points = 10;

    let bonus_points = 0;
    if (Math.abs(rev.price_to_vwap_pct) < 0.1) bonus_points += 3;
    if (rev.rel_vol_20 >= 3.0) bonus_points += 3;
    if (this.timeframe === '1m' && (rev.rsi >= 80 || rev.rsi <= 20)) bonus_points += 4;

    let penalty_points = 0;
    if (rev.move_vs_atr < 0.8) penalty_points += 5;
    if (rev.rel_vol_20 < 1.2) penalty_points += 3;

    const base_points = trend_points + exhaustion_points + structure_points + confirmation_points + follow_points;
    const total_points = clamp(base_points + bonus_points - penalty_points, 0, 100);

    let point_tier = 'C';
    if (total_points >= 85) point_tier = 'S';
    else if (total_points >= 70) point_tier = 'A';
    else if (total_points >= 55) point_tier = 'B';

    return { base_points, total_points, point_tier };
  }
  detectReversals(ohlcv, timestamps) {
    const { high, low, close, volume } = ohlcv;
    const reversals = [];
    const rsi = TechnicalIndicators.rsi(close);
    const atr = TechnicalIndicators.atr(high, low, close);
    const ema8 = TechnicalIndicators.ema(close, 8);
    const ema21 = TechnicalIndicators.ema(close, 21);
    const vwap = TechnicalIndicators.vwap(high, low, close, volume);
    const relVol = volume.map((v, i) => {
      if (i < 20) return 1.0;
      let sum = 0; for (let j = i - 19; j <= i; j++) sum += volume[j] || 0;
      return (volume[i] || 0) / (sum / 20);
    });
    const swing = this.detectSwingPoints(high, low);
    const startIdx = Math.max(30, 21, 14);
    for (let i = startIdx; i < close.length - 10; i++) {
      if (rsi[i] == null || atr[i] == null || ema8[i] == null || ema21[i] == null) continue;
      const rev = this.checkReversal(i, { high, low, close }, { rsi, atr, ema8, ema21, vwap, relVol }, swing, timestamps[i]);
      if (rev) reversals.push(rev);
    }
    return reversals;
  }
  checkReversal(index, ohlcv, ind, swing, timestamp) {
    const { high, low, close } = ohlcv;
    const { rsi, atr, ema8, ema21, vwap, relVol } = ind;
    const trend = this.trendContext(index, swing);
    if (!trend.hasTrend) return null;
    const exhaustion = this.exhaustion(index, { close, rsi, atr, ema21, vwap, relVol });
    if (!exhaustion.hasExhaustion) return null;
    const sBreak = this.structureBreak(index, trend, swing, { high, low });
    if (!sBreak.hasBreak) return null;
    const confirm = this.confirmation(index, { rsi, ema8, ema21, vwap, close });
    if (!confirm.hasConfirmation) return null;
    const follow = this.followThrough(index, { high, low, close }, atr, trend.direction);
    if (!follow.hasFollowThrough) return null;

    const rev = {
      timestamp, timeframe: this.timeframe, index,
      swing_trend_incoming: trend.direction,
      broke_last_higher_low: sBreak.brokeHigherLow,
      broke_last_lower_high: sBreak.brokeLowerHigh,
      bars_since_peak_trough: trend.barsSincePeakTrough,
      rsi: rsi[index],
      rsi_lookback_max: this.rsiLookback(rsi, index, Math.max, -Infinity),
      rsi_lookback_min: this.rsiLookback(rsi, index, Math.min, Infinity),
      move_vs_atr: exhaustion.atrStretch,
      price_to_vwap_pct: ((close[index] - vwap[index]) / vwap[index]) * 100,
      price_to_ema21_pct: ((close[index] - ema21[index]) / ema21[index]) * 100,
      rel_vol_20: relVol[index],
      vol_climax_flag: relVol[index] >= this.config.relVolumeMin,
      ema8_cross_ema21: confirm.emaCross,
      rsi_cross_50: confirm.rsiCross,
      vwap_retest_result: confirm.vwapRetest,
      follow_through_atr: follow.atr,
      follow_through_bars_10: follow.bars
    };
    const pts = this.calculatePoints(rev, exhaustion, sBreak, confirm, follow);
    rev.base_points = Number.isFinite(pts.base_points) ? Math.round(pts.base_points) : null;
    rev.total_points = Number.isFinite(pts.total_points) ? Math.round(pts.total_points) : null;
    rev.point_tier = pts.point_tier || null;
    return rev;
  }
  rsiLookback(rsi, idx, fn, seed) { const start = Math.max(0, idx - 10); let acc = seed; for (let i = start; i <= idx; i++) if (rsi[i] != null) acc = fn(acc, rsi[i]); return acc === seed ? rsi[idx] : acc; }
  trendContext(index, swing) {
    const look = this.timeframe === '1m' ? 50 : 25;
    const lhs = swing.lowerHighs.filter(lh => lh.index < index && lh.index > index - look);
    const hls = swing.higherLows.filter(hl => hl.index < index && hl.index > index - look);
    if (lhs.length >= 2) return { hasTrend: true, direction: 'downtrend', barsSincePeakTrough: index - lhs[lhs.length - 1].index };
    if (hls.length >= 2) return { hasTrend: true, direction: 'uptrend', barsSincePeakTrough: index - hls[hls.length - 1].index };
    return { hasTrend: false };
  }
  exhaustion(index, { close, rsi, atr, ema21, vwap, relVol }) {
    let has = false; let stretch = 0;
    if (rsi[index] >= this.config.rsiOverbought || rsi[index] <= this.config.rsiOversold) has = true;
    if (atr[index] != null && ema21[index] != null) {
      stretch = Math.abs(close[index] - ema21[index]) / (atr[index] || 1e-9);
      if (stretch >= this.config.atrStretchMin) has = true;
    }
    if (relVol[index] >= this.config.relVolumeMin) has = true;
    return { hasExhaustion: has, atrStretch: stretch };
  }
  structureBreak(index, trend, swing, { high, low }) {
    let has = false, brokeHL = false, brokeLH = false;
    if (trend.direction === 'uptrend') {
      const hls = swing.higherLows.filter(hl => hl.index < index);
      if (hls.length) { const last = hls[hls.length - 1]; if (low[index] < last.price) { has = true; brokeHL = true; } }
    } else if (trend.direction === 'downtrend') {
      const lhs = swing.lowerHighs.filter(lh => lh.index < index);
      if (lhs.length) { const last = lhs[lhs.length - 1]; if (high[index] > last.price) { has = true; brokeLH = true; } }
    }
    return { hasBreak: has, brokeHigherLow: brokeHL, brokeLowerHigh: brokeLH };
  }
  confirmation(index, { rsi, ema8, ema21, vwap, close }) {
    let has = false, emaCross = 'none', rsiCross = 'none', vwapRetest = 'none';
    const maxI = Math.min(index + (this.config.confirmationBarsMax || 3), close.length - 1);
    for (let i = index; i <= maxI; i++) {
      if (ema8[i] == null || ema21[i] == null || rsi[i] == null) continue;
      if (i > 0 && ema8[i - 1] != null && ema21[i - 1] != null) {
        if (ema8[i - 1] <= ema21[i - 1] && ema8[i] > ema21[i]) { emaCross = 'up'; has = true; }
        else if (ema8[i - 1] >= ema21[i - 1] && ema8[i] < ema21[i]) { emaCross = 'down'; has = true; }
      }
      if (i > 0 && rsi[i - 1] != null) {
        if (rsi[i - 1] <= 50 && rsi[i] > 50) { rsiCross = 'up'; has = true; }
        else if (rsi[i - 1] >= 50 && rsi[i] < 50) { rsiCross = 'down'; has = true; }
      }
      const vdist = Math.abs(close[i] - vwap[i]) / vwap[i];
      if (vdist < 0.002) { vwapRetest = 'accept'; has = true; }
    }
    return { hasConfirmation: has, emaCross, rsiCross, vwapRetest };
  }
  followThrough(index, { close }, atr, dir) {
    let ftAtr = 0, bars = 0;
    const start = close[index];
    let maxMove = 0;
    const checkBars = this.timeframe === '1m' ? 10 : 6;
    for (let i = index + 1; i < Math.min(close.length, index + 1 + checkBars); i++) {
      const move = dir === 'uptrend' ? (close[i] - start) : (start - close[i]);
      if (move > maxMove) maxMove = move;
      if (move > 0) bars++;
    }
    if (atr[index] != null && atr[index] > 0) ftAtr = maxMove / atr[index];
    const ok = (ftAtr >= this.config.followThroughAtrMin) || (bars >= this.config.followThroughBarsMin);
    return { hasFollowThrough: ok, atr: ftAtr, bars };
  }
}

class DataCollectionService {
  constructor() { this.detectors = Object.fromEntries(config.timeframes.map(tf => [tf, new ReversalDetector(tf)])); }
  sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
  async yahooHistoricalWithRetry(symbol, options, interval) {
    let attempt = 0, delay = config.yahooBackoffMs;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        await this.sleep(config.yahooRateLimit);
        const result = await yahooFinance.historical(symbol, options);
        if (!result || result.length === 0) throw new Error(`No data for ${symbol} ${interval}`);
        return result;
      } catch (err) {
        attempt++; if (attempt > config.yahooMaxRetries) throw err;
        const jitter = Math.floor(Math.random() * 200);
        await this.sleep(delay + jitter); delay *= 2;
      }
    }
  }
  async fetchYahooData(symbol, startDate, endDate, interval = '1m') {
    const options = { period1: startDate, period2: endDate, interval, includeAdjustedClose: true };
    const rows = await this.yahooHistoricalWithRetry(symbol, options, interval);
    return {
      timestamps: rows.map(r => r.date),
      open: rows.map(r => r.open),
      high: rows.map(r => r.high),
      low: rows.map(r => r.low),
      close: rows.map(r => r.close),
      volume: rows.map(r => r.volume || 0)
    };
  }
  createTimeSlices(start, end, sliceHours) {
    const slices = [], step = sliceHours * 60 * 60 * 1000, overlap = config.sliceOverlapMinutes * 60 * 1000;
    let s = new Date(start);
    while (s < end) {
      const e = new Date(Math.min(s.getTime() + step, end.getTime()));
      slices.push({ start: new Date(s), end: new Date(e) });
      s = new Date(e.getTime() - overlap);
    }
    return slices;
  }
  async processTimeframeData(symbol, timeframe, startDate, endDate, runId) {
    const client = await pool.connect();
    const startTime = Date.now();
    let lastSuccessfulSliceEnd = startDate;
    try {
      const slices = this.createTimeSlices(startDate, endDate, config.sliceHours);
      let bars = 0, revs = 0, ins = 0;
      for (const slice of slices) {
        try {
          const ohlcv = await this.fetchYahooData(symbol, slice.start, slice.end, timeframe);
          if (!ohlcv.timestamps.length) continue;
          const reversals = this.detectors[timeframe].detectReversals(ohlcv, ohlcv.timestamps);
          bars += ohlcv.timestamps.length; revs += reversals.length;
          for (const rev of reversals) {
            try {
              const base_points = Number.isFinite(rev.base_points) ? rev.base_points : null;
              const total_points = Number.isFinite(rev.total_points) ? rev.total_points : null;
              const point_tier = typeof rev.point_tier === 'string' ? rev.point_tier : null;
              await client.query(`
                INSERT INTO reversals (
                  symbol, timeframe, "timestamp", swing_trend_incoming,
                  broke_last_higher_low, broke_last_lower_high, bars_since_peak_trough,
                  rsi, rsi_lookback_max, rsi_lookback_min, move_vs_atr,
                  price_to_vwap_pct, price_to_ema21_pct, rel_vol_20, vol_climax_flag,
                  ema8_cross_ema21, rsi_cross_50, vwap_retest_result,
                  follow_through_atr, follow_through_bars_10,
                  base_points, total_points, point_tier, run_id, source
                ) VALUES (
                  $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25
                ) ON CONFLICT (symbol, timeframe, "timestamp") DO NOTHING
              `, [
                symbol, timeframe, rev.timestamp, rev.swing_trend_incoming,
                rev.broke_last_higher_low, rev.broke_last_lower_high, rev.bars_since_peak_trough,
                rev.rsi, rev.rsi_lookback_max, rev.rsi_lookback_min, rev.move_vs_atr,
                rev.price_to_vwap_pct, rev.price_to_ema21_pct, rev.rel_vol_20, rev.vol_climax_flag,
                rev.ema8_cross_ema21, rev.rsi_cross_50, rev.vwap_retest_result,
                rev.follow_through_atr, rev.follow_through_bars_10,
                base_points, total_points, point_tier, runId, 'daily_refresh'
              ]);
              ins++;
            } catch {}
          }
          lastSuccessfulSliceEnd = slice.end;
        } catch {}
      }
      await client.query(`
        INSERT INTO processing_watermarks (symbol, timeframe, last_processed_ts, last_successful_run)
        VALUES ($1,$2,$3,NOW())
        ON CONFLICT (symbol, timeframe) DO UPDATE SET
          last_processed_ts = $3, last_successful_run = NOW(), consecutive_failures = 0, updated_at = NOW()
      `, [symbol, timeframe, lastSuccessfulSliceEnd]);
      const duration = Math.floor((Date.now() - startTime) / 1000);
      return { symbol, timeframe, barsProcessed: bars, reversalsDetected: revs, reversalsInserted: ins, duration };
    } catch (err) {
      await client.query(`
        INSERT INTO processing_watermarks (symbol, timeframe, last_processed_ts, consecutive_failures)
        VALUES ($1,$2,$3,1)
        ON CONFLICT (symbol, timeframe) DO UPDATE SET
          consecutive_failures = processing_watermarks.consecutive_failures + 1, updated_at = NOW()
      `, [symbol, timeframe, lastSuccessfulSliceEnd || startDate]);
      throw err;
    } finally { client.release(); }
  }
  async labelReversals(daysBack = 1) {
    const client = await pool.connect();
    try {
      const cutoff1m = new Date(Date.now() - (20 * 60 * 1000) - (daysBack * 24 * 60 * 60 * 1000));
      const cutoff5m = new Date(Date.now() - (15 * 60 * 1000) - (daysBack * 24 * 60 * 60 * 1000));
      const { rows } = await client.query(`
        SELECT * FROM reversals
         WHERE outcome='pending'
           AND ((timeframe='1m' AND "timestamp" < $1) OR (timeframe='5m' AND "timestamp" < $2))
         ORDER BY "timestamp" DESC LIMIT 200
      `, [cutoff1m, cutoff5m]);
      let labeled = 0;
      for (const r of rows) {
        try {
          const mins = r.timeframe === '1m' ? 30 : 25;
          const end = new Date(new Date(r.timestamp).getTime() + mins * 60 * 1000);
          const ohlcv = await this.fetchYahooData(r.symbol, r.timestamp, end, r.timeframe);
          if (!ohlcv.close || ohlcv.close.length < 10) continue;
          const label = this.calculateOutcomeLabel(ohlcv, r.swing_trend_incoming, r.timeframe);
          await client.query(`
            UPDATE reversals SET outcome=$1, quality_score=$2, mfe_atr=$3, mae_atr=$4,
              rr_achieved=$5, time_to_target_bars=$6, time_to_stop_bars=$7, structure_invalidated=$8, updated_at=NOW()
              WHERE id=$9
          `, [label.outcome, label.qualityScore, label.mfeAtr, label.maeAtr, label.rrAchieved, label.timeToTargetBars, label.timeToStopBars, label.structureInvalidated, r.id]);
          labeled++;
        } catch {}
      }
      return labeled;
    } finally { client.release(); }
  }
  calculateOutcomeLabel(ohlcv, trendDirection, timeframe) {
    const { close, high, low } = ohlcv;
    const entry = close[0];
    const atr = this.estimateATR(ohlcv);
    let mfe = 0, mae = 0, tTarget = null, tStop = null;
    const targetATR = timeframe === '1m' ? 1.5 : 1.2;
    const stopATR = timeframe === '1m' ? 0.7 : 0.6;
    const maxBars = timeframe === '1m' ? 20 : 15;
    const tPrice = trendDirection === 'uptrend' ? entry + targetATR * atr : entry - targetATR * atr;
    const sPrice = trendDirection === 'uptrend' ? entry - stopATR * atr : entry + stopATR * atr;
    for (let i = 1; i < Math.min(close.length, maxBars + 1); i++) {
      const p = close[i];
      if (trendDirection === 'uptrend') {
        const fav = (p - entry) / atr, adv = (entry - p) / atr;
        mfe = Math.max(mfe, fav); mae = Math.max(mae, adv);
        if (p >= tPrice && tTarget == null) tTarget = i;
        if (p <= sPrice && tStop == null) tStop = i;
      } else {
        const fav = (entry - p) / atr, adv = (p - entry) / atr;
        mfe = Math.max(mfe, fav); mae = Math.max(mae, adv);
        if (p <= tPrice && tTarget == null) tTarget = i;
        if (p >= sPrice && tStop == null) tStop = i;
      }
    }
    let outcome = 'neutral', quality = 50, rr = 0;
    const goldT = timeframe === '1m' ? 15 : 12;
    const goldMae = timeframe === '1m' ? 0.5 : 0.4;
    if (tTarget && tTarget <= goldT && mae <= goldMae) { outcome = 'gold'; quality = Math.min(100, Math.round(70 + (30 * (targetATR - mae) / targetATR) + (10 * (goldT - tTarget) / goldT))); rr = mfe; }
    else if ((tStop && tStop <= 10) || (mfe < 0.5 && mae > 0.7)) { outcome = 'hard_negative'; quality = Math.max(0, Math.round(30 - mae * 10)); rr = -mae; }
    else if (mfe >= 1.0 && mae <= 1.0) { outcome = 'neutral'; quality = Math.round(40 + mfe * 10); rr = mfe - mae; }
    else { outcome = 'neutral'; quality = Math.max(20, Math.round(50 - mae * 5)); rr = mfe - mae; }
    return {
      outcome, qualityScore: quality,
      mfeAtr: Math.round(mfe * 100) / 100, maeAtr: Math.round(mae * 100) / 100,
      rrAchieved: Math.round(rr * 100) / 100,
      timeToTargetBars: tTarget, timeToStopBars: tStop, structureInvalidated: false
    };
  }
  estimateATR(ohlcv, period = 14) {
    const { high, low, close } = ohlcv;
    if (high.length < period + 1) return (high[0] - low[0]) || 1;
    let sum = 0, count = 0;
    for (let i = 1; i < Math.min(high.length, period + 1); i++) {
      const tr1 = high[i] - low[i];
      const tr2 = Math.abs(high[i] - close[i - 1]);
      const tr3 = Math.abs(low[i] - close[i - 1]);
      sum += Math.max(tr1, tr2, tr3); count++;
    }
    return count ? sum / count : 1;
  }
  async archiveAndCleanup() {
    const client = await pool.connect();
    try {
      const archived = await client.query(`SELECT archive_labeled_reversals($1::INTERVAL)`, [`${config.retentionDays} days`]);
      const deleted  = await client.query(`SELECT cleanup_old_reversals($1::INTERVAL)`, [`${config.retentionDays} days`]);
      await client.query('SELECT update_system_stats()');
      await client.query('ANALYZE reversals, reversals_archive');
      return { archivedCount: archived.rows[0].archive_labeled_reversals, deletedCount: deleted.rows[0].cleanup_old_reversals };
    } finally { client.release(); }
  }
}

const dataService = new DataCollectionService();

function rthWindowFor(date = DateTime.now().setZone(config.timezone)) {
  const start = date.set({ hour: 9, minute: 30, second: 0, millisecond: 0 });
  const end = date.set({ hour: 16, minute: 0, second: 0, millisecond: 0 });
  return { start: start.toJSDate(), end: end.toJSDate() };
}
async function runWithConcurrency(tasks, max = 3) {
  const ret = []; let i = 0, active = 0;
  return await new Promise((resolve) => {
    const next = () => {
      while (active < max && i < tasks.length) {
        const cur = tasks[i++]();
        active++;
        cur.then(v => ret.push(v)).catch(e => ret.push({ error: String(e) })).finally(() => { active--; if (ret.length === tasks.length) resolve(ret); else next(); });
      }
    };
    next();
  });
}

app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'healthy', timestamp: new Date().toISOString(), config: { symbols: config.symbols, timeframes: config.timeframes, retentionDays: config.retentionDays } });
  } catch (e) {
    res.status(500).json({ status: 'unhealthy', error: e.message, timestamp: new Date().toISOString() });
  }
});

app.use('/api', requireApiKey);

app.post('/api/process', async (req, res) => {
  try {
    const { symbol, timeframe, startDate, endDate } = req.body || {};
    if (!assertSymbol(symbol) || !assertTimeframe(timeframe)) return res.status(400).json({ error: 'Invalid symbol/timeframe' });
    const start = parseDateSafe(startDate), end = parseDateSafe(endDate);
    if (!start || !end || start >= end) return res.status(400).json({ error: 'Invalid startDate/endDate' });

    const runId = uuidv4();
    await pool.query(`INSERT INTO job_runs (run_id, job_type, symbol, timeframe, start_date, end_date, status) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [runId, 'backfill', symbol, timeframe, start, end, 'running']);

    dataService.processTimeframeData(symbol, timeframe, start, end, runId)
      .then(async (r) => {
        await pool.query(`UPDATE job_runs SET status=$1, bars_processed=$2, reversals_detected=$3, duration_seconds=$4, completed_at=NOW() WHERE run_id=$5`,
          ['completed', r.barsProcessed, r.reversalsDetected, r.duration, runId]);
      })
      .catch(async (err) => {
        await pool.query(`UPDATE job_runs SET status=$1, error_message=$2, completed_at=NOW() WHERE run_id=$3`,
          ['failed', err.message, runId]);
      });

    res.json({ success: true, runId, message: 'Processing started', symbol, timeframe, startDate, endDate });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/label', async (req, res) => {
  try {
    const daysBack = clampInt((req.body || {}).daysBack ?? 1, 0, 7, 1);
    const runId = uuidv4();
    await pool.query(`INSERT INTO job_runs (run_id, job_type, status) VALUES ($1,$2,$3)`, [runId, 'labeling', 'running']);
    dataService.labelReversals(daysBack)
      .then(async (count) => { await pool.query(`UPDATE job_runs SET status=$1, reversals_labeled=$2, completed_at=NOW() WHERE run_id=$3`, ['completed', count, runId]); })
      .catch(async (err) => { await pool.query(`UPDATE job_runs SET status=$1, error_message=$2, completed_at=NOW() WHERE run_id=$3`, ['failed', err.message, runId]); });
    res.json({ success: true, runId, message: 'Labeling started', daysBack });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/daily_collect', async (req, res) => {
  try {
    const runId = uuidv4();
    const nowET = DateTime.now().setZone(config.timezone);
    const y = nowET.minus({ days: 1 });
    const { start: yStart } = rthWindowFor(y);
    const { end: tEnd } = rthWindowFor(nowET);

    await pool.query(`INSERT INTO job_runs (run_id, job_type, start_date, end_date, status) VALUES ($1,$2,$3,$4,$5)`,
      [runId, 'daily_refresh', yStart, tEnd, 'running']);

    let totalBars = 0, totalReversals = 0;
    const tasks = [];
    for (const symbol of config.symbols) {
      for (const timeframe of config.timeframes) {
        tasks.push(() => dataService.processTimeframeData(symbol, timeframe, yStart, tEnd, runId)
          .then(r => { totalBars += r.barsProcessed; totalReversals += r.reversalsDetected; return r; })
          .catch(err => ({ symbol, timeframe, error: err.message })));
      }
    }
    runWithConcurrency(tasks, config.maxConcurrency)
      .then(async (results) => {
        const hasErrors = results.some(r => r && r.error);
        await pool.query(`UPDATE job_runs SET status=$1, bars_processed=$2, reversals_detected=$3, completed_at=NOW() WHERE run_id=$4`,
          [hasErrors ? 'partial' : 'completed', totalBars, totalReversals, runId]);
      })
      .catch(async (err) => {
        await pool.query(`UPDATE job_runs SET status=$1, error_message=$2, completed_at=NOW() WHERE run_id=$3`,
          ['failed', err.message, runId]);
      });

    res.json({ success: true, runId, message: 'Daily collection started', symbols: config.symbols, timeframes: config.timeframes, dateRange: { start: yStart, end: tEnd } });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/housekeeping', async (req, res) => {
  try {
    const runId = uuidv4();
    await pool.query(`INSERT INTO job_runs (run_id, job_type, status) VALUES ($1,$2,$3)`, [runId, 'housekeeping', 'running']);
    dataService.archiveAndCleanup()
      .then(async (result) => {
        await pool.query(`UPDATE job_runs SET status=$1, reversals_archived=$2, reversals_deleted=$3, completed_at=NOW() WHERE run_id=$4`,
          ['completed', result.archivedCount, result.deletedCount, runId]);
      })
      .catch(async (err) => {
        await pool.query(`UPDATE job_runs SET status=$1, error_message=$2, completed_at=NOW() WHERE run_id=$3`,
          ['failed', err.message, runId]);
      });
    res.json({ success: true, runId, message: 'Housekeeping started', retentionDays: config.retentionDays });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/status/:runId', async (req, res) => {
  try {
    const id = String(req.params.runId || '');
    if (!id || id.length < 10) return res.status(400).json({ error: 'Invalid runId' });
    const r = await pool.query(`SELECT * FROM job_runs WHERE run_id=$1`, [id]);
    if (!r.rows.length) return res.status(404).json({ error: 'Job not found' });
    res.json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/jobs', async (req, res) => {
  try {
    const limit = clampInt(req.query.limit ?? 20, 1, 200, 20);
    const jobType = req.query.jobType;
    const status = req.query.status;
    let where = '', params = [];
    if (jobType) { where += 'WHERE job_type = $1'; params.push(jobType); }
    if (status) { where += where ? ' AND ' : 'WHERE '; where += `status = $${params.length + 1}`; params.push(status); }
    const r = await pool.query(`SELECT * FROM job_runs ${where} ORDER BY started_at DESC LIMIT $${params.length + 1}`, [...params, limit]);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/stats', async (req, res) => {
  try {
    const days = clampInt(req.query.days ?? 7, 1, 365, 7);
    const symbol = req.query.symbol;
    const timeframe = req.query.timeframe;
    if (symbol && !assertSymbol(symbol)) return res.status(400).json({ error: 'Invalid symbol' });
    if (timeframe && !assertTimeframe(timeframe)) return res.status(400).json({ error: 'Invalid timeframe' });

    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    let where = 'WHERE "timestamp" > $1';
    const params = [startDate];
    if (symbol) { where += ` AND symbol = $${params.length + 1}`; params.push(symbol); }
    if (timeframe) { where += ` AND timeframe = $${params.length + 1}`; params.push(timeframe); }

    const stats = await pool.query(`
      SELECT symbol, timeframe,
             COUNT(*) AS total_reversals,
             COUNT(CASE WHEN outcome='gold' THEN 1 END) AS gold_count,
             COUNT(CASE WHEN outcome='hard_negative' THEN 1 END) AS hard_negative_count,
             COUNT(CASE WHEN outcome='neutral' THEN 1 END) AS neutral_count,
             COUNT(CASE WHEN outcome='pending' THEN 1 END) AS pending_count,
             AVG(CASE WHEN outcome<>'pending' THEN quality_score END) AS avg_quality_score,
             AVG(CASE WHEN outcome='gold' THEN quality_score END) AS avg_gold_score,
             AVG(total_points) AS avg_points,
             MAX("timestamp") AS latest_reversal
        FROM reversals ${where}
       GROUP BY symbol, timeframe
       ORDER BY symbol, timeframe
    `, params);

    let archiveWhere = '', archiveParams = [];
    if (symbol) { archiveWhere = 'WHERE symbol = $1'; archiveParams.push(symbol); }
    if (timeframe) { archiveWhere += archiveWhere ? ' AND ' : 'WHERE '; archiveWhere += `timeframe = $${archiveParams.length + 1}`; archiveParams.push(timeframe); }

    const archive = await pool.query(`
      SELECT symbol, timeframe,
             COUNT(*) AS archived_total,
             COUNT(CASE WHEN outcome='gold' THEN 1 END) AS archived_gold,
             COUNT(CASE WHEN outcome='hard_negative' THEN 1 END) AS archived_hard_negative,
             AVG(total_points) AS archived_avg_points
        FROM reversals_archive ${archiveWhere}
       GROUP BY symbol, timeframe
    `, archiveParams);

    res.json({ current: stats.rows, archive: archive.rows, config: { symbols: config.symbols, timeframes: config.timeframes, retentionDays: config.retentionDays } });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/reversals', async (req, res) => {
  try {
    const symbol = req.query.symbol;
    const timeframe = req.query.timeframe;
    const outcome = req.query.outcome;
    const trend = req.query.trend;
    const limit = clampInt(req.query.limit ?? 50, 1, 500, 50);
    const minQuality = req.query.minQuality ? clampInt(req.query.minQuality, 0, 100, 0) : null;
    const minPoints = req.query.minPoints ? clampInt(req.query.minPoints, 0, 100, 0) : null;
    const tier = req.query.tier;
    const sort = (req.query.sort || 'time_desc') === 'points_desc' ? 'points_desc' : 'time_desc';
    const source = ['live', 'archive', 'both'].includes(req.query.source) ? req.query.source : 'live';

    if (symbol && !assertSymbol(symbol)) return res.status(400).json({ error: 'Invalid symbol' });
    if (timeframe && !assertTimeframe(timeframe)) return res.status(400).json({ error: 'Invalid timeframe' });
    if (tier && !['S', 'A', 'B', 'C'].includes(tier)) return res.status(400).json({ error: 'Invalid tier' });

    let where = 'WHERE 1=1', params = [];
    if (symbol) { where += ` AND symbol = $${params.length + 1}`; params.push(symbol); }
    if (timeframe) { where += ` AND timeframe = $${params.length + 1}`; params.push(timeframe); }
    if (outcome) { where += ` AND outcome = $${params.length + 1}`; params.push(outcome); }
    if (trend) { where += ` AND swing_trend_incoming = $${params.length + 1}`; params.push(trend); }
    if (minQuality != null) { where += ` AND quality_score >= $${params.length + 1}`; params.push(minQuality); }
    if (minPoints != null) { where += ` AND total_points >= $${params.length + 1}`; params.push(minPoints); }
    if (tier) { where += ` AND point_tier = $${params.length + 1}`; params.push(tier); }
    const order = sort === 'points_desc' ? 'ORDER BY total_points DESC, "timestamp" DESC' : 'ORDER BY "timestamp" DESC';

    let query;
    if (source === 'archive') {
      query = `SELECT *, 'archive' AS source_table FROM reversals_archive ${where} ${order} LIMIT $${params.length + 1}`;
    } else if (source === 'both') {
      query = `
        (SELECT *, 'live' AS source_table FROM reversals ${where})
        UNION ALL
        (SELECT *, 'archive' AS source_table FROM reversals_archive ${where})
        ${order} LIMIT $${params.length + 1}
      `;
    } else {
      query = `SELECT *, 'live' AS source_table FROM reversals ${where} ${order} LIMIT $${params.length + 1}`;
    }
    const r = await pool.query(query, [...params, limit]);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

if (process.env.NODE_ENV === 'production') {
  const headers = { 'Content-Type': 'application/json', 'x-api-key': process.env.ADMIN_API_KEY };
  cron.schedule('30 18 * * 1-5', async () => { try { await fetch(`http://localhost:${port}/api/admin/daily_collect`, { method: 'POST', headers }); } catch {} }, { timezone: config.timezone });
  cron.schedule('0 */4 * * *',     async () => { try { await fetch(`http://localhost:${port}/api/label`, { method: 'POST', headers, body: JSON.stringify({ daysBack: 1 }) }); } catch {} });
  cron.schedule('0 2 * * *',       async () => { try { await fetch(`http://localhost:${port}/api/admin/housekeeping`, { method: 'POST', headers }); } catch {} }, { timezone: config.timezone });
}

app.listen(port, () => { console.log(`Reversal API (production) listening on :${port}`); });
