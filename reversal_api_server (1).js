// Enhanced Reversal Data Collection API (Aligned to schema v2.0.1)
// Multi-timeframe (1m, 5m) with automated archive system
// Production-ready Node.js Express server for Render / Node 18+

/* Key fixes vs your draft:
   - Align INSERT columns with DB schema (no undefined trend_points/bonus_points/etc.).
   - Add simple scoring to populate base_points,total_points,point_tier.
   - Fix SQL placeholders ($1, $2, ...) in /api/jobs, /api/stats, /api/reversals, /api/training.
   - Guard optional DB objects (training_fingerprints, get_neighbor_patterns, refresh_training_view).
   - Improve duration calculation for job_runs.
   - Safe LIMIT handling with parameter binding.
*/

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const yahooFinance = require('yahoo-finance2').default;
const cron = require('node-cron');
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

// Node 18+ has global fetch; add fallback just in case.
let fetchFn = global.fetch;
if (!fetchFn) {
  fetchFn = (...args) => import('node-fetch').then(({default: f}) => f(...args));
}

const app = express();
}

// ---------- Reversal Detector ----------
class ReversalDetector {
  constructor(timeframe = '1m') {
    this.timeframe = timeframe;
    this.cfg = this.getTimeframeConfig(timeframe);
  }

  getTimeframeConfig(tf) {
    const base = {
      rsiOB: 70, rsiOS: 30,
      atrStretchMin: 1.0,
      relVolumeMin: 2.0,
      followThroughAtrMin: 1.0,
      followThroughBarsMin: 5,
      confirmBarsMax: 5
    };
    if (tf === '5m') {
      return { ...base, atrStretchMin: 0.8, relVolumeMin: 1.5, followThroughBarsMin: 3, confirmBarsMax: 3 };
    }
    return base;
  }

  detectSwingPoints(high, low, lookback = 3) {
    const higherHighs = [], lowerLows = [], higherLows = [], lowerHighs = [];
    for (let i = lookback; i < high.length - lookback; i++) {
      if (high.slice(i - lookback, i).every(h => h < high[i]) &&
          high.slice(i + 1, i + 1 + lookback).every(h => h < high[i])) higherHighs.push({ index: i, price: high[i] });
      if (low.slice(i - lookback, i).every(l => l > low[i]) &&
          low.slice(i + 1, i + 1 + lookback).every(l => l > low[i])) lowerLows.push({ index: i, price: low[i] });
      if (low.slice(i - lookback, i).some(l => l < low[i]) &&
          low.slice(i + 1, i + 1 + lookback).every(l => l > low[i])) higherLows.push({ index: i, price: low[i] });
      if (high.slice(i - lookback, i).some(h => h > high[i]) &&
          high.slice(i + 1, i + 1 + lookback).every(h => h < high[i])) lowerHighs.push({ index: i, price: high[i] });
    }
    return { higherHighs, lowerLows, higherLows, lowerHighs };
  }

  detectReversals(ohlcv, timestamps) {
    const { open, high, low, close, volume } = ohlcv;
    const rsi = TechnicalIndicators.rsi(close);
    const atr = TechnicalIndicators.atr(high, low, close);
    const ema8 = TechnicalIndicators.ema(close, 8);
    const ema21 = TechnicalIndicators.ema(close, 21);
    const vwap = TechnicalIndicators.vwap(high, low, close, volume);
    const relVol = volume.map((v, i) => {
      if (i < 20) return 1.0;
      const avg = volume.slice(i - 19, i + 1).reduce((a, b) => a + b, 0) / 20;
      return avg ? v / avg : 1.0;
    });
    const swings = this.detectSwingPoints(high, low);

    const out = [];
    for (let i = 30; i < close.length - 10; i++) {
      const rev = this.checkReversalConditions(i, { open, high, low, close, volume }, { rsi, atr, ema8, ema21, vwap, relVol }, swings, timestamps[i]);
      if (rev) out.push(rev);
    }
    return out;
  }

  checkReversalConditions(i, ohlcv, ind, swings, ts) {
    const { close, high, low } = ohlcv;
    const { rsi, atr, ema8, ema21, vwap, relVol } = ind;

    const trend = this.checkTrendContext(i, swings);
    if (!trend.hasTrend) return null;
    const exhaust = this.checkExhaustion(i, { close, rsi, atr, ema21, vwap, relVol });
    if (!exhaust.hasExhaustion) return null;
    const structure = this.checkStructureBreak(i, trend, swings, { high, low });
    if (!structure.hasBreak) return null;
    const confirm = this.checkConfirmation(i, { rsi, ema8, ema21, vwap, close });
    if (!confirm.hasConfirmation) return null;
    const follow = this.checkFollowThrough(i, { high, low, close }, atr, trend.direction);
    if (!follow.hasFollowThrough) return null;

    // Lightweight scoring to fill base_points/total_points/point_tier
    let basePoints = 0;
    if (exhaust.atrStretch >= this.cfg.atrStretchMin) basePoints += 2;
    if (relVol[i] >= this.cfg.relVolumeMin) basePoints += 1;
    if ((trend.direction === 'uptrend' && confirm.emaCross === 'up') ||
        (trend.direction === 'downtrend' && confirm.emaCross === 'down')) basePoints += 2;
    if ((trend.direction === 'uptrend' && confirm.rsiCross === 'up') ||
        (trend.direction === 'downtrend' && confirm.rsiCross === 'down')) basePoints += 1;
    if (follow.atr >= this.cfg.followThroughAtrMin) basePoints += 2;

    const totalPoints = basePoints;
    const pointTier = totalPoints >= 6 ? 'S' : totalPoints >= 5 ? 'A' : totalPoints >= 3 ? 'B' : 'C';

    return {
      timestamp: ts,
      timeframe: this.timeframe,
      swing_trend_incoming: trend.direction,
      broke_last_higher_low: structure.brokeHigherLow,
      broke_last_lower_high: structure.brokeLowerHigh,
      bars_since_peak_trough: trend.barsSincePeakTrough,
      rsi: rsi[i],
      rsi_lookback_max: Math.max(...rsi.slice(Math.max(0, i - 10), i + 1).filter(x => x != null)),
      rsi_lookback_min: Math.min(...rsi.slice(Math.max(0, i - 10), i + 1).filter(x => x != null)),
      move_vs_atr: exhaust.atrStretch ?? 0,
      price_to_vwap_pct: ((close[i] - vwap[i]) / vwap[i]) * 100,
      price_to_ema21_pct: ((close[i] - ema21[i]) / ema21[i]) * 100,
      rel_vol_20: ind.relVol[i],
      vol_climax_flag: ind.relVol[i] >= this.cfg.relVolumeMin,
      ema8_cross_ema21: confirm.emaCross,
      rsi_cross_50: confirm.rsiCross,
      vwap_retest_result: confirm.vwapRetest,
      follow_through_atr: follow.atr,
      follow_through_bars_10: follow.bars,
      base_points: basePoints,
      total_points: totalPoints,
      point_tier: pointTier
    };
  }

  checkTrendContext(index, swings) {
    const look = this.timeframe === '1m' ? 50 : 25;
    const highs = swings.higherHighs.filter(h => h.index < index && h.index > index - look);
    const lows = swings.lowerLows.filter(l => l.index < index && l.index > index - look);
    if (highs.length >= 3) return { hasTrend: true, direction: 'downtrend', barsSincePeakTrough: index - highs.at(-1).index };
    if (lows.length >= 3)  return { hasTrend: true, direction: 'uptrend',   barsSincePeakTrough: index - lows.at(-1).index };
    return { hasTrend: false };
  }

  checkExhaustion(i, { close, rsi, atr, ema21, vwap, relVol }) {
    let hasExhaustion = false;
    let atrStretch = 0;
    if (rsi[i] != null && (rsi[i] >= this.cfg.rsiOB || rsi[i] <= this.cfg.rsiOS)) hasExhaustion = true;
    if (atr[i]) {
      atrStretch = Math.abs(close[i] - ema21[i]) / atr[i];
      if (atrStretch >= this.cfg.atrStretchMin) hasExhaustion = true;
    }
    if (relVol[i] >= this.cfg.relVolumeMin) hasExhaustion = true;
    return { hasExhaustion, atrStretch };
  }

  checkStructureBreak(i, trend, swings, { high, low }) {
    let hasBreak = false, brokeHigherLow = false, brokeLowerHigh = false;
    if (trend.direction === 'uptrend') {
      const lowerHighs = swings.lowerHighs.filter(lh => lh.index < i);
      if (lowerHighs.length) {
        const lastLH = lowerHighs.at(-1);
        if (high[i] > lastLH.price) { hasBreak = true; brokeLowerHigh = true; }
      }
    } else {
      const higherLows = swings.higherLows.filter(hl => hl.index < i);
      if (higherLows.length) {
        const lastHL = higherLows.at(-1);
        if (low[i] < lastHL.price) { hasBreak = true; brokeHigherLow = true; }
      }
    }
    return { hasBreak, brokeHigherLow, brokeLowerHigh };
  }

  checkConfirmation(i, { rsi, ema8, ema21, vwap, close }) {
    let has = false, emaCross = 'none', rsiCross = 'none', vwapRetest = 'none';
    for (let k = i; k <= Math.min(i + this.cfg.confirmBarsMax, close.length - 1); k++) {
      if (k > 0) {
        if (ema8[k - 1] <= ema21[k - 1] && ema8[k] > ema21[k]) { emaCross = 'up'; has = true; }
        else if (ema8[k - 1] >= ema21[k - 1] && ema8[k] < ema21[k]) { emaCross = 'down'; has = true; }
        if (rsi[k - 1] != null && rsi[k] != null) {
          if (rsi[k - 1] <= 50 && rsi[k] > 50) { rsiCross = 'up'; has = true; }
          else if (rsi[k - 1] >= 50 && rsi[k] < 50) { rsiCross = 'down'; has = true; }
        }
      }
      const vwDist = Math.abs(close[k] - vwap[k]) / vwap[k];
      if (vwDist < 0.002) { vwapRetest = 'accept'; has = true; }
    }
    return { hasConfirmation: has, emaCross, rsiCross, vwapRetest };
  }

  checkFollowThrough(i, { high, low, close }, atr, dir) {
    let maxMove = 0, barsDir = 0;
    const start = close[i];
    const checkBars = this.timeframe === '1m' ? 10 : 6;
    for (let k = i + 1; k <= Math.min(i + checkBars, close.length - 1); k++) {
      const move = dir === 'uptrend' ? (close[k] - start) : (start - close[k]);
      if (move > maxMove) maxMove = move;
      if (move > 0) barsDir++;
    }
    const atrVal = atr[i] ? maxMove / atr[i] : 0;
    const has = atrVal >= this.cfg.followThroughAtrMin || barsDir >= this.cfg.followThroughBarsMin;
    return { hasFollowThrough: has, atr: atrVal, bars: barsDir };
  }
}

// ---------- Data Collection Service ----------
class DataCollectionService {
  constructor() {
    this.detectors = {};
    config.timeframes.forEach(tf => this.detectors[tf] = new ReversalDetector(tf));
  }

  async fetchYahooData(symbol, startDate, endDate, interval = '1m') {
    // rate-limit
    await new Promise(r => setTimeout(r, config.yahooRateLimit));
    const queryOptions = { period1: startDate, period2: endDate, interval, includeAdjustedClose: true };
    const rows = await yahooFinance.historical(symbol, queryOptions);
    if (!rows || !rows.length) return { timestamps: [], open: [], high: [], low: [], close: [], volume: [] };
    return {
      timestamps: rows.map(r => new Date(r.date)),
      open: rows.map(r => r.open),
      high: rows.map(r => r.high),
      low: rows.map(r => r.low),
      close: rows.map(r => r.close),
      volume: rows.map(r => r.volume || 0)
    };
  }

  async processTimeframeData(symbol, timeframe, startDate, endDate, runId) {
    const client = await pool.connect();
    const jobStart = Date.now();
    try {
      const slices = this.createTimeSlices(startDate, endDate, config.sliceHours);
      let bars = 0, detected = 0, inserted = 0;

      for (const slice of slices) {
        const ohlcv = await this.fetchYahooData(symbol, slice.start, slice.end, timeframe);
        if (!ohlcv.timestamps.length) continue;
        const reversals = this.detectors[timeframe].detectReversals(ohlcv, ohlcv.timestamps);
        bars += ohlcv.timestamps.length;
        detected += reversals.length;

        for (const r of reversals) {
          try {
            await client.query(`
              INSERT INTO reversals (
                symbol, timeframe, "timestamp",
                swing_trend_incoming, broke_last_higher_low, broke_last_lower_high, bars_since_peak_trough,
                rsi, rsi_lookback_max, rsi_lookback_min, move_vs_atr,
                price_to_vwap_pct, price_to_ema21_pct, rel_vol_20, vol_climax_flag,
                ema8_cross_ema21, rsi_cross_50, vwap_retest_result,
                follow_through_atr, follow_through_bars_10,
                base_points, total_points, point_tier, run_id, source
              ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25
              )
              ON CONFLICT (symbol, timeframe, "timestamp") DO NOTHING
            `, [
              symbol, timeframe, r.timestamp,
              r.swing_trend_incoming, r.broke_last_higher_low, r.broke_last_lower_high, r.bars_since_peak_trough,
              r.rsi, r.rsi_lookback_max, r.rsi_lookback_min, r.move_vs_atr,
              r.price_to_vwap_pct, r.price_to_ema21_pct, r.rel_vol_20, r.vol_climax_flag,
              r.ema8_cross_ema21, r.rsi_cross_50, r.vwap_retest_result,
              r.follow_through_atr, r.follow_through_bars_10,
              r.base_points, r.total_points, r.point_tier, runId, 'daily_refresh'
            ]);
            inserted++;
          } catch (e) {
            console.error(`Insert error @ ${r.timestamp}:`, e.message);
          }
        }
      }

      await client.query(`
        INSERT INTO processing_watermarks (symbol, timeframe, last_processed_ts, last_successful_run)
        VALUES ($1,$2,$3,NOW())
        ON CONFLICT (symbol, timeframe)
        DO UPDATE SET last_processed_ts = EXCLUDED.last_processed_ts,
                      last_successful_run = NOW(),
                      consecutive_failures = 0,
                      updated_at = NOW()
      `, [symbol, timeframe, endDate]);

      const duration = Math.floor((Date.now() - jobStart) / 1000);
      return { symbol, timeframe, barsProcessed: bars, reversalsDetected: detected, reversalsInserted: inserted, duration };
    } catch (error) {
      await client.query(`
        INSERT INTO processing_watermarks (symbol, timeframe, last_processed_ts, consecutive_failures)
        VALUES ($1,$2,$3,1)
        ON CONFLICT (symbol, timeframe)
        DO UPDATE SET consecutive_failures = processing_watermarks.consecutive_failures + 1,
                      updated_at = NOW()
      `, [symbol, timeframe, endDate]);
      throw error;
    } finally {
      client.release();
    }
  }

  createTimeSlices(start, end, sliceHours) {
    const out = [];
    const step = sliceHours * 60 * 60 * 1000;
    let cur = new Date(start);
    while (cur < end) {
      const nxt = new Date(Math.min(cur.getTime() + step, end.getTime()));
      out.push({ start: new Date(cur), end: new Date(nxt) });
      cur = new Date(nxt);
    }
    return out;
  }

  async labelReversals(daysBack = 1) {
    const client = await pool.connect();
    try {
      const cutoff1m = new Date(Date.now() - (20 * 60 * 1000) - (daysBack * 86400000));
      const cutoff5m = new Date(Date.now() - (15 * 60 * 1000) - (daysBack * 86400000));

      const { rows } = await client.query(`
        SELECT * FROM reversals 
        WHERE outcome = 'pending' 
          AND ((timeframe = '1m' AND "timestamp" < $1) OR (timeframe = '5m' AND "timestamp" < $2))
        ORDER BY "timestamp" DESC
        LIMIT 200
      `, [cutoff1m, cutoff5m]);

      let labeled = 0;
      for (const rev of rows) {
        try {
          const fuMins = rev.timeframe === '1m' ? 30 : 25;
          const endTime = new Date(new Date(rev.timestamp).getTime() + fuMins * 60000);
          const ohlcv = await this.fetchYahooData(rev.symbol, new Date(rev.timestamp), endTime, rev.timeframe);
          if (ohlcv.close.length < 10) continue;
          const label = this.calculateOutcomeLabel(ohlcv, rev.swing_trend_incoming, rev.timeframe);
          await client.query(`
            UPDATE reversals SET
              outcome = $1, quality_score = $2, mfe_atr = $3, mae_atr = $4,
              rr_achieved = $5, time_to_target_bars = $6, time_to_stop_bars = $7,
              structure_invalidated = $8, updated_at = NOW()
            WHERE id = $9
          `, [label.outcome, label.qualityScore, label.mfeAtr, label.maeAtr, label.rrAchieved,
              label.timeToTargetBars, label.timeToStopBars, label.structureInvalidated, rev.id]);
          labeled++;
        } catch (e) {
          console.error('Label error', e.message);
        }
      }
      return labeled;
    } finally {
      client.release();
    }
  }

  calculateOutcomeLabel(ohlcv, trendDirection, timeframe) {
    const { close, high, low } = ohlcv;
    const entry = close[0];
    const atr = this.estimateATR(ohlcv);
    let mfe = 0, mae = 0, ttt = null, tts = null, structureInvalidated = false;
    const targetATR = timeframe === '1m' ? 1.5 : 1.2;
    const stopATR = timeframe === '1m' ? 0.7 : 0.6;
    const maxBars = timeframe === '1m' ? 20 : 15;
    const target = trendDirection === 'uptrend' ? entry + targetATR * atr : entry - targetATR * atr;
    const stop = trendDirection === 'uptrend' ? entry - stopATR * atr : entry + stopATR * atr;

    for (let i = 1; i <= Math.min(maxBars, close.length - 1); i++) {
      const p = close[i];
      if (trendDirection === 'uptrend') {
        const fav = (p - entry) / atr, adv = (entry - p) / atr;
        mfe = Math.max(mfe, fav); mae = Math.max(mae, adv);
        if (p >= target && ttt == null) ttt = i;
        if (p <= stop && tts == null) tts = i;
      } else {
        const fav = (entry - p) / atr, adv = (p - entry) / atr;
        mfe = Math.max(mfe, fav); mae = Math.max(mae, adv);
        if (p <= target && ttt == null) ttt = i;
        if (p >= stop && tts == null) tts = i;
      }
    }

    let outcome = 'neutral', qualityScore = 50, rrAchieved = mfe - mae;
    const goldTime = timeframe === '1m' ? 15 : 12;
    const goldMae = timeframe === '1m' ? 0.5 : 0.4;
    if (ttt && ttt <= goldTime && mae <= goldMae) {
      outcome = 'gold';
      qualityScore = Math.min(100, 70 + (30 * (targetATR - mae) / targetATR) + (10 * (goldTime - ttt) / goldTime));
      rrAchieved = mfe;
    } else if ((tts && tts <= 10) || (mfe < 0.5 && mae > 0.7)) {
      outcome = 'hard_negative';
      qualityScore = Math.max(0, 30 - (mae * 10));
      rrAchieved = -mae;
    } else {
      qualityScore = Math.max(20, 50 - (mae * 5)) + Math.min(30, mfe * 10);
    }
    return {
      outcome,
      qualityScore: Math.round(qualityScore),
      mfeAtr: Math.round(mfe * 100) / 100,
      maeAtr: Math.round(mae * 100) / 100,
      rrAchieved: Math.round(rrAchieved * 100) / 100,
      timeToTargetBars: ttt,
      timeToStopBars: tts,
      structureInvalidated
    };
  }

  estimateATR(ohlcv, period = 14) {
    const { high, low, close } = ohlcv;
    if (high.length < 2) return Math.max(0.25, (high[0] - low[0]) || 1); // fallback
    const trs = [];
    for (let i = 1; i < Math.min(high.length, period + 1); i++) {
      const tr1 = high[i] - low[i];
      const tr2 = Math.abs(high[i] - close[i - 1]);
      const tr3 = Math.abs(low[i] - close[i - 1]);
      trs.push(Math.max(tr1, tr2, tr3));
    }
    return trs.reduce((a, b) => a + b, 0) / trs.length;
  }

  async archiveAndCleanup() {
    const client = await pool.connect();
    try {
      const { rows: aRows } = await client.query(`SELECT archive_labeled_reversals($1::INTERVAL) AS archived`, [`${config.retentionDays} days`]);
      const { rows: dRows } = await client.query(`SELECT cleanup_old_reversals($1::INTERVAL) AS deleted`, [`${config.retentionDays} days`]);

      // Best-effort optional maintenance (these objects may not exist)
      try { await client.query('SELECT refresh_training_view()'); } catch {}
      try { await client.query('SELECT update_system_stats()'); } catch {}
      try { await client.query('ANALYZE reversals'); } catch {}
      try { await client.query('ANALYZE reversals_archive'); } catch {}

      return { archivedCount: aRows[0]?.archived ?? 0, deletedCount: dRows[0]?.deleted ?? 0 };
    } finally {
      client.release();
    }
  }
}

const dataService = new DataCollectionService();

// ---------- API Routes ----------

// Process a specific date range
app.post('/api/process', async (req, res) => {
  try {
    const { symbol, timeframe, startDate, endDate } = req.body;
    if (!symbol || !timeframe || !startDate || !endDate) {
      return res.status(400).json({ error: 'symbol, timeframe, startDate, and endDate are required' });
    }
    if (!config.symbols.includes(symbol) || !config.timeframes.includes(timeframe)) {
      return res.status(400).json({ error: `Unsupported symbol or timeframe. Supported symbols=${config.symbols.join(',')} timeframes=${config.timeframes.join(',')}` });
    }

    const runId = uuidv4();
    const start = new Date(startDate);
    const end = new Date(endDate);

    await pool.query(`
      INSERT INTO job_runs (run_id, job_type, symbol, timeframe, start_date, end_date, status)
      VALUES ($1,$2,$3,$4,$5,$6,$7)
    `, [runId, 'backfill', symbol, timeframe, start, end, 'running']);

    (async () => {
      try {
        const result = await dataService.processTimeframeData(symbol, timeframe, start, end, runId);
        await pool.query(`
          UPDATE job_runs SET status=$1, bars_processed=$2, reversals_detected=$3, duration_seconds=$4, completed_at=NOW()
          WHERE run_id=$5
        `, ['completed', result.barsProcessed, result.reversalsDetected, result.duration, runId]);
      } catch (err) {
        await pool.query(`UPDATE job_runs SET status=$1, error_message=$2, completed_at=NOW() WHERE run_id=$3`, ['failed', err.message, runId]);
      }
    })();

    res.json({ success: true, runId, message: 'Processing started', symbol, timeframe, startDate, endDate });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Label pending reversals
app.post('/api/label', async (req, res) => {
  try {
    const { daysBack = 1 } = req.body || {};
    const runId = uuidv4();
    await pool.query(`INSERT INTO job_runs (run_id, job_type, status) VALUES ($1,$2,$3)`, [runId, 'labeling', 'running']);

    (async () => {
      try {
        const count = await dataService.labelReversals(daysBack);
        await pool.query(`UPDATE job_runs SET status=$1, reversals_labeled=$2, completed_at=NOW() WHERE run_id=$3`, ['completed', count, runId]);
      } catch (err) {
        await pool.query(`UPDATE job_runs SET status=$1, error_message=$2, completed_at=NOW() WHERE run_id=$3`, ['failed', err.message, runId]);
      }
    })();

    res.json({ success: true, runId, message: 'Labeling started', daysBack });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Admin daily collect (previous trading day 9:30-16:00 ET)
app.post('/api/admin/daily_collect', async (req, res) => {
  try {
    const runId = uuidv4();
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    yesterday.setHours(9, 30, 0, 0);
    const today = new Date();
    today.setHours(16, 0, 0, 0);

    await pool.query(`INSERT INTO job_runs (run_id, job_type, start_date, end_date, status) VALUES ($1,$2,$3,$4,$5)`, [runId, 'daily_refresh', yesterday, today, 'running']);

    const tasks = [];
    let totalBars = 0, totalReversals = 0;

    for (const sym of config.symbols) {
      for (const tf of config.timeframes) {
        tasks.push(
          dataService.processTimeframeData(sym, tf, yesterday, today, runId)
            .then(r => { totalBars += r.barsProcessed; totalReversals += r.reversalsDetected; return r; })
            .catch(err => ({ symbol: sym, timeframe: tf, error: err.message }))
        );
      }
    }

    Promise.all(tasks).then(async results => {
      const hasErr = results.some(r => r.error);
      await pool.query(`UPDATE job_runs SET status=$1, bars_processed=$2, reversals_detected=$3, completed_at=NOW() WHERE run_id=$4`,
        [hasErr ? 'partial' : 'completed', totalBars, totalReversals, runId]);
    }).catch(async err => {
      await pool.query(`UPDATE job_runs SET status=$1, error_message=$2, completed_at=NOW() WHERE run_id=$3`, ['failed', err.message, runId]);
    });

    res.json({ success: true, runId, message: 'Daily collection started', symbols: config.symbols, timeframes: config.timeframes, dateRange: { start: yesterday, end: today } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Admin housekeeping
app.post('/api/admin/housekeeping', async (req, res) => {
  try {
    const runId = uuidv4();
    await pool.query(`INSERT INTO job_runs (run_id, job_type, status) VALUES ($1,$2,$3)`, [runId, 'housekeeping', 'running']);

    (async () => {
      try {
        const out = await dataService.archiveAndCleanup();
        await pool.query(`UPDATE job_runs SET status=$1, reversals_archived=$2, reversals_deleted=$3, completed_at=NOW() WHERE run_id=$4`,
          ['completed', out.archivedCount, out.deletedCount, runId]);
      } catch (err) {
        await pool.query(`UPDATE job_runs SET status=$1, error_message=$2, completed_at=NOW() WHERE run_id=$3`, ['failed', err.message, runId]);
      }
    })();

    res.json({ success: true, runId, message: 'Housekeeping started', retentionDays: config.retentionDays });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Job status
app.get('/api/status/:runId', async (req, res) => {
  try {
    const { rows } = await pool.query(`SELECT * FROM job_runs WHERE run_id=$1`, [req.params.runId]);
    if (!rows.length) return res.status(404).json({ error: 'Job not found' });
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Recent jobs (with proper $ placeholders)
app.get('/api/jobs', async (req, res) => {
  try {
    const { limit = 20, jobType, status } = req.query;
    const params = [];
    const clauses = [];
    if (jobType) { params.push(jobType); clauses.push(`job_type = $${params.length}`); }
    if (status)  { params.push(status);  clauses.push(`status = $${params.length}`); }
    params.push(parseInt(limit, 10));
    const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
    const { rows } = await pool.query(`SELECT * FROM job_runs ${where} ORDER BY started_at DESC LIMIT $${params.length}`, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Stats (aligned placeholders)
app.get('/api/stats', async (req, res) => {
  try {
    const { symbol, timeframe, days = 7 } = req.query;
    const startDate = new Date(Date.now() - parseInt(days, 10) * 86400000);

    // Current stats
    const params1 = [startDate];
    const clauses1 = [`"timestamp" > $1`];
    if (symbol) { params1.push(symbol); clauses1.push(`symbol = $${params1.length}`); }
    if (timeframe) { params1.push(timeframe); clauses1.push(`timeframe = $${params1.length}`); }
    const { rows: current } = await pool.query(`
      SELECT symbol, timeframe,
             COUNT(*) AS total_reversals,
             COUNT(CASE WHEN outcome='gold' THEN 1 END) AS gold_count,
             COUNT(CASE WHEN outcome='hard_negative' THEN 1 END) AS hard_negative_count,
             COUNT(CASE WHEN outcome='neutral' THEN 1 END) AS neutral_count,
             COUNT(CASE WHEN outcome='pending' THEN 1 END) AS pending_count,
             AVG(CASE WHEN outcome <> 'pending' THEN quality_score END) AS avg_quality_score,
             AVG(CASE WHEN outcome = 'gold' THEN quality_score END) AS avg_gold_score,
             MAX("timestamp") AS latest_reversal
      FROM reversals
      WHERE ${clauses1.join(' AND ')}
      GROUP BY symbol, timeframe
      ORDER BY symbol, timeframe
    `, params1);

    // Archive stats (optional filters)
    const params2 = [];
    const clauses2 = [];
    if (symbol) { params2.push(symbol); clauses2.push(`symbol = $${params2.length}`); }
    if (timeframe) { params2.push(timeframe); clauses2.push(`timeframe = $${params2.length}`); }
    const where2 = clauses2.length ? `WHERE ${clauses2.join(' AND ')}` : '';
    const { rows: archive } = await pool.query(`
      SELECT symbol, timeframe,
             COUNT(*) AS archived_total,
             COUNT(CASE WHEN outcome='gold' THEN 1 END) AS archived_gold,
             COUNT(CASE WHEN outcome='hard_negative' THEN 1 END) AS archived_hard_negative
      FROM reversals_archive
      ${where2}
      GROUP BY symbol, timeframe
    `, params2);

    // Daily breakdown
    const params3 = [startDate];
    const clauses3 = [`"timestamp" > $1`];
    if (symbol) { params3.push(symbol); clauses3.push(`symbol = $${params3.length}`); }
    if (timeframe) { params3.push(timeframe); clauses3.push(`timeframe = $${params3.length}`); }
    const { rows: daily } = await pool.query(`
      SELECT DATE("timestamp") AS date, symbol, timeframe,
             COUNT(*) AS daily_reversals,
             COUNT(CASE WHEN outcome='gold' THEN 1 END) AS daily_gold,
             COUNT(CASE WHEN outcome='hard_negative' THEN 1 END) AS daily_hard_negative
      FROM reversals
      WHERE ${clauses3.join(' AND ')}
      GROUP BY DATE("timestamp"), symbol, timeframe
      ORDER BY date DESC, symbol, timeframe
    `, params3);

    res.json({ current, archive, daily, config: { symbols: config.symbols, timeframes: config.timeframes, retentionDays: config.retentionDays } });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Reversals listing
app.get('/api/reversals', async (req, res) => {
  try {
    const { symbol, timeframe, outcome, trend, limit = 50, minQuality, source = 'live' } = req.query;
    const params = [];
    const clauses = ['1=1'];
    let from = 'reversals';
    if (source === 'archive') from = 'reversals_archive';
    if (source === 'both')   from = 'reversals'; // keep simple; union can be added later

    if (symbol)    { params.push(symbol);    clauses.push(`symbol = $${params.length}`); }
    if (timeframe) { params.push(timeframe); clauses.push(`timeframe = $${params.length}`); }
    if (outcome)   { params.push(outcome);   clauses.push(`outcome = $${params.length}`); }
    if (trend)     { params.push(trend);     clauses.push(`swing_trend_incoming = $${params.length}`); }
    if (minQuality){ params.push(parseInt(minQuality,10)); clauses.push(`quality_score >= $${params.length}`); }
    params.push(parseInt(limit,10));

    const { rows } = await pool.query(`
      SELECT * FROM ${from}
      WHERE ${clauses.join(' AND ')}
      ORDER BY "timestamp" DESC
      LIMIT $${params.length}
    `, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Training data endpoint (optional materialized view)
app.get('/api/training', async (req, res) => {
  try {
    const { symbol = 'ES=F', timeframe = '1m', trend, outcome, conviction, limit = 100 } = req.query;
    // Check view exists
    const { rows: x } = await pool.query(`
      SELECT COUNT(*) FROM information_schema.tables WHERE table_name='training_fingerprints'
    `);
    if (!x[0] || x[0].count === '0') {
      return res.status(501).json({ error: 'training_fingerprints view not configured' });
    }
    const params = [symbol, timeframe];
    const clauses = ['symbol = $1', 'timeframe = $2'];
    if (trend)      { params.push(trend);      clauses.push(`swing_trend_incoming = $${params.length}`); }
    if (outcome)    { params.push(outcome);    clauses.push(`outcome = $${params.length}`); }
    if (conviction) { params.push(conviction); clauses.push(`conviction_level = $${params.length}`); }
    params.push(parseInt(limit,10));
    const { rows } = await pool.query(`
      SELECT source_table, symbol, timeframe, "timestamp", swing_trend_incoming,
             rsi, move_vs_atr, price_to_vwap_pct, price_to_ema21_pct,
             rel_vol_20, vol_climax_flag, ema8_cross_ema21, rsi_cross_50,
             follow_through_atr, outcome, quality_score, mfe_atr, mae_atr,
             pattern_weight, recency_score, conviction_level
      FROM training_fingerprints
      WHERE ${clauses.join(' AND ')}
      ORDER BY pattern_weight DESC, quality_score DESC, "timestamp" DESC
      LIMIT $${params.length}
    `, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Neighbor patterns (optional SQL function)
app.get('/api/neighbors', async (req, res) => {
  try {
    const { symbol = 'ES=F', timeframe = '1m', trend, rsi, moveAtr, vwapPct, limit = 20 } = req.query;
    if (!trend || rsi == null || moveAtr == null || vwapPct == null) {
      return res.status(400).json({ error: 'trend, rsi, moveAtr, and vwapPct are required' });
    }
    const { rows: x } = await pool.query(`
      SELECT COUNT(*) FROM pg_proc WHERE proname='get_neighbor_patterns'
    `);
    if (!x[0] || x[0].count === '0') {
      return res.status(501).json({ error: 'get_neighbor_patterns() not configured' });
    }
    const { rows } = await pool.query(`
      SELECT * FROM get_neighbor_patterns($1,$2,$3,$4,$5,$6,$7)
    `, [symbol, timeframe, trend, parseFloat(rsi), parseFloat(moveAtr), parseFloat(vwapPct), parseInt(limit,10)]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- Schedules (production only) ----------
if (process.env.NODE_ENV === 'production') {
  // Daily refresh at 6:30 PM ET (Mon-Fri)
  cron.schedule('30 18 * * 1-5', async () => {
    try {
      const r = await fetchFn(`http://localhost:${port}/api/admin/daily_collect`, { method: 'POST', headers: { 'Content-Type': 'application/json' } });
      await r.json();
    } catch (e) { console.error('daily_collect failed', e.message); }
  }, { timezone: config.timezone });

  // Labeling job every 4 hours
  cron.schedule('0 */4 * * *', async () => {
    try {
      const r = await fetchFn(`http://localhost:${port}/api/label`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ daysBack: 1 }) });
      await r.json();
    } catch (e) { console.error('label job failed', e.message); }
  }, { timezone: config.timezone });

  // Housekeeping at 2:00 AM ET daily
  cron.schedule('0 2 * * *', async () => {
    try {
      const r = await fetchFn(`http://localhost:${port}/api/admin/housekeeping`, { method: 'POST', headers: { 'Content-Type': 'application/json' } });
      await r.json();
    } catch (e) { console.error('housekeeping failed', e.message); }
  }, { timezone: config.timezone });
}

// Start server
app.listen(port, () => {
  console.log(`Enhanced Reversal Data Collection API running on port ${port}`);
  console.log('Configuration:', config);
});
