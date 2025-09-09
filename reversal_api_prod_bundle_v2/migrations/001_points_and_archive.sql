-- migrations/001_points_and_archive.sql
-- Adds points to live/archive tables, fixes indexes, and updates archive function to carry points.
-- Run this after the base schema you shared earlier.

-- 1) Columns
ALTER TABLE reversals
  ADD COLUMN IF NOT EXISTS base_points INTEGER,
  ADD COLUMN IF NOT EXISTS total_points INTEGER,
  ADD COLUMN IF NOT EXISTS point_tier VARCHAR(2) CHECK (point_tier IN ('S','A','B','C'));

ALTER TABLE reversals_archive
  ADD COLUMN IF NOT EXISTS base_points INTEGER,
  ADD COLUMN IF NOT EXISTS total_points INTEGER,
  ADD COLUMN IF NOT EXISTS point_tier VARCHAR(2) CHECK (point_tier IN ('S','A','B','C'));

-- 2) Helpful indexes (IF NOT EXISTS for idempotency)
CREATE INDEX IF NOT EXISTS idx_reversals_points_tier ON reversals (total_points DESC, point_tier, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_reversals_base_points ON reversals (base_points DESC, timeframe, symbol);
CREATE INDEX IF NOT EXISTS idx_archive_points_tier ON reversals_archive (total_points DESC, point_tier, "timestamp" DESC);

-- 3) Update archive function to include points
DROP FUNCTION IF EXISTS archive_labeled_reversals(INTERVAL) CASCADE;
CREATE OR REPLACE FUNCTION archive_labeled_reversals(retention_period INTERVAL DEFAULT '120 days')
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER := 0;
    cutoff_date TIMESTAMPTZ;
BEGIN
    cutoff_date := NOW() - retention_period;

    INSERT INTO reversals_archive (
        id, symbol, timeframe, "timestamp", session,
        swing_trend_incoming, broke_last_higher_low, broke_last_lower_high, bars_since_peak_trough,
        rsi, rsi_lookback_max, rsi_lookback_min, move_vs_atr, price_to_vwap_pct, price_to_ema21_pct,
        rel_vol_20, vol_climax_flag, candle_pattern,
        ema8_cross_ema21, rsi_cross_50, vwap_retest_result,
        follow_through_atr, follow_through_bars_10,
        tf5_bias, daily_bias,
        outcome, quality_score, mfe_atr, mae_atr, rr_achieved,
        time_to_target_bars, time_to_stop_bars, structure_invalidated,
        source, run_id, schema_version, data_quality,
        created_at, updated_at,
        base_points, total_points, point_tier
    )
    SELECT
        id, symbol, timeframe, "timestamp", session,
        swing_trend_incoming, broke_last_higher_low, broke_last_lower_high, bars_since_peak_trough,
        rsi, rsi_lookback_max, rsi_lookback_min, move_vs_atr, price_to_vwap_pct, price_to_ema21_pct,
        rel_vol_20, vol_climax_flag, candle_pattern,
        ema8_cross_ema21, rsi_cross_50, vwap_retest_result,
        follow_through_atr, follow_through_bars_10,
        tf5_bias, daily_bias,
        outcome, quality_score, mfe_atr, mae_atr, rr_achieved,
        time_to_target_bars, time_to_stop_bars, structure_invalidated,
        source, run_id, schema_version, data_quality,
        created_at, updated_at,
        base_points, total_points, point_tier
    FROM reversals
    WHERE outcome IN ('gold', 'hard_negative')
      AND "timestamp" < cutoff_date
    ON CONFLICT (symbol, timeframe, "timestamp") DO NOTHING;

    GET DIAGNOSTICS archived_count = ROW_COUNT;

    DELETE FROM reversals
    WHERE outcome IN ('gold', 'hard_negative')
      AND "timestamp" < cutoff_date;

    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;
