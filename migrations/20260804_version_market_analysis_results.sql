-- Lossless shared analysis DTO. Existing rows remain explicitly version 1.
ALTER TABLE market_analysis_results ADD COLUMN IF NOT EXISTS stage TEXT NULL;
ALTER TABLE market_analysis_results ADD COLUMN IF NOT EXISTS payload_version INTEGER NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_market_analysis_results_latest
  ON market_analysis_results(asset_id, engine_name, timeframe, payload_version, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_analysis_results_filters
  ON market_analysis_results(engine_name, stage, signal, timeframe, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_analysis_results_run
  ON market_analysis_results(analysis_run_id);
COMMENT ON COLUMN market_analysis_results.payload_version IS
  '1=legacy simplified payload; 2=lossless shared workflow DTO';
