CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS market_universes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL UNIQUE, description TEXT NULL,
  region TEXT NULL, asset_type TEXT NULL, currency TEXT NULL, provider TEXT NOT NULL DEFAULT 'EODHD',
  enabled BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS market_assets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), universe_id UUID NULL REFERENCES market_universes(id),
  symbol TEXT NOT NULL, provider_symbol TEXT NOT NULL, exchange_code TEXT NULL, name TEXT NULL,
  asset_type TEXT NOT NULL, region TEXT NULL, country TEXT NULL, currency TEXT NULL, isin TEXT NULL, mic TEXT NULL,
  pea_eligible BOOLEAN NULL, ucits BOOLEAN NULL, enabled BOOLEAN NOT NULL DEFAULT TRUE, priority INTEGER NOT NULL DEFAULT 100,
  last_synced_at TIMESTAMP NULL, last_error TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now(),
  UNIQUE(provider_symbol, asset_type)
);

ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS asset_id UUID NULL REFERENCES market_assets(id);
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS provider TEXT DEFAULT 'EODHD';
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS provider_symbol TEXT NULL;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS timeframe TEXT NULL;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS timestamp TIMESTAMP NULL;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS adjusted_close NUMERIC NULL;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT now();
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT now();
CREATE UNIQUE INDEX IF NOT EXISTS uq_market_candles_asset_provider_time ON market_candles(asset_id, provider, timeframe, timestamp) WHERE asset_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS market_data_import_runs (
  id BIGSERIAL PRIMARY KEY, provider TEXT NOT NULL, run_type TEXT NOT NULL, status TEXT NOT NULL,
  started_at TIMESTAMP NOT NULL DEFAULT now(), finished_at TIMESTAMP NULL, total_assets INTEGER DEFAULT 0,
  success_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, error_message TEXT NULL, metadata JSONB NULL
);

CREATE TABLE IF NOT EXISTS market_analysis_runs (
  id BIGSERIAL PRIMARY KEY, engine_name TEXT NOT NULL, universe_id UUID NULL REFERENCES market_universes(id),
  timeframe TEXT NOT NULL DEFAULT '1d', status TEXT NOT NULL, started_at TIMESTAMP NOT NULL DEFAULT now(), finished_at TIMESTAMP NULL,
  total_assets INTEGER DEFAULT 0, success_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, metadata JSONB NULL, error_message TEXT NULL
);

CREATE TABLE IF NOT EXISTS market_analysis_results (
  id BIGSERIAL PRIMARY KEY, analysis_run_id BIGINT NULL REFERENCES market_analysis_runs(id), asset_id UUID NOT NULL REFERENCES market_assets(id),
  engine_name TEXT NOT NULL, timeframe TEXT NOT NULL, signal TEXT NULL, score NUMERIC NULL, trend TEXT NULL, confidence NUMERIC NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMP NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_market_analysis_results_asset_engine_time_created ON market_analysis_results(asset_id, engine_name, timeframe, created_at);

CREATE TABLE IF NOT EXISTS market_data_job_requests (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    payload JSONB NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
