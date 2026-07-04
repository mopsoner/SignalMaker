ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS number_of_trades INTEGER NOT NULL DEFAULT 0;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS taker_buy_base_volume DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS taker_buy_quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS provider VARCHAR(32) NOT NULL DEFAULT 'KRAKEN';
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS asset_id VARCHAR(96);
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS provider_symbol VARCHAR(64);
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS asset_type VARCHAR(32);
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS currency VARCHAR(16);
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS exchange VARCHAR(64);
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS universe VARCHAR(128);
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS metadata_json JSON;

CREATE TABLE IF NOT EXISTS market_data_import_runs (
    id VARCHAR(96) PRIMARY KEY,
    provider VARCHAR(32),
    run_type VARCHAR(64),
    status VARCHAR(32),
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    total_assets INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_market_candles_symbol_interval_close_time_desc
ON market_candles (symbol, interval, close_time DESC);

CREATE INDEX IF NOT EXISTS idx_market_candles_symbol_interval_open_time_asc
ON market_candles (symbol, interval, open_time ASC);

CREATE INDEX IF NOT EXISTS idx_market_candles_ingested_at_desc
ON market_candles (ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_candles_provider_symbol_interval
ON market_candles (provider, symbol, interval);
