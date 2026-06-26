-- Phase 1 IBKR isolated market-data storage.
-- These tables are intentionally separate from market_candles and the existing candle pipeline.

CREATE TABLE IF NOT EXISTS ibkr_contracts (
    id BIGSERIAL PRIMARY KEY,

    asset_id UUID NULL,

    symbol TEXT NOT NULL,
    sec_type TEXT NOT NULL DEFAULT 'STK',
    exchange TEXT NOT NULL DEFAULT 'SMART',
    primary_exchange TEXT NULL,
    currency TEXT NOT NULL,

    conid BIGINT NULL,
    local_symbol TEXT NULL,
    trading_class TEXT NULL,

    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    ambiguous BOOLEAN NOT NULL DEFAULT FALSE,
    active BOOLEAN NOT NULL DEFAULT TRUE,

    last_resolved_at TIMESTAMP NULL,
    last_error TEXT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ibkr_contracts_identity
    ON ibkr_contracts (symbol, sec_type, exchange, currency, COALESCE(primary_exchange, ''));

CREATE TABLE IF NOT EXISTS ibkr_candles (
    id BIGSERIAL PRIMARY KEY,

    asset_id UUID NULL,
    symbol TEXT NOT NULL,
    conid BIGINT NULL,

    timeframe TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,

    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NULL,

    source TEXT NOT NULL DEFAULT 'IBKR',

    created_at TIMESTAMP NOT NULL DEFAULT now(),

    UNIQUE(symbol, conid, timeframe, timestamp)
);

CREATE TABLE IF NOT EXISTS ibkr_import_runs (
    id BIGSERIAL PRIMARY KEY,

    run_type TEXT NOT NULL,
    status TEXT NOT NULL,

    started_at TIMESTAMP NOT NULL DEFAULT now(),
    finished_at TIMESTAMP NULL,

    total_assets INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,

    error_message TEXT NULL,
    metadata JSONB NULL
);
