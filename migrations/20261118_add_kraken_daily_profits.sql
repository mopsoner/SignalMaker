-- Incremental verified Kraken realized profit. No historical recalculation.
CREATE TABLE IF NOT EXISTS kraken_profit_contributions (
    position_id VARCHAR(64) PRIMARY KEY,
    day VARCHAR(10) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    value DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS kraken_daily_profits (
    day VARCHAR(10) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    value DOUBLE PRECISION NOT NULL DEFAULT 0,
    closed_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, symbol, currency)
);
