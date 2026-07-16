ALTER TABLE momentum_current
    ADD COLUMN IF NOT EXISTS momentum_candle_time_15m TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS momentum_candle_time_1h TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS momentum_candle_time_4h TIMESTAMPTZ;
