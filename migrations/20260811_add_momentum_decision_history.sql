CREATE TABLE IF NOT EXISTS momentum_engine_decision_history (
    id BIGSERIAL PRIMARY KEY,
    market_scope VARCHAR(32) NOT NULL DEFAULT 'crypto',
    decision_id VARCHAR(96) NOT NULL UNIQUE,
    payload_json JSON NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_momentum_engine_decision_history_scope_produced
    ON momentum_engine_decision_history (market_scope, produced_at);
