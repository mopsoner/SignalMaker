-- Isolate shared momentum-engine state by asset market.
ALTER TABLE momentum_engine_positions ADD COLUMN IF NOT EXISTS market_scope VARCHAR(32) NOT NULL DEFAULT 'crypto';
ALTER TABLE momentum_engine_trades ADD COLUMN IF NOT EXISTS market_scope VARCHAR(32) NOT NULL DEFAULT 'crypto';
ALTER TABLE momentum_engine_current_decision ADD COLUMN IF NOT EXISTS market_scope VARCHAR(32) NOT NULL DEFAULT 'crypto';
ALTER TABLE momentum_structure_current ADD COLUMN IF NOT EXISTS market_scope VARCHAR(32) NOT NULL DEFAULT 'crypto';

CREATE UNIQUE INDEX IF NOT EXISTS ux_momentum_engine_positions_scope_id ON momentum_engine_positions (market_scope, position_id);
CREATE INDEX IF NOT EXISTS ix_momentum_engine_positions_scope_strategy_status_opened ON momentum_engine_positions (market_scope, strategy, status, opened_at);
CREATE UNIQUE INDEX IF NOT EXISTS ux_momentum_engine_trades_scope_id ON momentum_engine_trades (market_scope, trade_id);
CREATE INDEX IF NOT EXISTS ix_momentum_engine_trades_scope_strategy_created ON momentum_engine_trades (market_scope, strategy, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS ux_momentum_engine_decision_scope ON momentum_engine_current_decision (market_scope);
CREATE UNIQUE INDEX IF NOT EXISTS ux_momentum_structure_scope_symbol ON momentum_structure_current (market_scope, symbol);
