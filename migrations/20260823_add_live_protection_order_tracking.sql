ALTER TABLE candidate_executions ADD COLUMN entry_order_id VARCHAR(64);
ALTER TABLE candidate_executions ADD COLUMN entry_order_status VARCHAR(32);
ALTER TABLE candidate_executions ADD COLUMN take_profit_order_id VARCHAR(64);
ALTER TABLE candidate_executions ADD COLUMN take_profit_order_status VARCHAR(32);

ALTER TABLE positions ADD COLUMN entry_order_id VARCHAR(64);
ALTER TABLE positions ADD COLUMN entry_order_status VARCHAR(32);
ALTER TABLE positions ADD COLUMN take_profit_order_id VARCHAR(64);
ALTER TABLE positions ADD COLUMN take_profit_order_status VARCHAR(32);
