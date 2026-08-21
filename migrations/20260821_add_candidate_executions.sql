CREATE TABLE IF NOT EXISTS candidate_executions (
    execution_id VARCHAR(96) PRIMARY KEY,
    candidate_id VARCHAR(64) NOT NULL REFERENCES trade_candidates(candidate_id) ON DELETE CASCADE,
    execution_mode VARCHAR(16) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'claimed',
    claimed_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    error VARCHAR(512),
    CONSTRAINT uq_candidate_execution_mode UNIQUE (candidate_id, execution_mode)
);

CREATE INDEX IF NOT EXISTS ix_candidate_executions_status
    ON candidate_executions (status);
