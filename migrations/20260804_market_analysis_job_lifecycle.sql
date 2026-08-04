ALTER TABLE market_data_job_requests ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE market_data_job_requests ADD COLUMN IF NOT EXISTS worker_id TEXT NULL;
ALTER TABLE market_data_job_requests ADD COLUMN IF NOT EXISTS started_at TIMESTAMP NULL;
ALTER TABLE market_data_job_requests ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP NULL;
ALTER TABLE market_data_job_requests ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP NULL;
ALTER TABLE market_data_job_requests ADD COLUMN IF NOT EXISTS last_error TEXT NULL;
CREATE INDEX IF NOT EXISTS ix_market_data_jobs_claim ON market_data_job_requests(job_type, status, created_at);
