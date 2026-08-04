-- Bounded admin histories and status filters must remain index-only as run tables grow.
CREATE INDEX IF NOT EXISTS idx_market_import_runs_status_started
    ON market_data_import_runs (status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_analysis_runs_status_started
    ON market_analysis_runs (status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_analysis_runs_engine_started
    ON market_analysis_runs (engine_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_jobs_status_created
    ON market_data_job_requests (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_jobs_type_status_created
    ON market_data_job_requests (job_type, status, created_at DESC);
