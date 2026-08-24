ALTER TABLE candidate_executions ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE candidate_executions ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMP WITH TIME ZONE;
UPDATE candidate_executions SET status = 'completed' WHERE status = 'executed';
