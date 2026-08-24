ALTER TABLE candidate_executions ADD COLUMN next_attempt_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE candidate_executions ADD COLUMN submitted_at TIMESTAMP WITH TIME ZONE;
UPDATE candidate_executions SET status = 'completed' WHERE status = 'executed';
