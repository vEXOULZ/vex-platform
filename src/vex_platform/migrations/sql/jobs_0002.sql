-- The channel (or other scope) a run belongs to, given at enqueue. Every audit row about the run carries
-- it, not only job.enqueue's, so a reader limited to some scopes sees the whole life of their runs.
ALTER TABLE job_runs ADD COLUMN scope text;
