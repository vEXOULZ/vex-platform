-- The run that queued this one, when a step did (StepContext.enqueue, or enqueue with a "job" actor): the
-- runs a job started make a tree that GET /jobs/{id}/related walks. A deleted parent leaves its children.
ALTER TABLE job_runs ADD COLUMN parent_id bigint REFERENCES job_runs (id) ON DELETE SET NULL;
CREATE INDEX job_runs_parent_id ON job_runs (parent_id) WHERE parent_id IS NOT NULL;
