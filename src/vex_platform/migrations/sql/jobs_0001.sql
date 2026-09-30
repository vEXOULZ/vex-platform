-- vex-platform jobs 0001: job runs and their event log. Applied after procrastinate_3.10.0.sql, in the
-- same schema (see vex_platform.migrations.jobs_sql).

CREATE TABLE job_runs (
    id bigserial PRIMARY KEY,
    kind text NOT NULL,
    -- What the run works on, as "type:id" (vod:123, channel:456). NULL until a step learns it.
    subject text,
    state text NOT NULL CHECK (state IN ('queued', 'running', 'paused', 'succeeded', 'failed', 'cancelled')),
    -- The first step not yet finished: where the run resumes. NULL once it succeeded.
    step text,
    payload jsonb NOT NULL DEFAULT '{}',
    attempts integer NOT NULL DEFAULT 0,
    last_error text,
    not_before timestamptz,
    -- Steps to pause before; NULL means the kind's default gates.
    pause_before text[],
    pause_next boolean NOT NULL DEFAULT false,
    cancel_requested boolean NOT NULL DEFAULT false,
    -- At most one queued run per (kind, queued_key), and one queued/running/paused run per
    -- (kind, active_key): enqueue dedupes on them.
    queued_key text,
    active_key text,
    actor_kind text NOT NULL,
    actor_id text,
    actor_login text,
    via text NOT NULL,
    procrastinate_job_id bigint,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz
);

CREATE UNIQUE INDEX job_runs_queued_key ON job_runs (kind, queued_key)
    WHERE state = 'queued' AND queued_key IS NOT NULL;
CREATE UNIQUE INDEX job_runs_active_key ON job_runs (kind, active_key)
    WHERE state IN ('queued', 'running', 'paused') AND active_key IS NOT NULL;
CREATE INDEX job_runs_state ON job_runs (state, id);
CREATE INDEX job_runs_kind_subject ON job_runs (kind, subject, id);
CREATE INDEX job_runs_subject ON job_runs (subject, id);
CREATE INDEX job_runs_payload ON job_runs USING gin (payload jsonb_path_ops);

CREATE FUNCTION job_runs_touch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER job_runs_touch BEFORE UPDATE ON job_runs FOR EACH ROW EXECUTE FUNCTION job_runs_touch();

CREATE TABLE job_run_events (
    id bigserial PRIMARY KEY,
    run_id bigint NOT NULL REFERENCES job_runs (id) ON DELETE CASCADE,
    at timestamptz NOT NULL DEFAULT now(),
    level text NOT NULL CHECK (level IN ('info', 'warning', 'error')),
    step text,
    message text NOT NULL,
    -- {done, total, unit} for a progress report; NULL for a plain log line.
    progress jsonb
);

CREATE INDEX job_run_events_run ON job_run_events (run_id, id);
