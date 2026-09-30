-- vex-platform audit 0001: the shared audit log. __TABLE__ is the (optionally schema-qualified) table
-- name, __NAME__ its bare name for index names (see vex_platform.migrations.audit_sql).
-- Append-only: grant the application role SELECT and INSERT on it, nothing more.

CREATE TABLE __TABLE__ (
    id bigserial PRIMARY KEY,
    at timestamptz NOT NULL DEFAULT now(),
    actor_kind text NOT NULL CHECK (actor_kind IN ('user', 'api_key', 'system', 'job', 'anonymous')),
    actor_id text,
    actor_login text,
    via text NOT NULL CHECK (via IN ('api', 'web', 'chat', 'cli', 'job', 'system')),
    -- Dotted noun.verb: vod.update, job.cancel, cc.create.
    action text NOT NULL,
    -- "type:id": vod:123, setting:runner_concurrency.
    target text,
    -- The channel the change belongs to; NULL for a global one.
    scope text,
    outcome text NOT NULL DEFAULT 'ok' CHECK (outcome IN ('ok', 'denied', 'failed')),
    before jsonb,
    after jsonb,
    detail jsonb,
    request_id text,
    job_run_id bigint
);

CREATE INDEX __NAME___at ON __TABLE__ (at);
CREATE INDEX __NAME___target ON __TABLE__ (target, id);
CREATE INDEX __NAME___scope ON __TABLE__ (scope, id);
CREATE INDEX __NAME___actor ON __TABLE__ (actor_kind, actor_id, id);
CREATE INDEX __NAME___action ON __TABLE__ (action text_pattern_ops, id);
