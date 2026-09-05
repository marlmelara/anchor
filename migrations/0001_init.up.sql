-- Anchor initial schema.
--
-- run_events is the source of truth. runs and steps are PROJECTIONS: they are
-- maintained in the same transaction as the event append purely for query
-- speed, and `anchorctl rebuild` must be able to drop and recompute both from
-- run_events alone. If that command ever produces a different result than the
-- live projection, the projection logic and the fold have drifted apart.

-- ---------------------------------------------------------------------------
-- agents: declarative agent graphs, versioned.
-- A run pins agent_version so replaying a historical run walks the graph it
-- originally ran, not whatever the graph looks like today.
-- ---------------------------------------------------------------------------
CREATE TABLE agents (
    name       text        NOT NULL,
    version    int         NOT NULL,
    definition jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, version)
);

-- ---------------------------------------------------------------------------
-- runs: projection. One row per submitted run.
-- ---------------------------------------------------------------------------
CREATE TABLE runs (
    id              uuid        PRIMARY KEY,
    tenant_id       text        NOT NULL,
    agent_name      text        NOT NULL,
    agent_version   int         NOT NULL,
    status          text        NOT NULL,
    input           jsonb       NOT NULL,
    budget_tokens   bigint      NOT NULL DEFAULT 0,
    budget_cents    bigint      NOT NULL DEFAULT 0,
    tokens_used     bigint      NOT NULL DEFAULT 0,
    cents_used      bigint      NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL,
    started_at      timestamptz,
    finished_at     timestamptz,
    terminal_output jsonb,
    error           text,
    -- set when this run is a replay of another run; the replay executes against
    -- a ReplaySource instead of live providers.
    replay_of       uuid        REFERENCES runs (id),

    CONSTRAINT runs_status_check CHECK (status IN (
        'pending', 'running', 'completed', 'failed', 'cancelled', 'needs_review'
    )),
    CONSTRAINT runs_agent_fk FOREIGN KEY (agent_name, agent_version)
        REFERENCES agents (name, version)
);

CREATE INDEX runs_tenant_created_idx ON runs (tenant_id, created_at DESC);
CREATE INDEX runs_status_idx         ON runs (status);
CREATE INDEX runs_replay_of_idx      ON runs (replay_of) WHERE replay_of IS NOT NULL;

-- ---------------------------------------------------------------------------
-- run_events: THE SOURCE OF TRUTH. Append-only.
--
-- UNIQUE (run_id, seq) is the optimistic-concurrency guard and the reason this
-- system needs no distributed lock. Two workers racing on the same run both
-- compute the same next seq; one commits, the other's insert violates the
-- constraint, its transaction aborts, and it releases the run. Cheap mutual
-- exclusion with no extra machinery.
-- ---------------------------------------------------------------------------
CREATE TABLE run_events (
    id         bigserial   PRIMARY KEY,
    run_id     uuid        NOT NULL REFERENCES runs (id),
    seq        int         NOT NULL,
    type       text        NOT NULL,
    payload    jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT run_events_run_seq_unique UNIQUE (run_id, seq),
    CONSTRAINT run_events_seq_positive   CHECK (seq >= 0)
);

-- The fold always reads a whole run in seq order.
CREATE INDEX run_events_run_seq_idx ON run_events (run_id, seq);

-- Append-only is enforced by the database, not by convention. Tests reset
-- state with TRUNCATE, which does not fire row-level triggers.
CREATE OR REPLACE FUNCTION run_events_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION
        'run_events is append-only: rejected % on run_id=% seq=%',
        TG_OP, OLD.run_id, OLD.seq
        USING ERRCODE = 'restrict_violation';
END;
$$;

CREATE TRIGGER run_events_append_only
    BEFORE UPDATE OR DELETE ON run_events
    FOR EACH ROW EXECUTE FUNCTION run_events_reject_mutation();

-- ---------------------------------------------------------------------------
-- steps: projection. One row per execution record (a single node iteration).
--
-- idempotency_key = sha256(run_id || step_index || canonical_json(input)) and
-- is UNIQUE across the whole table. Before executing a side-effecting step the
-- worker inserts the key; a conflict means the step already ran, so the worker
-- reads the recorded result instead of executing it a second time.
-- ---------------------------------------------------------------------------
CREATE TABLE steps (
    run_id          uuid        NOT NULL REFERENCES runs (id),
    step_index      int         NOT NULL,
    kind            text        NOT NULL,
    name            text        NOT NULL,
    node_id         text        NOT NULL DEFAULT '',
    idempotency_key text        NOT NULL,
    status          text        NOT NULL,
    attempt         int         NOT NULL DEFAULT 0,
    started_at      timestamptz,
    finished_at     timestamptz,
    tokens          bigint      NOT NULL DEFAULT 0,
    cents           bigint      NOT NULL DEFAULT 0,
    error           text,

    PRIMARY KEY (run_id, step_index),
    CONSTRAINT steps_kind_check   CHECK (kind IN ('model', 'tool')),
    CONSTRAINT steps_status_check CHECK (status IN (
        'scheduled', 'running', 'completed', 'failed', 'retrying', 'needs_review'
    ))
);

CREATE UNIQUE INDEX steps_idempotency_key_uniq ON steps (idempotency_key);

-- ---------------------------------------------------------------------------
-- leases: worker ownership of a run. One owner at a time.
-- A worker claims with an expiry and heartbeats; an expired lease may be taken
-- over by any other worker. A zombie worker that wakes up after takeover cannot
-- corrupt anything -- its event append hits the (run_id, seq) constraint.
-- ---------------------------------------------------------------------------
CREATE TABLE leases (
    run_id       uuid        PRIMARY KEY REFERENCES runs (id),
    worker_id    text        NOT NULL,
    acquired_at  timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL
);

CREATE INDEX leases_expires_at_idx ON leases (expires_at);

-- ---------------------------------------------------------------------------
-- queue: runs awaiting execution. Claimed with SELECT ... FOR UPDATE SKIP
-- LOCKED. No Redis queue, no Kafka -- one fewer moving part.
-- ---------------------------------------------------------------------------
CREATE TABLE queue (
    run_id       uuid        PRIMARY KEY REFERENCES runs (id),
    available_at timestamptz NOT NULL DEFAULT now(),
    priority     int         NOT NULL DEFAULT 0,
    enqueued_at  timestamptz NOT NULL DEFAULT now()
);

-- Matches the claim query's ORDER BY priority, available_at.
CREATE INDEX queue_claim_idx ON queue (priority, available_at);
