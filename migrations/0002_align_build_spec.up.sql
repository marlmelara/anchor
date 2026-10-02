-- Align the schema with the build spec, and fix one design error found while
-- auditing M1 against it.

-- ---------------------------------------------------------------------------
-- 1. run_events must not reference a projection.
--
-- run_events had a foreign key to runs. That is backwards: runs is a cache
-- derived from run_events, so making the source of truth depend on it means the
-- log cannot exist without its own cache -- and it made the spec's acceptance
-- criterion ("drop runs and steps, rebuild reproduces them") impossible to
-- satisfy, because the delete was blocked by the log referencing the row.
-- ---------------------------------------------------------------------------
ALTER TABLE run_events DROP CONSTRAINT IF EXISTS run_events_run_id_fkey;

-- ---------------------------------------------------------------------------
-- 2. seq is 1-based, per the spec: "seq int NOT NULL -- 1, 2, 3 ... per run".
--
-- Events already written under the old 0-based scheme cannot be renumbered.
-- That is not an oversight in this migration: run_events is append-only and the
-- trigger rejects UPDATE, so there is no path that could rewrite them. A log you
-- can retroactively renumber is not a log.
--
-- Anchor has no production data, so the fix is to reset the development
-- database. This check exists to say that plainly rather than let the ALTER
-- fail with a bare constraint violation.
-- ---------------------------------------------------------------------------
DO $$
DECLARE legacy bigint;
BEGIN
    SELECT count(*) INTO legacy FROM run_events WHERE seq < 1;
    IF legacy > 0 THEN
        RAISE EXCEPTION
            'run_events holds % event(s) with a 0-based seq, written before sequence numbers became 1-based. An append-only log cannot be renumbered -- UPDATE is rejected by trigger, by design. Reset the development database with "make reset-db" and re-run migrations.', legacy
            USING HINT = 'make reset-db';
    END IF;
END $$;

ALTER TABLE run_events DROP CONSTRAINT IF EXISTS run_events_seq_positive;
ALTER TABLE run_events ADD  CONSTRAINT run_events_seq_positive CHECK (seq >= 1);

-- ---------------------------------------------------------------------------
-- 3. agents.definition -> agents.graph, the spec's name. The column holds a
--    declarative node graph, and "graph" says that where "definition" did not.
-- ---------------------------------------------------------------------------
ALTER TABLE agents RENAME COLUMN definition TO graph;

-- ---------------------------------------------------------------------------
-- 4. Nothing that is not itself a projection may hard-reference one.
--
-- queue and leases are live coordination state. They are keyed by run id, but
-- that id is the LOG's identity, not the projection row's -- so a foreign key
-- to runs was the same inversion as the one removed from run_events above, and
-- it made dropping the runs row impossible whenever a run was queued or leased.
--
-- steps is a different case: it is a projection, rebuilt in the same
-- transaction as runs. Its foreign key stays, but DEFERRABLE INITIALLY
-- DEFERRED moves the check to COMMIT, so a rebuild can delete both rows and
-- re-insert them without the intermediate state being rejected -- while still
-- refusing to leave a dangling step behind at the end.
-- ---------------------------------------------------------------------------
ALTER TABLE leases DROP CONSTRAINT IF EXISTS leases_run_id_fkey;
ALTER TABLE queue  DROP CONSTRAINT IF EXISTS queue_run_id_fkey;

ALTER TABLE steps  DROP CONSTRAINT IF EXISTS steps_run_id_fkey;
ALTER TABLE steps  ADD  CONSTRAINT steps_run_id_fkey
    FOREIGN KEY (run_id) REFERENCES runs (id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE runs   DROP CONSTRAINT IF EXISTS runs_replay_of_fkey;
ALTER TABLE runs   ADD  CONSTRAINT runs_replay_of_fkey
    FOREIGN KEY (replay_of) REFERENCES runs (id) DEFERRABLE INITIALLY DEFERRED;
