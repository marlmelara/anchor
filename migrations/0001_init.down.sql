DROP TABLE IF EXISTS queue;
DROP TABLE IF EXISTS leases;
DROP TABLE IF EXISTS steps;
DROP TRIGGER IF EXISTS run_events_append_only ON run_events;
DROP FUNCTION IF EXISTS run_events_reject_mutation();
DROP TABLE IF EXISTS run_events;
DROP TABLE IF EXISTS runs;
DROP TABLE IF EXISTS agents;
