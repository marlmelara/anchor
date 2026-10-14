-- Lossy: sub-cent precision is discarded, which is the whole reason for the
-- forward migration.
UPDATE runs  SET budget_micros = budget_micros / 10000,
                 micros_used   = micros_used   / 10000;
UPDATE steps SET cost_micros   = cost_micros   / 10000;

ALTER TABLE steps RENAME COLUMN cost_micros TO cents;
ALTER TABLE runs  RENAME COLUMN micros_used   TO cents_used;
ALTER TABLE runs  RENAME COLUMN budget_micros TO budget_cents;
