-- Money moves from integer cents to micro-dollars (one millionth of a dollar).
--
-- Cents cannot represent what a model call costs. A short call on Opus 5.5 --
-- 36 input and 143 output tokens -- costs about $0.003, which is zero in
-- integer cents. A cost budget counted in cents therefore rounds almost every
-- real run to nothing and can never trigger, which makes it worse than having
-- no budget at all: the operator believes they have one.
--
-- Micro-dollars give six decimal places, so that same call records 3004 and a
-- budget of "stop at fifty cents" (500000) is enforceable.
--
-- This is cheap now because both columns currently hold only zeros -- nothing
-- has ever written a cost. It would be expensive once there is real data, which
-- is the argument for doing it before anything depends on it.

ALTER TABLE runs RENAME COLUMN budget_cents TO budget_micros;
ALTER TABLE runs RENAME COLUMN cents_used   TO micros_used;

ALTER TABLE steps RENAME COLUMN cents TO cost_micros;

-- Any value already stored was whole cents; 1 cent is 10,000 micro-dollars.
-- In practice every row is zero, but converting is correct and costs nothing.
UPDATE runs  SET budget_micros = budget_micros * 10000,
                 micros_used   = micros_used   * 10000;
UPDATE steps SET cost_micros   = cost_micros   * 10000;

COMMENT ON COLUMN runs.budget_micros IS 'spending cap for this run, in micro-dollars (1e-6 USD)';
COMMENT ON COLUMN runs.micros_used  IS 'spent so far, in micro-dollars (1e-6 USD)';
COMMENT ON COLUMN steps.cost_micros IS 'what this step cost, in micro-dollars (1e-6 USD)';
