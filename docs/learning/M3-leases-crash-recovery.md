# M3 — Leases and crash recovery

> Study material for the M3 checkpoint quiz. Every code block is the real code.

---

## 1. Plain-English summary

A worker can die at any moment. If it is killed with `SIGKILL` it gets no
warning and runs no cleanup code — it simply stops existing. Its run must not be
lost, and whatever that run had already done must not happen a second time.

Anchor solves this by never relying on the dying worker to do anything.
Ownership of a run is a **lease**: a row saying "worker X owns this until
10:04:32". The live worker pushes that deadline forward every few seconds. A
dead one stops pushing, the deadline passes, and a background **reaper** puts
the run back on the queue.

The next worker folds the run's journal, walks the graph from the start, and
skips every step the log already shows as finished — so it continues from
exactly the next unfinished step rather than redoing work.

*(147 words)*

---

## 2. Data flow: a worker is killed mid-run

1. **`worker-1` holds the lease.** `Worker.heartbeat` (`internal/worker/worker.go`)
   runs `UPDATE leases SET expires_at = now() + 30s WHERE run_id = $1 AND
   worker_id = $2` every 10 seconds.

2. **`kill -9`.** No deferred function runs. No lease is released. The `leases`
   row is left behind pointing at a process that no longer exists.

3. **The deadline passes.** Up to 30 seconds later, `expires_at < now()`.

4. **`store.ReapExpiredLeases`** — running inside *every* worker — deletes the
   expired lease and inserts the run back into `queue`, skipping runs the
   projection shows as terminal.

5. **`worker-2` claims it** through the ordinary `ClaimNext` path. Nothing about
   this claim is special; it is the same `SKIP LOCKED` transaction used for new
   work.

6. **`Worker.executeRun`** folds the journal. `StartedAt` is already set, so it
   appends `RunStarted` with `resumed: true` — the marker the chaos test asserts
   on.

7. **The interpreter re-walks the graph from node one.** For each step it checks
   the folded state: completed steps are reused, costing nothing. It lands on
   the first unfinished step and continues.

8. **If the dead worker died inside the ambiguous window** — after reserving a
   step's idempotency key but before journaling a result — `beginStep` finds the
   key already present while the journal shows nothing there. For an idempotent
   tool it re-runs. For a non-idempotent one it writes `RunNeedsReview`.

---

## 3. Annotated code

### 3.1 The reaper — `internal/store/claim.go`

```go
tag, err := s.pool.Exec(ctx, `
	WITH expired AS (
		DELETE FROM leases WHERE expires_at < $1 RETURNING run_id
	)
	INSERT INTO queue (run_id, available_at, priority, enqueued_at)
	SELECT e.run_id, $1, 0, $1
	FROM expired e
	JOIN runs r ON r.id = e.run_id
	WHERE r.status NOT IN ('completed', 'failed', 'cancelled', 'needs_review')
	ON CONFLICT (run_id) DO NOTHING`, now)
```

- **`WITH expired AS (DELETE ... RETURNING run_id)`** is a CTE (common table
  expression): a named subquery. Here it deletes and hands the deleted ids to
  the `INSERT` in the same statement, so a lease cannot be removed without the
  run being requeued.
- **The `JOIN runs`** filters out finished runs. This reads a *projection* to
  make a scheduling decision, which is normally something to avoid — it is safe
  here only because being wrong costs one wasted claim: the worker would fold
  the log, see a terminal state, and release.
- **`ON CONFLICT (run_id) DO NOTHING`** makes it idempotent. Every worker runs
  the reaper, so two can reap the same lease at once; without this they would
  race to insert the same queue row.

**Why every worker reaps.** A single designated reaper is one more process whose
death stops recovery — and recovery is precisely the thing that must keep
working when processes die.

### 3.2 The heartbeat, and what a `false` return means

```go
func (s *Store) Heartbeat(ctx context.Context, runID uuid.UUID, workerID string, leaseFor time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE leases SET heartbeat_at = $1, expires_at = $2
		WHERE run_id = $3 AND worker_id = $4`, ...)
	return tag.RowsAffected() == 1, nil
}
```

`AND worker_id = $4` is the whole point. If another worker has taken the run
over, that row no longer carries this worker's id, zero rows update, and the
function returns `false`.

In the worker:

```go
if !ok {
	w.cfg.Logf("run %s: lease lost, stopping", runID)
	cancelRun()
	return
}
```

**This is a correctness check, not telemetry.** A worker that kept going after
losing its lease would mean two workers executing one run. The `(run_id, seq)`
constraint would eventually catch the second writer — but only *after* it had
already performed a side effect.

### 3.3 Resumption, and the bug that was hiding in it

```go
func (e *execution) beginStep(...) (stepDecision, error) {
	index := e.cursor
	e.cursor++

	if existing, ok := e.st.Step(index); ok {
		if existing.Status == journal.StepStatusCompleted {
			return stepDecision{index: index, recorded: existing}, nil
		}
		return stepDecision{index: index}, nil
	}
	// ... reserve the key, journal StepScheduled
}
```

Three cases, and the third is where crash recovery actually lives:

1. **Completed in the journal** → reuse it. This is how a resumed run replays
   its finished prefix for free.
2. **Scheduled or failed in the journal** → an earlier attempt got this far.
   Execute, but do not journal `StepScheduled` again — the fold allows exactly
   one per index and would reject it.
3. **Nothing in the journal** → reserve the idempotency key and journal. If the
   reservation reports the key **already exists** while the journal shows
   nothing, a previous worker reserved it and died before recording anything.
   That is the ambiguous window, and only the tool's `idempotent` declaration
   can say what to do about it.

`index := e.cursor` is load-bearing. See §6.

---

## 4. New terms

| Term | One sentence |
|---|---|
| Reaper | A background job that cleans up resources whose owner disappeared. |
| Lease expiry | Ownership ending automatically because it was not renewed. |
| Takeover | Another worker claiming a run whose lease expired. |
| Zombie worker | A worker that froze, lost its lease, then resumed believing it still owns the run. |
| `SIGKILL` vs `SIGTERM` | `SIGTERM` asks a process to stop and it can clean up; `SIGKILL` stops it instantly with no cleanup. |
| Graceful shutdown | Stopping new work, finishing or checkpointing current work, then exiting. |
| Rolling restart | Replacing processes one at a time so the service stays up. |
| Chaos testing | Deliberately breaking things to prove recovery works. |
| CTE | A named subquery in SQL, written `WITH name AS (...)`. |
| Fault injection | Deliberately causing failures to test behaviour under them. |

---

## 5. Decisions made here

In [`../decisions.md`](../decisions.md):

- Every worker runs the reaper, rather than one designated reaper process
- The reaper reads the `runs` projection to skip terminal runs
- Chaos tests spawn real processes and send real `SIGKILL`
- The chaos effect is a committed database row, not an in-process counter

---

## 6. Struggles

### Resumption re-executed the whole graph

**Symptom.** Under the chaos loop, runs completed with **3 or 4 steps instead of
2**. Every single-worker test passed.

**Cause.** The step index came from `len(e.st.Steps)`. That equals the walk
position only on a **fresh** run. On a resumed run the folded state already
holds the completed steps, so the walk started past the end and executed the
whole graph again at higher indexes.

**How it was found.** Only the chaos suite surfaced it, because only the chaos
suite produced a resumed run. Normal tests never crashed a worker mid-run, so
`len(Steps)` and the walk position never diverged.

**Fix.** An explicit `cursor` on the execution, incremented once per step the
walk reaches — including steps replayed from the journal rather than executed.

**What it taught.** The comment above that line already described the correct
design ("step indexes are assigned sequentially as the walk proceeds"). The code
did something subtly different that happened to agree in the only case the tests
covered. A test that never produces the second case cannot tell them apart.

### A chaos test that passed after one kill

**Symptom.** The first chaos suite passed in 2.3 seconds, reporting **1 kill**.

**Cause.** 40 runs against a 20ms mock finished in about two seconds, while the
chaos loop killed a worker once per second. The suite was green and had tested
essentially nothing.

**Fix.** Three changes: slow the mock to 400ms so work outlasts the chaos, assert
a **minimum kill count**, and — the one that matters — assert at least one
`RunStarted` carries `resumed: true`.

**What it taught.** A passing chaos test is not evidence unless it also proves
the chaos happened. Counting completed runs cannot distinguish "survived a kill"
from "was never interrupted". With the real assertions in place the suite
immediately failed and found the bug above.

### Proving a regression test actually regresses

After fixing the cursor bug, the fix was verified by **reintroducing the bug**
and confirming `TestResumeReusesCompletedStepsInsteadOfRepeatingThem` failed
with `len(Steps) = 3, want 2`, then restoring it. A regression test that has
never been seen to fail is a guess about what it covers.

---

## 7. Checkpoint quiz check

The five M3 questions still match the code. Notes:

- **Q1 — "Walk through what happens when a worker is `SIGKILL`ed mid-run."** The
  published answer is right. The detail worth adding is *why* recovery cannot
  involve the dying worker at all: `SIGKILL` runs no cleanup, so any design
  depending on the loser releasing its lease is broken by construction.
- **Q5 — "How does the chaos test prove there are no duplicate effects?"** Be
  specific that the effect is a **committed database row**, not an in-process
  counter — a counter dies with the process and could never measure a kill.

**Added questions:**

6. Why does every worker run the reaper rather than one designated process?
   *Key: a single reaper is one more process whose death stops recovery, which
   is exactly what must keep working when processes die. Concurrent reaping is
   made safe by `ON CONFLICT DO NOTHING`.*
7. A worker resumes a run whose journal shows step 2 scheduled but not
   completed. What does it do, and what decides?
   *Key: it does not re-journal `StepScheduled` — the fold allows one per index.
   It reserves nothing new; the key is already there. Whether it re-executes is
   decided by the tool's `idempotent` flag: idempotent retries, non-idempotent
   halts as `RunNeedsReview`, because the effect may or may not have landed.*
8. Why is reading the `runs` projection acceptable in the reaper, when execution
   never reads it?
   *Key: it is only a scheduling decision. If the projection were stale and a
   finished run were requeued, the worker would fold the log, see a terminal
   state, and release — the cost of being wrong is one wasted claim, not
   incorrect execution.*
