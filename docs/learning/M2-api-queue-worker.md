# M2 — API, queue, worker, idempotency

> Study material for the M2 checkpoint quiz. Every code block is the real code.

---

## 1. Plain-English summary

M1 could record a run. M2 actually runs one.

A **worker** is a process holding a pool of goroutines (lightweight threads
Go manages itself). Each goroutine loops: take one run off the queue, execute
it, write down what happened, let it go.

Taking a run uses a Postgres trick. `SELECT ... FOR UPDATE SKIP LOCKED` locks
the row it picks and makes *other* workers step over rows that are already
locked instead of waiting in line. Ten workers can each grab a different run at
the same moment with no coordination between them.

Before any step that touches the outside world, the worker writes an
**idempotency key** — a fingerprint of (run, step number, input). The database
rejects duplicates, so if the same step is somehow attempted twice, the second
attempt sees the key already exists and skips the work instead of doing it
again.

*(149 words)*

---

## 2. Data flow: `POST /v1/runs` to a completed run

1. **`internal/api/api.go` → `handleSubmitRun`.** Checks the bearer token, then
   the queue depth: above the high-water mark it returns **429** rather than
   accepting work it cannot run. Resolves the agent's latest version if the
   caller did not pin one.

2. **`store.SubmitRun`** writes `RunSubmitted` and the `queue` row in **one
   transaction**. A run in the log but not the queue is never executed; in the
   queue but not the log is claimed by a worker that finds nothing to fold.

3. **`internal/worker/worker.go` → `Worker.Run`** has `PoolSize` goroutines
   looping. Each calls `store.ClaimNext`.

4. **`store.ClaimNext`** (`internal/store/claim.go`) — one transaction:
   `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`, `DELETE FROM queue`, `INSERT INTO
   leases`. All three commit together.

5. **`Worker.executeRun`** folds the run from the journal (never from the
   projection), starts a heartbeat goroutine, loads and parses the pinned agent
   graph, appends `RunStarted`, and hands off to the interpreter.

6. **`internal/worker/interpret.go` → `interpret`** walks the graph. For each
   node: `scheduleStep` reserves the idempotency key and journals
   `StepScheduled`, then the effect runs, then `…Completed` or `StepFailed` is
   journaled.

7. On the last node, `RunCompleted` is appended with the final output. The
   deferred `ReleaseLease` in `executeRun` gives the run back.

---

## 3. Annotated code

### 3.1 Claiming work — `internal/store/claim.go`

```go
err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
	err := tx.QueryRow(ctx, `
		SELECT run_id FROM queue
		WHERE available_at <= $1
		ORDER BY priority, available_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1`, now).Scan(&runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoWork
	}
	...
	tx.Exec(ctx, `DELETE FROM queue WHERE run_id = $1`, runID)
	tx.Exec(ctx, `INSERT INTO leases (...) VALUES (...)
	              ON CONFLICT (run_id) DO UPDATE SET ...`)
	return nil
})
```

- `pgx.BeginFunc` runs the closure in a transaction: commits if it returns
  `nil`, rolls back on an error. Python: `with conn.transaction():`.
- **`FOR UPDATE`** locks the chosen row until the transaction ends.
- **`SKIP LOCKED`** is the important half. Without it, ten workers all queue
  behind the same row and the pool serialises. With it they each get a
  different run and nobody blocks.
- The delete and the lease insert are in the **same transaction** as the select,
  so a run can never be dequeued without an owner, or owned while still queued.
- **`ON CONFLICT DO UPDATE`** is an upsert — a run whose previous owner died
  leaves a stale lease row, and taking over is an overwrite, not an insert.

Python equivalent:

```python
with conn.transaction():
    row = conn.execute("""SELECT run_id FROM queue WHERE available_at <= %s
                          ORDER BY priority, available_at
                          FOR UPDATE SKIP LOCKED LIMIT 1""", (now,)).fetchone()
    if row is None:
        raise NoWork()
    conn.execute("DELETE FROM queue WHERE run_id = %s", (row[0],))
    conn.execute("INSERT INTO leases ... ON CONFLICT (run_id) DO UPDATE SET ...")
```

### 3.2 Reserving a step — `internal/store/steps.go`

```go
tag, err := tx.Exec(ctx, `
	INSERT INTO steps (run_id, step_index, kind, name, idempotency_key, status, attempt)
	VALUES ($1, $2, $3, $4, $5, 'scheduled', 0)
	ON CONFLICT (idempotency_key) DO NOTHING`,
	runID, stepIndex, string(kind), name, key)
...
return Reservation{Key: key, AlreadyRan: tag.RowsAffected() == 0}, nil
```

- `ON CONFLICT ... DO NOTHING` inserts nothing when the key already exists and
  reports **zero rows affected**.
- `RowsAffected() == 0` therefore means *this step already started before*. The
  worker reads the recorded result rather than running the effect again.

**What this cannot tell you, and it matters:** it says the step was **started**,
not that it **completed**. A worker that died between reserving and journaling
leaves a reservation with no result — the **ambiguous window**. Who resolves it
is decided by the tool's `idempotent` flag:

- idempotent → retry it, repeating is harmless
- not idempotent → `RunNeedsReview`, because nothing can tell you whether the
  effect landed and guessing is worse than stopping

### 3.3 Resumption with no cursor — `internal/worker/interpret.go`

```go
func (e *execution) modelStep(ctx context.Context, nodeID, model string, req provider.Request) (provider.Response, error) {
	stepIndex := len(e.st.Steps)

	if done, resp, ok := e.recordedModelStep(stepIndex); ok {
		if done {
			return resp, nil
		}
	}
	...
}
```

This is the whole resumption mechanism, and it is worth understanding properly
because it looks like nothing.

The interpreter **always walks the graph from the first node**, even on a run
that is half finished. Before doing any work it asks: does the step I am about
to take already exist in the folded state, completed? If yes, the recorded
result is returned and the walk moves on, costing nothing. If no, it executes.

`stepIndex := len(e.st.Steps)` is what makes it work — step indexes are assigned
by position in the walk, so the same graph always produces the same indexes.

**There is no cursor column, no "resume from step 7" bookkeeping.** A worker
taking over a dead worker's run folds the journal, re-walks the graph, skips the
completed prefix for free, and lands exactly on the first unfinished step.

### 3.4 Graceful shutdown — two contexts

```go
func (w *Worker) Run(claimCtx, runCtx context.Context) error {
	g, claimCtx := errgroup.WithContext(claimCtx)
	for i := 0; i < w.cfg.PoolSize; i++ {
		g.Go(func() error {
			for claimCtx.Err() == nil {
				runID, err := w.store.ClaimNext(claimCtx, w.cfg.WorkerID, w.cfg.LeaseFor)
				...
				w.executeRun(runCtx, runID)
			}
			return nil
		})
	}
	return g.Wait()
}
```

- `errgroup.WithContext` starts a group of goroutines; `g.Wait()` blocks until
  all finish and returns the first error. Python: `asyncio.TaskGroup`.
- Go has one loop keyword. `for cond { }` is `while cond:`.
- **The two contexts are the whole trick.** `claimCtx` dies on SIGTERM so
  goroutines stop taking *new* runs. `runCtx` stays alive so work already in
  flight finishes and journals its result. Using one context for both would
  abandon every in-flight step mid-execution on every deploy.
- **Backpressure comes from the pool size**: a goroutine claims only when it is
  free, so a worker never takes more runs than it can execute. No semaphore, no
  queue of claimed-but-unstarted runs.

### 3.5 Budgets are enforced before the money is spent

```go
projected := e.st.TokensUsed + int64(e.w.cfg.MaxTokensPerCall)
if e.st.BudgetTokens > 0 && projected > e.st.BudgetTokens {
	// append BudgetExceeded + RunFailed
	return provider.Response{}, errBudgetExceeded
}
```

The projection is deliberately pessimistic — the per-call ceiling, not an
estimate — because the real completion length is unknown until after the call is
paid for. Budgets are enforced in code, never by asking the model to behave.

---

## 4. New terms

| Term | One sentence |
|---|---|
| Goroutine | A very lightweight thread Go manages; `go f()` starts one. |
| Worker pool | A fixed number of goroutines each processing one unit of work at a time. |
| `SKIP LOCKED` | A Postgres option making a query step over rows other transactions have locked instead of waiting. |
| `FOR UPDATE` | Locks the selected rows until the transaction ends. |
| Lease | Time-limited ownership that must be renewed, so ownership ends automatically if the owner dies. |
| Heartbeat | The periodic renewal that keeps a lease alive. |
| Backpressure | Making producers slow down when consumers cannot keep up, rather than piling up work. |
| High-water mark | The threshold above which the system refuses new work (HTTP 429). |
| Context (Go) | A value threaded through calls carrying cancellation and deadlines. |
| errgroup | A group of goroutines where `Wait` returns the first error. |
| Exponential backoff | Waiting longer after each failed retry: 1s, 2s, 4s. |
| Jitter | Random variation added to backoff so many clients do not retry in lockstep. |
| Retryable vs terminal error | Retryable may succeed if tried again (timeout, 429, 5xx); terminal will not (bad input, 4xx). |
| Cooperative cancellation | Cancelling by signalling, letting the worker stop at a safe point, rather than killing it. |
| Stateless service | Keeps no data between requests, so copies are interchangeable. |
| Interface (Go) | A set of method signatures; any type with those methods satisfies it, with no declaration. |
| Dependency injection | Passing a component its dependencies instead of having it build them, so they can be swapped. |
| Discriminated union | A set of shapes told apart by one literal field. |
| Upsert | `INSERT ... ON CONFLICT DO UPDATE / DO NOTHING`. |

---

## 5. Decisions made here

Full entries in [`../decisions.md`](../decisions.md):

- Each pool goroutine claims its own run, rather than one claimer feeding
  executors through a channel
- Short retry backoffs are waited in place; long ones park the run
- The graph is validated at registration, not at execution
- Prompt templates are substitution only, never `text/template`
- Tool names are returned sorted, never in Go map order
- One schema per test package

---

## 6. Struggles

### A shared `*rand.Rand` is a data race

**Symptom.** `go test -race` failed only once two database-touching test
packages ran at the same time: `WARNING: DATA RACE … math/rand.(*Rand).Int63`.

**Cause.** Every goroutine in the pool shared one `*rand.Rand` — for the step's
recorded seed and for retry jitter. `*rand.Rand` is explicitly **not** safe for
concurrent use. It had been there since the pool was written and single-package
test runs never exposed it.

**Fix.** `math/rand/v2`, whose top-level functions *are* safe for concurrent
use. Determinism is unaffected: what matters is that the seed is journaled, not
which generator produced it.

**What it taught.** The race detector finds bugs that are invisible until load
or scheduling changes. This one would have shipped and then shown up as
occasional corrupted seeds under production concurrency — the hardest category
of bug to reproduce. This is why `-race` is not optional in CI.

### Parallel test packages shared one database

**Symptom.** Tests that passed alone failed together: `run … not found`, and a
foreign-key violation on `runs_agent_fk`.

**Cause.** Go runs each package's test binary **in parallel**. `internal/store`
and `internal/worker` both `TRUNCATE`d the same tables, so each was deleting the
other's rows mid-test.

**Fix.** A schema per test package via `search_path`, in
`internal/testsupport`. Same database, same migrations, separate tables.
Rejected `-p 1` (serialising the packages) because it hides the problem and
makes CI slower rather than making the tests independent.

**What it taught.** "Works alone" and "works in the suite" are different claims.
Shared mutable fixtures are the usual cause, and the fix is isolation, not
serialisation.

### `ON CONFLICT DO NOTHING` says started, not finished

Not a bug, but the thing that took longest to get right. The reservation proves
a step **started**. The gap between "started" and "completed" is the ambiguous
window, and no amount of cleverness in the database closes it — it is closed by
a policy decision (`idempotent: true|false`) about what to do when you cannot
know. Getting this wrong in either direction is a real outage: retry a
non-idempotent effect and you double-charge someone; refuse to retry an
idempotent one and you stall runs that would have been fine.

---

## 7. Checkpoint quiz check

The five M2 questions still match the code. Two notes:

- **Q3 — "What goes into an idempotency key, and why include the input?"** The
  published key is right. Add the omission: the **attempt number is not in it**,
  deliberately, because retries of a step are the same logical effect.
- **Q4 — "What does `RowsAffected() == 0` mean in `ReserveStep`?"** Say the
  limit out loud: it proves the step *started*, not that it *finished*.

**Added question:**

6. Why does each pool goroutine claim its own run, instead of one claimer
   goroutine feeding executors through a channel?
   *Key: a claimed run sitting in a channel holds a lease while nobody is
   working on it, so the lease can expire and the run be claimed twice. Having
   each executor claim its own run means a lease is only ever held by a
   goroutine actually executing it.*
