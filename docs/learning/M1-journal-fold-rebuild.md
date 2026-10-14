# M1 — Journal, fold, and rebuild

> Study material for the M1 checkpoint quiz. Read this, then answer the five
> questions out loud without looking. Every code block below is the real code,
> not a teaching simplification.

---

## 1. Plain-English summary

Anchor records what a run did as a list of facts that are never edited. Each
fact is an **event** — "step 3 started", "the model replied with this" — stored
as one row in `run_events`. To know a run's current state, you start from
nothing and apply every event in order; that computation is called a **fold**
(Python's `functools.reduce`).

Two tables, `runs` and `steps`, hold a copy of that computed state so the UI can
query it quickly. They are **projections**: caches. The `anchorctl rebuild`
command deletes them and recomputes them from `run_events` alone, which proves
the log really is the only thing that matters.

Each event carries a per-run counter, `seq`. The database enforces that no two
rows share a `(run_id, seq)` pair, so if two workers both think they own a run
and both try to write event 7, one succeeds and the other is rejected.

*(148 words)*

---

## 2. Data flow: one run submission, end to end

Following `anchorctl submit -agent research` through the code:

1. **`cmd/anchorctl/main.go` → `cmdSubmit`** parses flags, then calls
   `store.LatestAgentVersion` to pin the agent version this run will use. A run
   pins its version so a replay later walks the graph it originally ran.

2. **`internal/store/runs.go` → `Store.SubmitRun`** validates the input is real
   JSON and builds an empty `journal.RunState` carrying a fresh `uuid.New()`.

3. It opens a transaction (`pgx.BeginFunc`) and calls **`Store.AppendTx`**
   (`internal/store/events.go`) with one pending event: `RunSubmitted`.

4. **`AppendTx`** does four things in order:
   - `journal.MarshalPayload` — encodes the payload and refuses it if `v` is
     unset, so no unversioned event can ever enter the log.
   - `st.Clone()` then `next.Apply(event)` — folds the candidate into a *copy*.
     If the event would corrupt the run, that is discovered here, before any SQL.
   - `upsertRun` (`internal/store/projection.go`) — writes the `runs` projection.
   - `INSERT INTO run_events` at `seq = next.NextAppendSeq()`, which is 1 for a
     new run. A unique-violation on `run_events_run_seq_unique` is translated to
     `ErrSeqConflict`.

5. Back in `SubmitRun`, still inside the same transaction, a row goes into
   `queue`. Journal and queue commit together: a run can never be in one and not
   the other.

6. On commit, `SubmitRun` returns the advanced state. `st` is only overwritten
   after the transaction succeeds, so a caller that gets an error still holds
   exactly the state it had.

Reading it back, **`Store.LoadState`** calls `LoadEvents` (ordered by `seq`,
timestamps normalised to UTC) and hands them to **`journal.Fold`**. Nothing
reads run state from the projections — a stale cache can never influence
execution.

---

## 3. Annotated code

### 3.1 The fold's envelope check — `internal/journal/fold.go`

This runs before every event, regardless of type. It is where "the log is
trustworthy" is actually enforced.

```go
func (s *RunState) checkEnvelope(e Event) error {
	expected := s.NextAppendSeq()
	fresh := s.NextSeq == 0

	if fresh {
		if e.Type != TypeRunSubmitted {
			return fmt.Errorf("first event must be %s, got %s", TypeRunSubmitted, e.Type)
		}
	} else {
		if e.Type == TypeRunSubmitted {
			return fmt.Errorf("seq %d: %s may only appear at seq %d", e.Seq, TypeRunSubmitted, FirstSeq)
		}
		if e.RunID != s.RunID {
			return fmt.Errorf("seq %d: event belongs to run %s, folding run %s", e.Seq, e.RunID, s.RunID)
		}
	}

	if e.Seq != expected {
		return fmt.Errorf("out-of-order event: expected seq %d, got %d (%s)", expected, e.Seq, e.Type)
	}
	...
}
```

- `expected := s.NextAppendSeq()` — computed into a **local**, not assigned to
  `s`. A rejected event must not leave the state half-advanced.
- `fresh := s.NextSeq == 0` — because `seq` is 1-based, zero unambiguously means
  "nothing folded yet". No extra boolean field is needed.
- The run-id check exists because a fold over the wrong run's events would
  otherwise produce a confident, wrong answer.
- `e.Seq != expected` rejects gaps **and** repeats with one comparison.

**Python equivalent:**

```python
def check_envelope(state, e):
    expected = state.next_seq or FIRST_SEQ
    if state.next_seq == 0:
        if e.type != "RunSubmitted":
            raise ValueError(f"first event must be RunSubmitted, got {e.type}")
    else:
        if e.run_id != state.run_id:
            raise ValueError("event belongs to a different run")
    if e.seq != expected:
        raise ValueError(f"expected seq {expected}, got {e.seq}")
```

**Why the fold must be pure:** it takes events and returns state, with no clock,
no network, no randomness inside. Recovery and replay both re-run it and must
get an identical result. A single `time.Now()` in here would make a resumed run
differ from the original, and replay fidelity would drop for reasons no one
could trace.

### 3.2 The concurrency guard — `internal/store/events.go`

```go
_, err := tx.Exec(ctx, `
	INSERT INTO run_events (run_id, seq, type, payload, created_at)
	VALUES ($1, $2, $3, $4, $5)`,
	e.RunID, e.Seq, string(e.Type), []byte(e.Payload), e.CreatedAt)
if err != nil {
	if isUniqueViolation(err, "run_events_run_seq_unique") {
		return nil, ErrSeqConflict
	}
	return nil, fmt.Errorf("insert run_event seq %d: %w", e.Seq, err)
}
```

- `$1, $2...` are query parameters. Values travel separately from the SQL text,
  which is what prevents SQL injection.
- `isUniqueViolation` wraps `errors.As(err, &pgErr) && pgErr.Code == "23505"`.
  `23505` is Postgres's unique-violation code. Python:
  `except psycopg.errors.UniqueViolation`.
- Returning a **named sentinel** rather than the raw error is what lets the
  worker write `if errors.Is(err, store.ErrSeqConflict) { release(); return }`.

**This is the whole of Anchor's mutual exclusion.** Two workers fold the same
log, both compute `seq = 7`, both insert. Postgres accepts one. The loser's
transaction aborts, it gets `ErrSeqConflict`, and it releases the run.

**Where it is not enough:** the guard fires at *write* time. A worker that
already performed a side effect and *then* loses the race has done the effect
without recording it. That gap is the ambiguous window, and it is closed by the
idempotency key, not by this constraint.

### 3.3 The idempotency key — `internal/idem/idem.go`

```go
func Key(runID uuid.UUID, stepIndex int, input json.RawMessage) (string, error) {
	canon, err := CanonicalJSON(input)
	if err != nil {
		return "", fmt.Errorf("canonicalise step input: %w", err)
	}
	h := sha256.New()
	writeField(h, runID[:])
	writeField(h, []byte(strconv.Itoa(stepIndex)))
	writeField(h, canon)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeField(h hash.Hash, b []byte) {
	_, _ = h.Write([]byte(strconv.Itoa(len(b))))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write(b)
}
```

- `runID[:]` turns the 16-byte UUID array into a slice so it can be hashed.
- **Length-prefixing each field** is not decoration. Without it, run `a` + step
  `11` and run `a` + step `1` with an input starting `1` hash the same bytes. A
  collision here means a real side effect is silently skipped.
- `CanonicalJSON` decodes with `UseNumber()` (so `12345678901234567890` is not
  rounded through `float64`) and re-encodes; Go's encoder sorts map keys, which
  is what makes the output canonical. Python:
  `json.dumps(x, sort_keys=True, separators=(",", ":"))`.

**What is deliberately absent: the attempt number.** Retries of a step are the
same logical effect. Sharing a key is exactly what makes attempt 2 safe after
attempt 1 may have landed.

**The cost:** any input that is not stable across a fold — anything carrying a
timestamp or a random value that was not itself journaled — produces a different
key on recovery, and the step runs twice. Everything feeding a step's input has
to come from the log.

### 3.4 The rebuild — `internal/store/rebuild.go`

```go
if _, err := tx.Exec(ctx, `DELETE FROM steps WHERE run_id = $1`, runID); err != nil {
	return fmt.Errorf("clear steps: %w", err)
}
if _, err := tx.Exec(ctx, `DELETE FROM runs WHERE id = $1`, runID); err != nil {
	return fmt.Errorf("clear run: %w", err)
}
if err := upsertRun(ctx, tx, st); err != nil {
	return err
}
return upsertSteps(ctx, tx, st, all)
```

- Both rows are **deleted**, not upserted over. An upsert would hide the exact
  failure this command exists to detect: a column the fold stopped writing would
  keep its stale value forever.
- Deleting the `runs` row is only possible because `run_events` has no foreign
  key to it, and because `steps` references it through a `DEFERRABLE INITIALLY
  DEFERRED` constraint — so the row may be absent mid-transaction and must be
  back by `COMMIT`.
- `rebuild -verify` reports differences instead of repairing them. CI runs it
  after the whole suite, so projection drift is a build failure.

---

## 4. New terms

| Term | One sentence |
|---|---|
| Event | An immutable fact about something that happened, stored as a `run_events` row. |
| Event sourcing | Storing every change as an append-only list and computing current state from it. |
| Fold | Applying a function to each item in order, carrying state forward; Python's `functools.reduce`. |
| Pure function | Output depends only on inputs — no clock, network, or randomness. |
| Projection | A read-optimised table derived from events that can be rebuilt from them. |
| Source of truth | The one place whose data wins when copies disagree. |
| Optimistic concurrency | Assume no conflict, detect one at write time, back off. |
| Idempotent | Doing it twice has the same result as doing it once. |
| Idempotency key | A unique id for an operation so repeats can be detected and skipped. |
| Ambiguous window | The moment between starting a side effect and recording it, where a crash leaves the outcome unknown. |
| Canonical JSON | One deterministic encoding of a value — keys sorted, whitespace removed — so equal values hash equally. |
| Migration | A versioned schema change applied in order. |
| Upsert | `INSERT ... ON CONFLICT DO UPDATE / DO NOTHING`. |
| Constraint | A rule the database enforces, such as `UNIQUE` or `NOT NULL`. |
| Transaction | A group of database operations that succeed or fail together. |
| Deferrable constraint | A foreign key checked at `COMMIT` rather than at each statement. |
| Trigger | A function the database runs automatically on insert, update or delete. |

---

## 5. Decisions made here

Full entries with alternatives and costs are in [`../decisions.md`](../decisions.md).
The ones specific to M1:

- Event sourcing over mutable rows
- `UNIQUE (run_id, seq)` over a distributed lock
- Append-only enforced by trigger, not convention
- Application-supplied timestamps, truncated to microseconds
- Derived idempotency keys, not random ones
- `BudgetExceeded` as a marker, `RunCancelled` as soft-terminal
- `RunNeedsReview` as its own terminal event
- `run_events` holds no foreign key to `runs`
- Projections updated per touched step, not wholesale

---

## 6. Struggles

These are the "tell me about a hard bug" answers. Each is real.

### Timestamp precision destroyed state equality

**Symptom.** Everything passed on macOS. CI on Linux failed:
`TestLoadStateMatchesInMemoryState` showed the writer holding
`...308907062Z` and the log returning `...308907Z`.

**Cause.** Postgres `timestamptz` stores **microseconds**; Go's `time.Now()`
carries **nanoseconds**. The state a worker holds in memory was not equal to the
same run folded back out of the log — same instant, different value.

**How it was found.** Only because CI runs on Linux. macOS clock granularity
usually leaves the nanosecond digits at zero, so the bug was invisible locally.

**What it taught.** Anything compared byte-for-byte has to be normalised **where
it is created**, not where it is compared. Events are now stamped through
`pgTimestamp()` at the single point they are made. Left unfixed, this would have
surfaced at M6 as replay fidelity sitting near zero with no obvious cause.

### `Clone` turned nil into empty

**Symptom.** The regression test for the bug above immediately failed again:
writer `"steps":[]`, log `"steps":null`.

**Cause.** A run with no steps folds to a nil `Steps` slice, but `Clone` always
allocated. Equal in meaning, unequal on the wire.

**What it taught.** Same lesson as above, which is why it is worth remembering
as one: representation drift between live and recovered state is a class of bug,
not an incident.

### You cannot renumber an append-only log

**Symptom.** Migration `0002` failed moving `seq` to 1-based:
`check constraint "run_events_seq_positive" is violated by some row`.

**Cause.** Existing rows were 0-based, and there is no way to fix them — the
append-only trigger rejects `UPDATE` **by design**.

**What it taught.** Immutability has a real cost and this is it: a schema
decision baked into existing events cannot be undone later. The migration now
detects the condition and explains it rather than dying on a bare constraint
violation. With production data the answer would have been to make the fold
accept both bases forever.

### The foreign key pointed the wrong way

**Symptom.** Found while auditing against the new spec, not by a test:
`rebuild` could not satisfy "drop `runs` and `steps` and reproduce them".

**Cause.** `run_events` had a foreign key to `runs`. The source of truth
depended on its own cache, so the cache could never be dropped.

**What it taught.** Direction of dependency is a design property worth checking
explicitly. The question "what is derived from what?" should be answerable by
looking at the constraints, and here the constraints said the opposite of the
architecture diagram. `queue` and `leases` had the same inversion.

### Two CI failures that were process, not code

`go.mod` and `go.sum` were never committed after pgx was added — every commit
staged explicit paths and dropped them. And the lint job installed
golangci-lint **v1**, whose binary is built with an older Go and refuses to lint
a module targeting a newer one. Both are now prevented: CI reads its Go version
from `go.mod`, a `go mod tidy && git diff --exit-code` step fails loudly, the
linter version is pinned, and `make ci` runs the whole sequence locally.

---

## 7. Checkpoint quiz check

The five M1 questions in the Quizzes tab still match the code. Two answers need
updating against what was actually built, and three questions are added for
material that did not exist when the quiz was written.

**Amended answers:**

- **Q2 — "What does `UNIQUE (run_id, seq)` protect against, concretely?"** The
  key is in the published answer, but a complete answer adds *where it does not
  protect*: the guard fires at write time, so a worker that performed a side
  effect before losing the race has already done it. That is the idempotency
  key's job.
- **Q4 — "What does the `rebuild` command prove?"** The answer should now be
  "that `runs` and `steps` can be **deleted entirely** and recomputed" — not
  merely corrected. Overwriting proves less, because a column the fold stopped
  writing would keep a stale value.

**Added questions:**

6. Why does `run_events` have no foreign key to `runs`?
   *Key: `runs` is a cache derived from the log; a source of truth that
   references its own cache cannot exist without it, and the cache can never be
   dropped and rebuilt.*
7. Why can't a migration renumber `seq` on existing events?
   *Key: the table is append-only and the trigger rejects `UPDATE` by design.
   Immutability means schema decisions baked into existing events are permanent;
   the options are to reset (no production data) or make the fold accept both.*
8. Why is `RunNeedsReview` its own event type rather than a flag on `RunFailed`?
   *Key: a failed run is finished and understood; a run needing review is
   finished and not understood — nobody knows whether its last effect landed.
   Alerting and the viewer must distinguish them without parsing a payload.*
