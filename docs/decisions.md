# Design decisions

Every decision here had a real alternative. This file is the source for
interview answers: each entry states the choice, the alternative, and what the
choice costs. A decision with no cost listed is a decision that was not thought
through.

---

### Event sourcing instead of mutable run state

**Chose:** an append-only `run_events` table as the sole source of truth, with
`runs` and `steps` as projections maintained in the same transaction.

**Alternative:** store run state directly in `runs`/`steps` and mutate it as the
run progresses.

**Why:** three things fall out for free that would otherwise each need building.
Crash recovery becomes "fold the log" rather than "reconstruct what the last
worker was probably doing". Replay becomes possible at all, because every
non-deterministic value the run observed is already recorded. And debugging a
failed run stops being archaeology — the exact prompt sent, the exact response,
and the exact ordering are all still there.

**Cost:** every read of authoritative state is O(events) rather than O(1), so
the projections exist purely to keep list and detail views fast, and they are a
second thing that can be wrong. `anchorctl rebuild -verify` is the guard: it
recomputes the projections from the log and reports any disagreement. Writes are
also larger — a step that would be one UPDATE is three appends.

---

### `UNIQUE (run_id, seq)` instead of a distributed lock

**Chose:** a per-run monotonic integer with a unique constraint as the
concurrency guard.

**Alternative:** advisory locks, a lease-only scheme with no write guard, or an
external lock service.

**Why:** two workers that both believe they own a run fold the same log, compute
the same next seq, and race to insert. Postgres picks the winner; the loser's
transaction aborts with a unique violation and it releases the run. This is
mutual exclusion with no extra infrastructure, and — unlike a lease — it is
correct even when the lease logic is wrong, because the guard is on the write
itself rather than on the intent to write.

**Cost:** it serialises all writes for a run, which forecloses parallel steps
within one run without a redesign (a per-branch sub-sequence, or seq ranges
reserved per branch). It also only protects the log: two workers can still both
*execute* a step, and it is the idempotency key, not this constraint, that stops
the effect happening twice.

**Where it is not enough:** the guard fires at commit time. A worker that has
already executed a side effect and then loses the seq race has performed the
effect without recording it. That window is what the idempotency key and the
journal-before-effect ordering exist to close.

---

### Append-only enforced by a database trigger

**Chose:** a `BEFORE UPDATE OR DELETE` trigger on `run_events` that raises.

**Alternative:** a comment saying "do not update this table", or permissions.

**Why:** "append-only" is the property every other guarantee rests on. If it can
be violated by one careless `UPDATE` in a migration or a debugging session, it
is not a property, it is a habit.

**Cost:** tests cannot clean up with `DELETE`, so they use `TRUNCATE`, which does
not fire row-level triggers. Genuine data repair requires dropping the trigger
deliberately — which is the point.

---

### Timestamps supplied by the application, not `now()`

**Chose:** `Store.Now` is an injectable function; every event's `created_at` is
supplied by Go.

**Alternative:** let Postgres set `created_at DEFAULT now()`.

**Why:** the run's clock has to be replayable. `RunState.Now()` returns the
timestamp of the last applied event, so agent-visible code never reads the wall
clock, and a replay observes exactly the times the original run observed. That
only works if the writer controls the clock. It also makes tests able to pin
time instead of tolerating it.

**Cost:** clock skew between workers is now visible in the log. It is bounded in
practice because only one worker owns a run at a time, and ordering within a run
comes from `seq`, not from the timestamp — but a timestamp going backwards
across a lease takeover is possible and the trace viewer must not assume
otherwise.

---

### Timestamps normalised to UTC on read

**Chose:** `LoadEvents` converts every `created_at` to UTC.

**Why:** `timestamptz` comes back in the session's timezone. The instant is
correct either way, but replay verification compares folded output
byte-for-byte, and a run folded on a laptop in `America/Chicago` would not match
the same run folded in CI. Canonical form is not optional for anything that gets
compared.

---

### Idempotency key = `sha256(run_id || step_index || canonical_json(input))`

**Chose:** a derived key, globally unique in `steps`.

**Alternative:** a random UUID generated when the step is scheduled.

**Why:** derived means two different workers compute the same key for the same
logical step without coordinating. A worker taking over from a crashed one can
ask "did this step already run?" and get a real answer. A random key would have
to be read back from the log first, which is one more thing that can be stale.

Fields are length-prefixed before hashing. Without that, `(run, step 11, input)`
and `(run, step 1, "1" + input)` can produce identical bytes, and a collision
here silently skips a real side effect.

The attempt number is deliberately **not** part of the key: retries of a step are
the same logical effect, and sharing a key is exactly what makes attempt 2 safe
after attempt 1 may have landed.

**Cost:** a step whose input is not stable across a fold — anything carrying a
timestamp or a random value that was not itself journaled — produces a different
key on recovery and would run twice. Everything feeding a step's input must come
from the log.

---

### Declarative agent graphs instead of an executable-code SDK

**Chose:** an agent is versioned JSON in the `agents` table; the Go worker
interprets it. A run pins `agent_version`.

**Alternative:** the Temporal model — users write code, the runtime polices
determinism.

**Why:** replay becomes trivially provable. The node sequence is fixed and
versioned, so a replay walks the identical graph by construction. With arbitrary
user code, determinism depends on the user never calling `time.Now()` or `rand`,
which is an enormous ongoing policing effort. It also removes a cross-language
execution boundary: no Go worker driving a Node process over RPC.

**Cost:** expressiveness. Anchor supports a closed set of node types; anything
outside it cannot be expressed at all. Temporal made the opposite call because it
supports arbitrary business logic — agent loops are a small closed set of shapes,
so the trade is worth it here and would not be there.

Pinning `agent_version` is what makes replaying a month-old run meaningful:
it walks the graph it originally ran, not the graph as it is today.

---

### `BudgetExceeded` is a marker, not a terminal event

**Chose:** `BudgetExceeded` records the breach detail and is immediately
followed by `RunFailed`.

**Alternative:** make `BudgetExceeded` itself terminal.

**Why:** keeping every terminal transition in one small set — `RunCompleted`,
`RunFailed`, `RunCancelled` — means the fold answers "how did this run end" with
no special cases, and the trace viewer has one query rather than several.

**Cost:** two events where one would do, and a fold that has to tolerate a
`BudgetExceeded` with no `RunFailed` after it if a worker dies between the two.

---

### `RunCancelled` is soft-terminal

**Chose:** after `RunCancelled`, the fold still accepts `StepFailed`.

**Why:** cancellation is cooperative — the worker's context is cancelled and the
in-flight model call or container aborts. That abort produces a failure that
still needs somewhere to land. Rejecting it would mean either dropping a real
event or ordering cancellation behind work it is trying to interrupt.

**Cost:** "terminal" now has two meanings in the fold, and the distinction has to
be respected by anything that appends.

---

### Postgres `SKIP LOCKED` instead of Redis or Kafka for the queue

**Chose:** `SELECT ... FOR UPDATE SKIP LOCKED` over a `queue` table.

**Alternative:** Redis list, Kafka, SQS.

**Why:** the run's state and its queue entry can then be changed in one
transaction. A run cannot be enqueued without being journaled, or journaled
without being enqueued. With an external broker that atomicity is gone and has to
be recovered with an outbox and a relay — two more moving parts, for a system
that already requires Postgres.

**Cost:** throughput ceiling. This is one database doing both state and
scheduling, and claim throughput is bounded by write throughput on one primary.
It will not reach the numbers a partitioned log reaches, and past that point the
answer is to shard by tenant, not to tune the query.

---

### `jsonb` rather than `json` for payloads

**Chose:** `jsonb` for `run_events.payload`, `runs.input`, and agent
definitions.

**Why:** indexable and queryable, which the trace viewer needs.

**Cost:** `jsonb` normalises — key order is not preserved and duplicate keys are
dropped. Recorded model responses therefore round-trip semantically, not
byte-for-byte. Replay comparison is unaffected because the original and the
replay pass through the same normalisation, but any future check that hashes a
raw payload must canonicalise first rather than trusting the stored bytes.

---

### Projections updated per touched step, not wholesale

**Chose:** an append upserts only the step rows its events mention.

**Alternative:** rewrite every step row on every append.

**Why:** an `agent_loop` bounded at 8 iterations with a tool call each is ~16
steps and ~50 appends. Rewriting all steps on each append makes the write cost
quadratic in the run's own length — precisely the shape of run Anchor exists to
serve.

**Cost:** the touched set is derived by probing each payload for `step_index`, so
a future event type that changes a step without naming it would silently fail to
project. `rebuild -verify` in CI is what catches that.
