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

### Event timestamps truncated to microseconds

**Chose:** `AppendTx` stamps every event with `t.UTC().Truncate(time.Microsecond)`.

**Why:** Postgres `timestamptz` stores microseconds; Go's `time.Now()` carries
nanoseconds. Without truncation the state a worker holds in memory differs from
the same run folded back out of the log by a few hundred nanoseconds. The values
are the same instant but not the same value, so any equality check between live
state and recovered state fails, and the fold stops being the single source of
truth in practice while still looking like it in principle.

This was found by CI rather than by local testing: macOS clock granularity
usually leaves the nanosecond digits at zero, so the bug was invisible on a
laptop and immediate on Linux. Anything compared byte-for-byte has to be
normalised at the point it is created, not at the point it is compared.

**Cost:** sub-microsecond ordering is not representable. It does not matter,
because ordering within a run comes from `seq`, never from the timestamp.

**Related:** `RunState.Clone` preserves the difference between a nil and an
empty `Steps` slice for the same reason — a run with no steps folds to nil, and
a clone that normalised it to `[]` produced state that was equal in meaning and
unequal on the wire.

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

### `run_events` holds no foreign key to `runs`

**Chose:** the log references nothing. `queue` and `leases` also dropped their
foreign keys to `runs`.

**Alternative:** the obvious one — `run_events.run_id REFERENCES runs(id)`,
which is what was built first.

**Why:** direction of dependency has to match the architecture. `runs` is a
cache computed from `run_events`; a source of truth that references its own
cache cannot exist without it, and more concretely the cache can then never be
dropped and recomputed — which made the M1 acceptance criterion ("drop `runs`
and `steps`, rebuild reproduces them") impossible to satisfy. `queue` and
`leases` had the same inversion: they are keyed by run id, but that id is the
log's identity, not the projection row's.

**Cost:** the database no longer rejects an event written for a run id that was
never submitted. That check moves into the fold, which refuses any log whose
first event is not `RunSubmitted` — a stronger check, but one that now has to be
*run* rather than being structurally impossible to violate.

**Kept:** `steps` still references `runs`. Both are projections rebuilt in the
same transaction, so the constraint is meaningful — but as `DEFERRABLE INITIALLY
DEFERRED`, so the delete-then-reinsert is legal mid-transaction and still
checked at `COMMIT`.

---

### Sequence numbers are 1-based

**Chose:** the first event of a run is `seq` 1. `journal.FirstSeq` names it and
`RunState.NextAppendSeq()` is the only way writers obtain the next value.

**Alternative:** 0-based, which was built first.

**Why:** the build spec specifies it, and it makes zero an unambiguous "not
set". A fresh `RunState` can report whether it has folded anything without
carrying a separate boolean, and a `seq` of 0 arriving from anywhere is
detectably wrong rather than being a legitimate first event.

**Cost:** it cannot be changed again. Existing events cannot be renumbered —
`run_events` is append-only and the trigger rejects `UPDATE` by design — so the
migration can only detect legacy rows and refuse. This was free here because
Anchor has no production data; with real data the only option would have been to
teach the fold both bases permanently.

---

### `RunNeedsReview` is its own terminal event

**Chose:** a distinct event type, with a required `Reason`.

**Alternative:** a `needs_review` boolean on `RunFailed`, which was built first.

**Why:** the two states mean different things operationally. A failed run is
finished and understood. A run needing review is finished and *not* understood —
nobody knows whether its last side effect landed. Alerting, the run list, and
anything paging a human must tell them apart without parsing a payload, and a
boolean buried in JSON is the wrong place for a distinction that changes who
gets woken up.

**Cost:** one more event type in the fold's switch and in every exhaustive
handler, forever. Event types are the one part of an event-sourced system that
can never be removed.

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

### Each pool goroutine claims its own run

**Chose:** every goroutine in the worker pool runs its own claim -> execute ->
journal -> release loop.

**Alternative:** one claimer goroutine feeding executors through a buffered
channel, which is the more obvious Go shape.

**Why:** a claimed run sitting in a channel holds a lease while nobody is
working on it. If the buffer is deep or the executors are busy, that lease can
expire before execution starts, another worker claims the run, and now two
workers hold it -- the exact failure leases exist to prevent. Claiming inside
the executor means a lease is only ever held by a goroutine actually executing.

It also gives backpressure for free: a goroutine claims only when it is free, so
a worker can never take on more runs than it can execute.

**Cost:** N goroutines each run a claim query, so an idle pool polls N times per
interval instead of once. At large pool sizes that is wasted database work, and
the fix would be a shared claimer with a *tiny* buffer and the heartbeat started
at claim time rather than at execution time.

---

### Server Components fetch; client components stay small

**Chose:** pages are Server Components that await their data; only the live
timeline, the step drawer and the replay panel are Client Components.

**Alternative:** a single-page app fetching everything from the browser.

**Why:** the API key is read in `process.env` on the server and never ships, so
it cannot be read out of the page source. The HTML also arrives complete -- no
spinner, no HTML-then-JavaScript-then-fetch round trip -- and only the
interactive parts cost bundle size (the run detail page ships ~3.4 kB of its
own).

**Cost:** two mental models in one codebase, and the boundary has to be right.
A `process.env` read that drifts into a client component leaks a secret, and
nothing about the syntax warns you.

---

### The viewer proxies SSE rather than giving the browser a key

**Chose:** a Next Route Handler fetches `anchor-server`'s stream and pipes the
body through.

**Alternative:** let the browser open an `EventSource` directly against
`anchor-server`.

**Why:** `EventSource` cannot send an `Authorization` header -- it is a
deliberately minimal API. The key would have to go in the query string, which
puts it in server logs, browser history and any referrer. The proxy already has
it server-side.

**Cost:** an extra hop, and the Next server now holds a connection per watching
browser. The body is piped unbuffered, so it adds latency but no delay.

---

### Server-Sent Events, not WebSockets

**Chose:** SSE.

**Why:** the viewer needs one-way server-to-browser updates, which is exactly
what SSE is. It runs over plain HTTP, reconnects on its own, and resumes from
`Last-Event-ID` with no application code. A WebSocket would be a second
transport to operate for no capability the viewer uses.

**Cost:** no browser-to-server channel on that connection, and some proxies
buffer streams unless told not to (hence `X-Accel-Buffering: no`). If the viewer
ever needs to push to the server mid-stream, this is the thing that changes.

---

### The SDK validates graphs as well as the server

**Chose:** `defineAgent` checks the same rules the Go server checks.

**Why:** a mistake caught in the editor costs nothing; the same mistake caught
after submission has already cost a round trip. The server remains the authority
-- a client is never trusted -- but a client that catches its own errors is a
better client.

**Cost:** two implementations of the same rules, which can drift. The server's
version is the one that matters, so drift shows up as the SDK being wrong rather
than as a security hole.

---

### `defineTool` requires `idempotent` to be declared

**Chose:** no default. Omitting it throws.

**Why:** neither default is safe. `true` silently permits retrying an effect
that must not repeat -- a double charge. `false` halts runs that were fine as
`needs_review`, waiting for a human who did not need to be involved. The author
has to decide, because that decision is what Anchor acts on when a worker dies
mid-effect.

**Cost:** more friction writing a tool. Anchor's own chaos tool got this wrong
-- declaring `true` without being idempotent -- and produced an intermittent
duplicate effect, so the friction is buying something real.

---

### The viewer computes its own replay diff

**Chose:** the diff Route Handler reads both event logs and compares them.

**Alternative:** have `anchor-server` store a verdict when a replay finishes and
display that.

**Why:** the page then shows what the two journals actually say, rather than a
cached opinion that could be stale or wrong. It also mirrors the Go
`replay.Compare` rules exactly -- type, order, step, attempt; never timestamps
-- so the two agree by construction.

**Cost:** the comparison logic exists twice, in Go and TypeScript, and both must
stay in step. They are each about thirty lines and tested separately.

---

### Replay is the same code path with swapped dependencies

**Chose:** a replay is a normal run with `replay_of` set, executed by the same
interpreter, holding sources that answer from the original's journal.

**Alternative:** a dedicated replay engine that walks the recorded events.

**Why:** if replay were its own code path, anything that only happened on the
live path would be untested by replay, and anything replay did differently would
be a divergence the real run never had. The thing being verified has to be the
thing that actually runs. It also means replay inherits crash recovery, leases
and idempotency for free rather than reimplementing them.

**Cost:** the interpreter now carries an override for its provider and runner,
and every future dependency it gains must be swappable too or replay silently
reaches the real one.

---

### Recordings keep per-attempt outcomes

**Chose:** `RecordedStep.Attempts` maps attempt number to outcome, including
failures.

**Alternative:** record each step's final result, which is what the folded state
holds.

**Why:** a step that failed once and then succeeded produces six events. A
replay handed only the success produces three, so it diverges -- and **every run
that ever retried would score as a fidelity failure** for no real reason. The
recording is therefore built by walking raw events, because the fold
deliberately collapses attempt history.

**Cost:** the recording is larger, and it is a second reader of the event log
that must be kept in step with the fold. Two consumers needing different views
of the same log is the real lesson.

---

### The event signature excludes timestamps and ids

**Chose:** compare event type, order, step index and attempt. Nothing else.

**Why:** a replay happens later by definition, so comparing timestamps would
make every replay diverge at its first event and the check would be worthless.
Database ids are sequence numbers with no meaning across runs. The model's
response bytes are *handed* to the replay, so comparing them would confirm the
recording was read correctly -- testing the test rather than the system.

**Kept:** terminal output and status, because those are **computed** by the
interpreter rather than handed over. Identical events producing a different final
answer means something outside the journal reached the result.

**Cost:** a divergence that changes only a payload field the signature ignores
goes undetected. The signature is the definition of "the same execution", and
widening it later would reclassify runs that currently pass.

---

### Only the first divergence is reported

**Chose:** the replay sources keep the first mismatch and ignore later ones.

**Why:** once a replay has diverged, everything after is downstream of that one
difference. A run that turns wrong at step 2 differs at every later step, and
listing all of it buries the cause under its consequences.

**Cost:** two independent bugs in one run show up as one, and the second is only
visible after the first is fixed.

---

### Errored replays leave the fidelity denominator

**Chose:** fidelity is matched / (matched + diverged). Errors are counted and
reported separately.

**Why:** a replay that never ran -- no worker, a timeout, a missing agent
version -- says nothing about determinism. Counting it as a failure blames the
system for a harness problem; counting it as a success inflates the number.

**Cost:** a harness that errors on exactly the hard runs would report a
flattering figure. Errors are printed alongside the percentage so the exclusion
is visible rather than silent.

---

### A replay pins the original's agent version

**Chose:** `SubmitReplay` copies `agent_version` from the original.

**Why:** replaying last month's run against today's graph compares two different
programs. Every difference would be reported as a determinism failure when it is
really a deployment.

**Cost:** old agent versions can never be deleted, which is already true because
`runs` has a foreign key to `agents`.

---

### Replays skip rate limits and the circuit breaker

**Chose:** the guard check is bypassed when a replay source is present.

**Why:** a replay reaches no provider and no network, so neither applies. More
importantly, consulting them would make a replay's event sequence depend on
fleet state at replay time -- the exact kind of outside influence replay exists
to rule out. A fidelity sweep during a provider outage would otherwise report
mass divergence.

**Cost:** one more branch where live and replay differ, and it has to stay
correct. It is the only such branch.

---

### Money is counted in micro-dollars

**Chose:** `budget_micros` / `micros_used`, one millionth of a dollar.

**Alternative:** integer cents, which is what the schema originally had.

**Why:** a real call -- 36 input and 143 output tokens on Opus 5.5 -- costs
about $0.003. In integer cents that is **zero**. A cost budget counted in cents
therefore rounds almost every run to nothing and can never trigger, which is
worse than having no budget at all: the operator believes they have one.

Floating point is never used for money; this is integer arithmetic throughout.
Prices are stored per *million* tokens so that models costing a fraction of a
micro-dollar per token stay exact integers.

**Cost:** a schema migration and a payload version bump. Cheap because both
columns held only zeros; expensive the moment there is real data, which is the
argument for doing it before anything depends on it. Integer division truncates,
so a figure can be up to one micro-dollar below true cost per call -- a
millionth of a dollar, preferable to float rounding across thousands of calls.

---

### Payload v2, with v1 still readable

**Chose:** bump `CurrentPayloadVersion` to 2 and give the payloads accessor
methods that convert v1's cent fields.

**Alternative:** rewrite the old events, or keep the field name and change what
it means.

**Why:** rewriting is impossible -- `run_events` is append-only and the trigger
rejects `UPDATE`. Reusing the name with a new meaning would make every old run
fold to a figure 10,000x wrong, silently. The version field exists for exactly
this, and this is the first time it has been used.

**Cost:** every money-carrying payload now has two fields and an accessor, and
both must be kept working forever. Event schemas are the one part of an
event-sourced system that can never be deleted.

---

### An unpriced model is an error, not a zero

**Chose:** `pricing.Cost` returns `ErrUnknownModel`. A run with a spending
budget against an unpriced model is refused outright; without a budget it runs
but records `cost_unknown`.

**Alternative:** charge zero and carry on.

**Why:** charging zero means the budget silently passes over that model. The
operator believes a cap is in force while nothing counts against it -- the same
failure the cents bug caused, re-created in a different place.

**Cost:** adding a model means editing the table, and forgetting to breaks runs
that set a spending budget. That friction is deliberate: the alternative fails
silently, and this fails loudly.

---

### Rate limits fail closed; the circuit breaker fails open

**Chose:** when Redis is unreachable, the token bucket denies and the breaker
allows.

**Why:** they are different kinds of thing. A rate limit is a **promise** to
someone else's service -- flooding a provider during a Redis outage breaks it,
so it fails closed. A breaker is an **optimisation** for us: it saves money on
calls likely to fail, and a Redis outage must not halt all work, so it fails
open. Budgets follow the limiter.

**Cost:** a Redis outage stops all model calls, turning a cache dependency into
a hard one for throughput. The alternative is overrunning a provider's limits
during exactly the incident when that hurts most.

---

### A refused call parks the run rather than sleeping

**Chose:** rate-limited and breaker-refused runs are requeued with a future
`available_at`.

**Alternative:** sleep in the worker until the bucket refills.

**Why:** a sleeping goroutine holds a worker slot **and a lease** while doing
nothing. The slot is unavailable to work that could proceed, and the lease can
expire mid-sleep, letting another worker claim a run this one still believes it
owns.

**Cost:** a requeue is a database round trip, which for a very short wait costs
more than the wait itself. Retries keep the in-place path for backoffs under a
second.

---

### The breaker is consulted before the rate limit

**Chose:** `Guards.Check` asks the breaker first.

**Why:** a call refused by an open breaker should not also consume a rate-limit
token. Spending a token on a call that will not be made throttles the tenant for
no reason, and during a provider outage would exhaust every tenant's bucket
without a single call being attempted.

---

### Fixed-cardinality metric series are pre-created

**Chose:** `initFixedLabels` touches every known label combination at startup.

**Why:** a Prometheus `*Vec` exposes nothing until a combination is first
observed, so a "failed runs" panel reads *no data* rather than 0 -- which looks
identical to a broken scrape, at exactly the moment someone is trying to tell
them apart.

**Cost:** only works for labels whose values are known in advance. Model and
provider names are open sets, so those series still appear on first use.

---

### Every worker runs the reaper

**Chose:** the lease reaper is a goroutine inside every worker process.

**Alternative:** one designated reaper process, or a cron job.

**Why:** a single reaper is one more process whose death stops recovery — and
recovery is precisely the thing that has to keep working when processes die.
Putting it in every worker means the fleet heals itself as long as any worker is
alive.

**Cost:** N workers each run the reap query on their own interval, so the
database sees N times the reap traffic. The query is cheap and indexed on
`expires_at`, but at large fleet sizes the interval has to grow or the reaper has
to move behind an advisory lock.

---

### Docker containers for tool sandboxing

**Chose:** one short-lived Docker container per tool call.

**Alternative:** gVisor or Firecracker microVMs, WebAssembly, or running tools
in-process.

**Why:** Docker is everywhere, well understood, and good enough for the actual
threat — a buggy tool, or a tool the model misuses. In-process is not a sandbox
at all; a tool could read the worker's memory, including other tenants' data.

**Cost:** containers share the host kernel, so this does **not** stop a
determined attacker with a container-escape exploit. Stronger isolation means
microVMs or separate hosts. It also adds container startup latency to every tool
call, which is real but small next to a model call.

Stating the limit is the point. "It sandboxes tools" invites the follow-up "so
it is safe to run hostile code?", and the honest answer is no.

---

### Default-deny networking, behind two gates

**Chose:** `--network none` unless the tool declares `egress: true` **and** the
deployment sets `AllowEgress`.

**Alternative:** allow network by default and let tools opt out, or trust the
tool's declaration alone.

**Why:** a tool that forgets to declare egress should fail loudly, not silently
gain access to everything the worker can reach — including the database and any
cloud metadata endpoint. The second gate exists because tool definitions are not
all written by the operator: a deployment needs to be able to refuse network
access fleet-wide regardless of what individual tools ask for.

**Cost:** a legitimate tool that needs the network fails until someone changes
two settings. That friction is the feature.

---

### Schema validation wraps the Runner

**Chose:** `ValidatingRunner` wraps whichever runner is in use.

**Alternative:** validate inside each Runner, or in the interpreter.

**Why:** the Docker sandbox and the mock then behave identically, and the rule
is written once. Putting it in the interpreter would mean the rule only applies
where the interpreter remembers to apply it.

**Cost:** one more layer between the worker and the tool. A tool with no
declared schema still validates trivially — a deliberate escape hatch, and the
cost is that such a tool's output is trusted.

---

### The official Anthropic SDK, not hand-rolled HTTP

**Chose:** `github.com/anthropics/anthropic-sdk-go`.

**Alternative:** `net/http` directly, which is what the build spec's teaching
snippet shows.

**Why:** the wire format changes, and an SDK tracks it. It also brings typed
errors, which is what makes retryable/terminal classification a type check
rather than string matching.

**Cost:** a substantial dependency, and the typed response has to be unwrapped
to get the raw body back for the journal (`msg.RawJSON()`). The raw body is
non-negotiable: a replay returns it in place of calling the provider, so
anything the adapter drops is lost to every future replay of that run.

---

### The real-API check is a script, not a test

**Chose:** `scripts/live-check.sh`, run deliberately.

**Alternative:** a test gated on `ANTHROPIC_API_KEY`.

**Why:** it spends real money and needs a credential. CI has to be runnable by
anyone on any branch without either, and a test that silently skips in CI
reports a green build for something never verified.

**Cost:** the real API is not exercised automatically, so a wire-format change
would be caught at the next manual run rather than by the next push. The local
wire-format tests cover everything Anchor owns; what the script adds is
confirmation that the real API agrees.

---

### Chaos tests spawn real processes and send real SIGKILL

**Chose:** the chaos suite builds the worker binary, runs it as subprocesses,
and kills them with `SIGKILL`. The effect it counts is a committed database row.

**Alternative:** simulate a crash in-process by cancelling a context or dropping
a lease.

**Why:** an in-process "crash" leaves the Go runtime intact. Deferred functions
still run, the lease still gets released, state still gets flushed — all the
cleanup a `SIGKILL`ed process never gets. Simulating the failure would test a
gentler failure than the one being claimed. The effect is a database row for the
same reason: an in-memory counter dies with the process holding it, so it could
never measure what happened across a kill.

**Cost:** the suite is slow (about 15 seconds), needs a built binary, and cannot
run under `-short`. It also cannot run where spawning processes is restricted.
The alternative is faster and proves less.

---

### Short retry backoffs wait in place; long ones park the run

**Chose:** a backoff of up to one second is slept through inside the worker;
anything longer requeues the run with a future `available_at`.

**Alternative:** always sleep, or always requeue.

**Why:** a sleeping goroutine holds a worker slot and a lease while doing
nothing. For a 200ms backoff the bookkeeping of requeueing costs more than the
wait. For a 30-second one it is the difference between a worker slot being
usable and being idle.

**Cost:** two code paths for one concept, and a threshold that is a guess until
there are metrics to tune it against.

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

### One Postgres schema per test package

**Chose:** each test package gets its own schema via `search_path`, created and
migrated by `internal/testsupport`.

**Alternative:** `go test -p 1` to run package test binaries one at a time.

**Why:** Go runs package test binaries in parallel, and two packages truncating
the same tables delete each other's rows mid-test. Serialising hides that rather
than fixing it, makes the suite slower as packages are added, and leaves the
tests genuinely coupled -- the next shared fixture would break them again.
Separate schemas make them independent, and concurrency within a package is
still exercised for real.

**Cost:** a `Schema` option on `store.Config` that exists mostly for tests,
though schema-per-tenant is a real deployment shape it would also serve.

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
