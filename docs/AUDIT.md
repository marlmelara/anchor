# Audit: repo vs. build spec

Run when the build spec changed on 2026-10-02 and M1 was already built. Per the
spec's instruction, working code was audited rather than rewritten: the snippets
in the walkthroughs teach ideas, the real code is the source of truth.

---

## M1 — Journal, fold, and rebuild

**Acceptance criteria, as written in the spec:**

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | Appending a duplicate `seq` fails | **pass** | `TestConcurrentAppendLosesOnSeqConflict` — two states folded from the same log, both append, second gets `ErrSeqConflict` and its state does not advance |
| 2 | Dropping `runs` and `steps` and running `rebuild` reproduces them exactly | **was failing → now passes** | `TestRebuildReproducesProjectionsAfterDroppingThem` — see gap 1 |
| 3 | Fold tests cover every event type | **pass, now enforced** | `TestEveryEventTypeHasFoldCoverage` fails if a type is added without a fold test |

### Gaps found and closed

**1. `rebuild` could not actually drop the projections.** `run_events` held a
foreign key to `runs`, and `queue`/`leases` did too. So the runs row could never
be deleted while its own log existed — criterion 2 was unsatisfiable, and the
old test corrupted the projection instead of dropping it, which is a weaker
claim.

The foreign key was backwards. `runs` is a cache derived from `run_events`;
making the source of truth depend on its own cache means the log cannot exist
without it. Same for `queue` and `leases`: they are keyed by run id, but that id
is the log's identity, not the projection row's.

Fixed in `0002_align_build_spec.up.sql`: dropped the FK from `run_events`, and
from `queue` and `leases`. `steps` keeps its FK — it is also a projection,
rebuilt in the same transaction — but as `DEFERRABLE INITIALLY DEFERRED` so the
delete-then-reinsert is legal mid-transaction and still checked at COMMIT.

**2. `seq` was 0-based; the spec says 1-based.** Changed. `journal.FirstSeq`
defines the base in one place, and `RunState.NextAppendSeq()` is the only way
writers obtain the next value. A zero `seq` is now always an uninitialised
value rather than a real event.

Note: the migration **cannot** renumber existing events. `run_events` is
append-only and the trigger rejects `UPDATE` — by design. The migration detects
0-based rows and fails with that explanation plus `make reset-db`, rather than
dying on a bare constraint violation.

**3. `RunNeedsReview` event type was missing.** It was modelled as a boolean on
`RunFailed`. Now its own terminal event: a failed run is finished and
understood, a run needing review is finished and *not* understood — nobody knows
whether its last side effect landed. Alerting and the trace viewer must tell
them apart without parsing a payload. `RunNeedsReviewPayload.Reason` is
required; a run halted for a human has to say what the human must check.

**4. `agents.definition` renamed to `agents.graph`**, the spec's name. The
column holds a declarative node graph, which "graph" says and "definition" did
not.

### Divergences kept deliberately

| Spec | Built | Why kept |
|---|---|---|
| `budget_tokens int`, `tokens_used int` | `bigint` | A long agent_loop against a large-context model can exceed 2.1 billion cumulative tokens; `int` would silently overflow. No cost to the wider type. |
| no `queue.enqueued_at` | present | Queue wait time (`enqueued_at` → claim) is the p99 scheduling latency metric M8 has to report. Measuring it later would need a migration and lose all history before it. |
| no `runs.error` | present | The trace viewer's run list shows why a run ended without folding every log. |
| `anchor rebuild` | `anchorctl rebuild` | `anchor` is reserved for the SDK/CLI users would install; `anchorctl` is the operator tool, matching the `kubectl`/`etcdctl` convention. |
| fold uses `map[int]StepState` | dense `[]*StepState` | A slice enforces what the spec requires anyway: `step_index` is assigned sequentially as the worker walks the graph, so a hole is a bug. A map would accept `steps[7]` with no `steps[6]` and the error would surface much later. |
| `ErrLostOwnership` | `ErrSeqConflict` | Same sentinel, named for the cause rather than the consequence. Worth renaming if it reads better in the worker loop at M2. |

---

## M2 — API, queue, worker, idempotency

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | 100 submitted runs all complete, no run claimed twice | **pass** | `TestHundredRunsCompleteWithNoDuplicateExecution` — 3 workers x 4 goroutines; asserts all 100 completed, provider called exactly 100 times (a second execution would exceed that), 200 steps with 200 distinct keys, and projections still matching the fold |
| 2 | Running a step twice with the same key executes it once | **pass** | `TestReserveStepMakesTheSecondAttemptANoop` — second reservation reports `AlreadyRan`, counter fires once, one `steps` row |

Also covered: retries recovering from retryable provider errors, budget refusal
before any spend, `agent_loop` running a tool then a final answer, and a
non-idempotent tool failure halting the run in `needs_review` without retrying.

Not yet built, by design: lease expiry takeover and the reaper (M3), the Docker
sandbox and a real provider (M4), rate limits, circuit breakers and metrics
(M5). `branch` and `map` nodes validate but return "not implemented yet" rather
than silently skipping.

---

## M3 — Leases and crash recovery

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | Under the chaos loop, no run is lost | **pass** | `TestChaosLoopLosesNoRunsAndDuplicatesNoEffects` — 60 runs, 3 real worker processes, a random one `SIGKILL`ed every 900ms. Asserts a minimum kill count and at least one `resumed: true` so the suite cannot pass without exercising recovery |
| 2 | The counting tool shows zero duplicate effects | **pass** | Same test: effect rows must equal run count exactly. The effect is a committed row in `chaos_effects`, not an in-process counter |
| 3 | A rolling restart strands no runs | **pass** | `TestRollingRestartStrandsNoRuns` — `SIGTERM` each worker in turn, all runs complete, effects fire exactly once |

Supporting: `TestReaperRequeuesExpiredLeases`, `TestReaperIgnoresTerminalRuns`,
`TestZombieWorkerCannotAdvanceARunItLost`, and
`TestResumeReusesCompletedStepsInsteadOfRepeatingThem` — the last verified by
reintroducing the bug and watching it fail.

**One real bug found.** Resumption re-executed the whole graph at new indexes
because the step index came from `len(st.Steps)` rather than a walk cursor. It
passed every single-worker test and only appeared under chaos. See the Struggles
section of `docs/learning/M3-leases-crash-recovery.md`.

---

## M4 — Sandbox and real provider

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | A tool that tries to use 1 GB of memory is killed | **pass** | `TestSandboxKillsAToolThatExceedsItsMemoryCap` — real container, 64 MB cap, `dd bs=1G`, killed with exit 137 |
| 2 | A tool that tries network access without declaring it fails | **pass** | `TestSandboxDeniesNetworkToAToolThatDidNotDeclareEgress`, plus `TestSandboxDeniesEgressWhenTheDeploymentForbidsIt` for the second gate |
| 3 | No containers remain after the chaos loop | **pass** | `TestSandboxLeavesNoContainersBehind` (5 calls including a timeout and an OOM) and `TestReapOrphansRemovesLeftoverContainers` |
| 4 | One real end-to-end run against the Anthropic API succeeds | **pass (2026-10-14)** | Two runs of agent `live` completed against `claude-opus-5-5`, message `msg_011Cg2wM2PYRwvMgFkPSErTA`, 36 input / 143 output tokens. Verified from `runs` and the `ModelCallCompleted` payload, not from the script's own exit code. |

Also covered: read-only root filesystem, non-root user, wall-clock timeout, and
schema validation of both tool input (before a container starts) and output.

Provider behaviour is tested against a local server speaking the Anthropic wire
format: message/tool mapping, raw-body capture, retryable vs terminal status
classification, connection failures, and refusal handling.

**Model ids.** The build spec's example graphs use `claude-sonnet-4-6`, which
predates the current generation. Current ids are `claude-opus-5-5` (default),
`claude-sonnet-5-5`, `claude-haiku-5-5`. The adapter does not default a model —
the graph supplies it — so nothing is hardcoded, but the README and
`live-check.sh` use current ids.

### Two findings from the real run

**1. `cents_used` is always zero, so the cost budget can never fire.** The
Anthropic adapter never sets `Response.Cents` -- the API does not return a cost,
it returns token counts, and the price has to be applied per model. The column,
the `budget_cents` field, and the budget check in the interpreter all exist and
all currently compare against zero. The token budget works; the cost budget is
dead code against the real provider.

Confirmed by query: 2 runs with `tokens_used > 0` and `cents_used = 0`.

This is the first thing M5 has to fix -- that milestone is "budgets, rate
limits, breakers", and a budget that cannot trigger is not a budget. It needs a
per-model price table keyed by the model id, applied to prompt and completion
tokens separately, since they are priced differently.

**2. The thinking-block gap is real, not theoretical.** The live response's
content blocks are `text, thinking`. The thinking block carries an empty `text`
(display defaults to omitted) and a `signature`. Current models bind those
blocks to the conversation that produced them, so the gap described below
affects real multi-turn runs, not just hypothetical ones. Single `model_call`
nodes -- what the live check exercises -- are unaffected, which is why it
passed.

**Known gap, deferred to M6/M7.** Anchor rebuilds the message array for each
`agent_loop` turn from its own state rather than echoing the provider's content
blocks back verbatim. Current models bind reasoning blocks to the conversation
that produced them, so a multi-turn loop against a real provider discards them.
Single `model_call` nodes are unaffected. Fixing it means journaling the
assistant content blocks and replaying them unchanged — which M6's replay source
needs anyway.

---

## M5 — Budgets, rate limits, breakers, metrics

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | A run over budget stops with `BudgetExceeded` | **pass** | `TestSpendingBudgetStopsTheRunBeforeTheCall` — provider called **zero** times, `BudgetExceeded` journaled with resource `micros`. Token budget covered separately. |
| 2 | A provider returning errors trips the breaker open, then half-open, then closed | **pass** | `TestBreakerGoesClosedToOpenToHalfOpenToClosed` (state machine, real Redis) and `TestBreakerTripsThroughTheWorkerAndParksRuns` (worker behaviour: park, do not fail) |
| 3 | Submits return 429 above the high-water mark | **pass** | `TestSubmitReturns429AboveTheHighWaterMark`, plus `Retry-After` and the rejection counter |

Also covered: real spend recorded on a completed run (140 micro-dollars for the
mock's token counts on Opus 5.5 pricing), an unpriced model refused under a
spending budget and permitted without one, bucket atomicity under 200 concurrent
takes, exactly-one half-open probe under 50 concurrent callers, fail-closed
limiter and fail-open breaker, and v1 payloads still folding after the v2 bump.

**The cents gap is closed.** All three parts: the adapter never populated cost,
nothing ever checked `BudgetCents`, and the column could not hold the value.
Money is now micro-dollars, cost is derived in `internal/pricing` from token
counts, and `Response.Cents` was removed so a mock cannot invent a cost the real
adapter never produces.

Grafana dashboard committed at `deploy/grafana/anchor-dashboard.json` (13
panels).

---

## M6 — Deterministic replay

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | Replaying a corpus of recorded runs produces a fidelity report | **pass** | `TestFidelityOverACorpusOfRuns` — **100% over 200 runs** of mixed shapes (plain, retried, agent_loop). `anchorctl fidelity` produces the same report from the CLI. |
| 2 | A deliberately injected non-determinism is caught as a divergence | **pass** | `TestDivergenceIsCaughtWhenSomethingWasNotRecorded`, plus unit coverage of every divergence kind |

Note on criterion 2's wording. The spec frames it as "calling `time.Now()`
directly". In Anchor's declarative model there is no user code that could call
it — an agent is JSON the worker interprets, which is the whole reason replay is
provable here. The equivalent failure is simulated at its real root: a source of
difference the journal did not capture, which is what an unrecorded clock read
*is*. Deleting a recorded attempt reproduces that exactly, and the check catches
it.

Also covered: replay reproducing retries (including the failures), `agent_loop`
iterations, refusal to replay an unfinished run or a replay, and comparison
tolerance for timestamps and jsonb key order.

**Two bugs found, one before any code ran.** Per-attempt recording — reasoned
out rather than discovered — and Go's typed-nil trap in the override wiring,
which segfaulted on the first test. Both in the Struggles section of
`docs/learning/M6-deterministic-replay.md`.

---

## Demo scripts

Both `make chaos` and `make live-check` originally existed only in `.PHONY`
with no recipe, so each was a silent no-op. Fixed 2026-10-14, confirmed with
`make -n` and then by running both.

| Script | Status |
|---|---|
| `scripts/chaos.sh` | **pass (2026-10-14)** — `make chaos` at defaults: 7 kills, 5 runs resumed, 40/40 completed, 40 effects for 40 runs, projections verified, exit 0 |
| `scripts/live-check.sh` | **pass (2026-10-14)** — see M4 criterion 4 |

### Three bugs in the chaos script, all found by running it

**1. `kill -9` killed the log filter, not the worker.** The worker was started as
`"$WORKER_BIN" ... | sed "s/^/[worker-N] /" &`, and in a pipeline the shell sets
`$!` to the **last** command. So every kill took out `sed` and left the worker
running. Workers accumulated across the loop.

Fixed with process substitution — `> >(sed ...)` — which leaves the worker as
the backgrounded command, so `$!` is the pid that needs killing.

This is the same mistake as the `go run` one in `live-check.sh`, reintroduced
through a different mechanism. The general rule: whenever a backgrounded command
is composed with anything, check what `$!` actually refers to.

**2. A bare `wait` in the cleanup trap hung the script forever.** With surviving
workers from bug 1, `wait` with no arguments blocks on every background job.
The script ran for over ten minutes before being killed by hand, leaving
workers running on the machine. Cleanup now kills by pid and never calls an
unqualified `wait`.

**3. The work finished before the kills landed.** At the original parameters
(`-pool 2`, 400ms model calls) 40 runs completed in about five seconds against a
30-second kill loop, so the kills hit idle workers and nothing was ever
interrupted. Defaults are now one goroutine per worker and a 2s model call, so
40 runs take roughly 27s against a 20s loop.

The script now **fails** rather than warns when `RESUMED` is zero: a green chaos
run that never interrupted anything proves nothing, and the whole claim is that
a killed worker's run gets picked up and finished.

### Isolation

The demo runs in its own `chaos_demo` schema (`anchorctl` gained a `-schema`
flag on every subcommand) rather than truncating `public`. The earlier version
would have destroyed the live-check evidence every time it ran.

### Process deliverables added

- `docs/learning/M1-journal-fold-rebuild.md` — the learning entry
- `docs/GLOSSARY.md` — term list, grown per milestone
- `docs/NUMBERS.md` — benchmark ledger, empty until M8
- This file

### Still outstanding

- **M1 checkpoint quiz — DEFERRED by the owner on 2026-10-14**, one question in
  (scored 0/1 on Q1). The spec gates M2 on 80%; that gate is knowingly skipped
  to meet a deadline, with quizzes to be taken in a block after the build.

  What this costs: the quiz exists to catch the gap between intuition and
  articulation, and Q1 found exactly that one — the benefits of event sourcing
  and the read cost of it were both unstateable, though the underlying
  understanding was there. That gap does not close by itself while building.

  What keeps the deferral recoverable: a `docs/learning/` entry is still written
  at every milestone. Those are the study material; without them there is
  nothing to revise from later.
- **Tests Marlon owns.** The spec assigns three suites to him with Claude
  reviewing: the fold, idempotency-key behaviour, and the replay divergence
  check. The first two currently exist in Claude's hand
  (`internal/journal/fold_test.go`, `internal/idem/idem_test.go`) and should be
  rewritten from his own understanding. The third does not exist yet (M6).
- **One PR per milestone** — M1 was committed straight to `main`.
