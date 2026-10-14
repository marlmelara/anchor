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
| 4 | One real end-to-end run against the Anthropic API succeeds | **NOT VERIFIED** | `scripts/live-check.sh` is written and `make live-check` runs it, but it needs a real credential and spends money, so it has not been executed. **Marlon must run this himself before claiming M4 complete.** |

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

**Known gap, deferred to M6/M7.** Anchor rebuilds the message array for each
`agent_loop` turn from its own state rather than echoing the provider's content
blocks back verbatim. Current models bind reasoning blocks to the conversation
that produced them, so a multi-turn loop against a real provider discards them.
Single `model_call` nodes are unaffected. Fixing it means journaling the
assistant content blocks and replaying them unchanged — which M6's replay source
needs anyway.

---

## Demo scripts: written, not verified

`make chaos` and `make live-check` existed only in `.PHONY` with no rule, so
both were silent no-ops (`make: Nothing to be done for ...`). A string
replacement failed quietly and the verification checked the wrong thing: the
`.PHONY` line and the script's `bash -n` syntax, never whether the rule existed.
Fixed 2026-10-14; confirmed with `make -n`.

Current status of the two shell entry points:

| Script | Status |
|---|---|
| `scripts/chaos.sh` | **Never completed a run.** Syntax-checked only. An attempted run was interrupted and left a worker process respawning, which had to be killed by hand. |
| `scripts/live-check.sh` | **Never run.** Needs a real Anthropic credential. |

**What this does not affect.** M3's acceptance criteria were proven by the Go
chaos suite -- `TestChaosLoopLosesNoRunsAndDuplicatesNoEffects` and
`TestRollingRestartStrandsNoRuns` -- which spawn real worker processes, send
real `SIGKILL`, and run in CI. Those remain verified. The shell scripts are
convenience wrappers for the demo video, and a broken wrapper does not weaken
the test evidence.

**Lesson, recorded because it recurred.** Three times now a check has passed
while testing the wrong thing: the chaos suite that passed after one kill, the
memory-limit test whose command never allocated, and these two phantom make
targets. In each case the verification step existed but confirmed something
adjacent to the claim. The habit that catches it is asserting on the *effect*
rather than on the setup -- a kill count rather than a green test, exit 137
rather than a non-zero exit, `make -n <target>` rather than a grep of `.PHONY`.

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
