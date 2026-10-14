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

### Process deliverables added

- `docs/learning/M1-journal-fold-rebuild.md` — the learning entry
- `docs/GLOSSARY.md` — term list, grown per milestone
- `docs/NUMBERS.md` — benchmark ledger, empty until M8
- This file

### Still outstanding

- **M1 checkpoint quiz** — not yet taken. 80% without notes gates M2.
- **Tests Marlon owns.** The spec assigns three suites to him with Claude
  reviewing: the fold, idempotency-key behaviour, and the replay divergence
  check. The first two currently exist in Claude's hand
  (`internal/journal/fold_test.go`, `internal/idem/idem_test.go`) and should be
  rewritten from his own understanding. The third does not exist yet (M6).
- **One PR per milestone** — M1 was committed straight to `main`.
