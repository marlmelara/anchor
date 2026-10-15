# M6 — Deterministic replay

> Study material for the M6 checkpoint quiz. This is the milestone the project
> exists for, and the one interviewers push hardest on.

---

## 1. Plain-English summary

A replay re-runs a finished run and checks it does exactly the same thing.

It works because a replay is a **real run** — claimed from the queue, walking
the same graph, writing its own events. The only difference is what it is
holding. A live run holds an Anthropic client, a Docker sandbox and the clock. A
replay holds three objects that answer from the original run's journal instead,
and the worker cannot tell them apart because they satisfy the same interfaces.

Afterwards the two event logs are compared. If the replay produced the same
events, in the same order, about the same steps, it reproduced the run. The
**first** point where they differ is the divergence — reported alone, because
everything after it is a consequence rather than a cause.

*(142 words)*

---

## 2. Data flow: one replay

1. **`store.SubmitReplay`** loads the original, refuses it if unfinished or if
   it is itself a replay, and submits a normal run with `replay_of` set and the
   **same pinned agent version**.

2. **A worker claims it** through the ordinary queue. Nothing about the claim is
   special.

3. **`executeRun`** sees `st.ReplayOf != nil`, loads the original's events, and
   calls `replay.Build` to construct the recording.

4. **`interpretFrom`** receives the replay provider and runner as overrides.
   Everything downstream — `beginStep`, the idempotency reservation, the event
   appends — is unchanged.

5. **Each model call** goes to `replay.Provider.Complete`, which looks up
   `(step index, attempt)` and returns the recorded response — or the recorded
   **failure**, so the interpreter takes the same retry path.

6. **Each tool call** goes to `replay.Runner.Run`, which reads its position from
   the context and returns the recorded output.

7. **Afterwards**, `replay.Compare` checks the event sequences and
   `replay.CompareFinal` checks the terminal state.

---

## 3. Annotated code

### 3.1 Why replay is not a separate code path

```go
var replayProvider provider.Provider
var replayRunner tool.Runner
if st.ReplayOf != nil {
	rec, err := replay.Build(originalEvents)
	rp := replay.NewProvider(rec)
	replayProvider = rp
	replayRunner = replay.NewRunner(rec, rp)
}
outcome := w.interpretFrom(runCtx, st, graph, claimedAt, replayProvider, replayRunner)
```

This is the design in four lines. The interpreter is identical for a live run
and a replay; only its dependencies change.

That matters more than it looks. If replay were its own code path, anything that
only happened on the live path would be **untested by replay**, and anything
replay did differently would be a divergence the real run never had. The thing
being verified has to be the thing that actually runs.

### 3.2 Per-attempt recording — the bug avoided by thinking

```go
type RecordedStep struct {
	Index    int
	Kind     journal.StepKind
	Attempts map[int]Outcome   // keyed by attempt, NOT collapsed
}
```

Consider a run whose model call failed once and then succeeded:

```
StepScheduled(0)
ModelCallStarted(0, attempt 0)
StepFailed(0, attempt 0)
RetryScheduled(0, attempt 1)
ModelCallStarted(0, attempt 1)
ModelCallCompleted(0, attempt 1)
```

If the replay source returned the recorded **success** when asked for step 0, the
replay would produce:

```
StepScheduled(0)
ModelCallStarted(0, attempt 0)
ModelCallCompleted(0, attempt 0)   ← three events, not six
```

Divergence — and **every run that ever retried would score as a failure**, for a
reason that is not a real bug. So the recording keeps each attempt, and a
recorded failure is replayed *as a failure*:

```go
if outcome.Failed {
	err := fmt.Errorf("replayed failure: %s", outcome.Error)
	if outcome.Retryable {
		return provider.Response{}, &provider.RetryableError{Err: err}
	}
	return provider.Response{}, err
}
```

**This is built by walking raw events, not the folded state.** The fold
deliberately collapses attempts — a retried step shows only its latest status —
so replay needs exactly the history the fold throws away.

### 3.3 What the divergence check compares, and what it must not

```go
type Signature struct {
	Seq       int
	Type      journal.EventType
	StepIndex int
	Attempt   int
}
```

| Compared | Why |
|---|---|
| Event **type** | The shape of the execution |
| **Order** | A different order is a different path |
| **Step index** and **attempt** | Which work the event is about |
| Terminal **output** and **status** | Computed, not handed over |

| Not compared | Why |
|---|---|
| **Timestamps** | A replay happens later *by definition*. Comparing times would make every replay diverge at event one and the check would be worthless. |
| **Database ids** | Sequence numbers, meaningless across runs. |
| **Model response bytes** | The replay is *handed* those. Comparing them confirms the recording was read correctly — testing the test, not the system. |

Terminal output is the interesting exception: it is **computed** by the
interpreter from the recorded parts. Identical events producing a different
final answer means something outside the journal reached the result, which is the
most alarming finding the check can make.

### 3.4 Only the first divergence

```go
func (p *Provider) note(d *Divergence) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.divergence == nil {
		p.divergence = d
	}
}
```

Once a replay has diverged, everything after it is downstream of that one
difference. A run that takes a wrong turn at step 2 will differ at every
subsequent step, and listing all of it buries the cause under its consequences.

**The first divergence is the bug; the rest is weather.**

### 3.5 Fidelity excludes what it cannot measure

```go
func (r *Report) Fidelity() float64 {
	comparable := r.Matched + r.Diverged
	if comparable == 0 {
		return 0
	}
	return float64(r.Matched) / float64(comparable) * 100
}
```

A replay that **errored** — never picked up, timed out, agent version missing —
says nothing about determinism. It is a harness failure, not a fidelity failure,
and counting it as either would be wrong.

It is excluded from the denominator and **reported separately**, so the
exclusion is visible rather than quietly improving the number. A report that
silently dropped failures it found inconvenient would not be a measurement.

---

## 4. New terms

| Term | One sentence |
|---|---|
| Replay | Re-running a finished run against its recorded inputs instead of live ones. |
| Recording | Everything the original run observed, keyed by step and attempt. |
| Divergence | The first point where a replay differs from its original. |
| Divergence point | Where the two runs parted, as opposed to everywhere they then differ. |
| Replay fidelity | The share of replayed runs that reproduced their original exactly. |
| Event signature | The comparable shape of an event: type, order, step, attempt. |
| Typed nil | A nil pointer stored in an interface, making the interface non-nil. |
| Dependency injection | Passing a component its dependencies so they can be swapped — how replay works at all. |

---

## 5. Decisions made here

In [`../decisions.md`](../decisions.md):

- Replay is the same code path with swapped dependencies
- Recordings keep per-attempt outcomes
- The signature excludes timestamps and ids
- Only the first divergence is reported
- Errored replays leave the fidelity denominator
- A replay pins the original's agent version
- Replays skip rate limits and the breaker

---

## 6. Struggles

### Retries would have broken every replay

**Found by reasoning, before any code ran.** Working through what a retried step
produces, the replay source as first designed would have returned the recorded
success immediately — skipping `StepFailed` and `RetryScheduled` and producing a
shorter event sequence.

Every run that ever retried would have scored as a fidelity failure. With a
flaky provider that could be most of the corpus, and the fidelity number would
have looked like a determinism catastrophe while the determinism was fine.

**Fix.** Outcomes keyed by attempt, built by walking raw events rather than the
fold.

**What it taught.** The fold's collapsing of attempt history is correct *for the
fold* and wrong for replay. Two consumers of the same log can need different
views of it, and "just use the fold" was the wrong instinct.

### The typed-nil trap

**Symptom.** A segfault on the first replay test:
`replay.(*Provider).Complete(0x0, ...)`.

**Cause.** `var replayProvider *replay.Provider` declared as a *concrete pointer*
and passed to a parameter of interface type. A nil pointer stored in an
interface makes **the interface non-nil** — it holds a type and a nil value. The
`if replayProvider != nil` guard inside the callee was comparing the interface,
which was not nil, so the nil pointer was dereferenced on first use.

**Fix.** Declare the variables as the *interface* types at the call site and
only ever assign real values. A non-replay run then passes a genuine nil.

**What it taught.** The canonical Go gotcha, and the reason it bites is that the
guard *looks* right. The rule: nil-check a concrete pointer **before** it
crosses into an interface, never after.

### A struct that looked comparable and was not

`Signature` originally used `*int` for step index and attempt. Go's `==` on a
struct with pointer fields compares the **pointers**, so two signatures printing
identically compared as different — which a test caught only because it used
`==` while the production code used a helper.

Changed to plain `int` with `-1` for absent. A type that supports `==` but gives
the wrong answer is worse than one that does not compile.

---

## 7. Checkpoint quiz check

The five M6 questions still match the code. Notes:

- **Q3 — "How does the replay provider detect divergence?"** The published
  answer covers a missing recorded call. Add the other kinds: wrong step kind,
  wrong tool, an attempt the original never made, and — separately, after the
  run — a different event sequence or terminal output.
- **Q5 — "Why did the declarative graph make replay easier?"** Worth stating the
  consequence: because there is no user code, there is nothing that *could*
  call `time.Now()`, so determinism is a property of the design rather than
  something to police.

**Added questions:**

6. A run's step failed once and then succeeded. What must the recording keep,
   and what breaks if it keeps only the final result?
   *Key: every attempt. With only the final result the replay returns the
   success immediately, skipping StepFailed and RetryScheduled — a shorter
   sequence, so every run that ever retried scores as a fidelity failure.*
7. Why are timestamps excluded from the comparison, but terminal output
   included?
   *Key: a replay happens later by definition, so comparing times would make
   every replay diverge at event one. Terminal output is computed by the
   interpreter rather than handed to it, so a difference there means something
   outside the journal reached the result.*
8. Why report only the first divergence?
   *Key: everything after it is downstream of that one difference. A run that
   turns wrong at step 2 differs at every later step, and listing all of it
   buries the cause under its consequences.*
9. Why are errored replays excluded from the fidelity denominator?
   *Key: a replay that could not run says nothing about determinism — it is a
   harness failure. It is reported separately so the exclusion is visible rather
   than quietly improving the number.*
