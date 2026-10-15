# M8 — Benchmarks and shipping

> Study material for the M8 checkpoint quiz, and the final exam.

---

## 1. Plain-English summary

Measuring the system, and saying what the numbers mean.

**Load tests** push submissions at a fixed rate and record what happens.
**Benchmarks** read the worker's own metrics for the figures k6 cannot see —
how long a run waited, how long a step took. **The chaos suite** runs in CI, so
a regression in durability fails the build rather than being discovered later.

The honest part matters as much as the numbers. Every figure has the command
that produced it, the hardware it ran on, and the date. Quantiles are bucket
upper bounds, not point estimates, and the hardware is a laptop running
Postgres, Redis, the server, the worker and the load generator at once. Both
caveats are written down rather than left for a reader to discover.

*(139 words)*

---

## 2. Data flow: one benchmark run

1. **`scripts/bench.sh`** builds the binaries, creates its own schema, and
   registers a two-node agent.

2. **Starts a server and a worker** with the mock provider at a fixed latency.
   This is what makes the numbers describe Anchor: a real provider would
   contribute its own response time and the figure would mostly be Anthropic's.

3. **Runs k6** at a constant arrival rate, which submits runs over HTTP.

4. **Waits for the queue to drain**, timing it.

5. **Reads the worker's `/metrics`** and computes quantiles from the Prometheus
   histograms — k6 never sees these, because they happen after the API has
   already returned 202.

6. **Prints the figures** for `docs/NUMBERS.md`, with hardware and config.

---

## 3. Annotated code

### 3.1 Constant arrival rate, not fixed users

```js
scenarios: {
  steady: {
    executor: "constant-arrival-rate",
    rate: Number(__ENV.RATE || 50),
    timeUnit: "1s",
    preAllocatedVUs: 20,
    maxVUs: 200,
  },
},
```

The single most important choice in the load test.

With a fixed pool of virtual users, each one waits for its response before
sending again. So when the server slows, **the load automatically slows too** —
the test backs off at exactly the moment the system is struggling, and the
measurement flatters it. You can never find the breaking point, because the test
refuses to push past it.

A constant arrival rate keeps sending regardless, which is what real traffic
does. It is the only way the saturation result below exists at all.

### 3.2 Two latencies that move differently

The result worth being able to explain:

| | Steady (50/s) | Saturated (400/s) |
|---|---|---|
| Scheduling latency (claim → first step) | p99 50 ms | p99 **50 ms** |
| Queue wait (submit → claim) | p99 250 ms | p50 **30 s** |

At eight times the sustainable rate, **scheduling latency does not move** while
queue wait grows by two orders of magnitude.

That is not a curiosity, it is a diagnosis. Everything that backs up, backs up
*before* the claim. Once a worker has a run, starting it is fast no matter how
much is waiting. So the fix for a slow system here is **more workers**, not a
faster worker — and a single averaged "latency" number would have hidden the
distinction entirely.

This is the concrete answer to "p99 scheduling latency doubles under load, how
do you diagnose it": compare it against queue wait. If scheduling latency rose
and queue wait did not, the problem is inside the worker — the claim query, the
connection pool, lock contention. If queue wait rose and scheduling latency did
not, the fleet is simply too small.

### 3.3 Why the quantiles are read with python, not awk

```bash
printf '%s' "$METRICS" | python3 -c '...'
```

The first version used `awk` with `match($0, /le="([^"]+)"/, m)` — a **GNU awk**
extension. macOS ships BSD awk, which has no three-argument `match`, so the
script failed with a syntax error on a Mac and would have worked fine in CI.

A tool that only breaks on the author's own machine is a bad trade.

### 3.4 Summing a labelled histogram

```python
buckets = defaultdict(float)
...
buckets[le] += float(m.group(2))
```

A labelled histogram emits **one series per label set**. `step_duration` has a
`kind="model"` series and a `kind="tool"` one, so every `le` appears twice.

The first version sorted all the lines as a single list and took the first
cumulative count past the target — which reported a **50 ms model call as a
1 ms p99**, because the tool series' buckets came first.

Two fixes: sum across labels for an aggregate, and accept an optional filter so
`model` and `tool` can be reported separately. The second is what makes the
output readable — a combined step-duration quantile is meaningless when half the
population is 50 ms and half is 1 ms.

### 3.5 The chaos suite in CI

```yaml
- name: Chaos demo
  run: ./scripts/chaos.sh 24 3 12

- name: Replay fidelity
  run: go run ./cmd/anchorctl fidelity -limit 20
```

Durability and determinism are the project's two central claims, so a regression
in either has to fail the build. The Go chaos tests already run under `go test`;
this adds the **script** path, which spawns real worker processes and
`SIGKILL`s them — and which would otherwise rot unnoticed, as it did once
already.

The script fails when no run was resumed, so a green chaos run that never
interrupted anything cannot pass.

---

## 4. New terms

| Term | One sentence |
|---|---|
| Load test | Sending heavy simulated traffic to measure throughput and latency. |
| k6 | A load-testing tool whose tests are written in JavaScript. |
| Virtual user (VU) | One simulated client in a load test. |
| Constant arrival rate | Sending at a fixed rate regardless of how fast responses come back. |
| Closed vs open model | Fixed users that wait (closed) versus a fixed send rate (open). |
| Saturation point | The load above which the queue grows faster than it drains. |
| Throughput | Work completed per unit of time. |
| Bucket upper bound | A histogram quantile is the top of a bucket, so an upper bound, not a point. |
| Drain time | How long the queue takes to empty after load stops. |

---

## 5. Decisions made here

In [`../decisions.md`](../decisions.md):

- A constant arrival rate, not fixed virtual users
- Benchmarks run against the mock provider only
- Quantiles come from the worker's metrics, not from k6
- The chaos script and a fidelity sweep run in CI
- Every number carries its command, hardware and date

---

## 6. Struggles

### A benchmark that measured the wrong thing twice

**First**, the quantile extraction used GNU awk's three-argument `match`, which
BSD awk — what macOS ships — does not have. Every quantile printed an awk syntax
error. Caught immediately because the output was visibly broken.

**Second, and worse**, after fixing that, the figures *looked* plausible and were
wrong: step duration reported **p99 of 1 ms for a 50 ms model call**. The cause
was treating a labelled histogram's two series as one sorted list.

The first failure announced itself. The second did not — it produced a number
that could have gone straight into a README. What caught it was knowing what the
answer should be: the mock latency was configured at 50 ms, so a 1 ms p99 was
arithmetically impossible.

**The lesson, and it is the one this whole project kept teaching:** a
measurement you cannot sanity-check against something you already know is not a
measurement. Every number here is checked against the configuration that
produced it.

### The saturation run was the real finding

The steady-state numbers are unremarkable — the worker kept up, drain was zero,
nothing interesting. Running at **eight times** the sustainable rate is what
produced the result worth talking about: nothing lost, scheduling latency flat,
queue wait up by two orders of magnitude.

A benchmark that only measures the comfortable case tells you the system works
when it is not under pressure, which nobody doubted.

---

## 7. Final exam check

The twenty final-exam questions all now have code to point at. Two notes:

- **Q11 — "p99 scheduling latency doubles under load, how do you diagnose it?"**
  The measured answer is in §3.2: compare it against queue wait. They move
  independently, and which one moved says whether the problem is inside the
  worker or the fleet is too small.
- **Q18–20 — the story questions.** The Struggles sections across
  `docs/learning/` hold eight real bugs with cause, discovery and fix. The
  strongest three for "tell me about a hard bug":
  - the **Postgres microsecond truncation** (M1) — invisible on macOS, immediate
    on Linux, would have surfaced at M6 as unexplainable replay divergence
  - the **walk-cursor bug** (M3) — resumption re-executed the whole graph;
    passed every single-worker test and only appeared under chaos
  - the **duplicate effect that was not one** (pre-M7) — the system was correct
    and the *test tool* lied about being idempotent

**A pattern worth naming**, because it recurred five times: the chaos test that
passed after one kill, the memory-limit test whose command never allocated, the
phantom make targets, the live-update check that loaded a finished run, and the
benchmark quantile above. In every case **a check existed and tested something
adjacent to the claim**. The habit that catches it is asserting on the *effect*
rather than the setup — a kill count rather than a green test, exit 137 rather
than a non-zero exit, `make -n` rather than a grep of `.PHONY`.
