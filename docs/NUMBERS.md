# Numbers

Every benchmark result that appears in the README, on a resume, or in an
interview answer has a row here with the exact command that produced it, the
hardware, and the date. **A number without a row here does not get stated.**

All measurements use the **mock provider at a fixed latency**, so the figures
describe Anchor's own cost — queueing, claiming, journaling, projection writes —
rather than a provider's response time. Against a real provider these would
mostly be Anthropic's latency, which says nothing about this system.

## Hardware

| Field | Value |
|---|---|
| Machine | Apple M2 |
| Cores | 8 |
| RAM | 8 GB |
| OS | macOS 26.6.2 |
| Postgres | 17-alpine in Docker, host port 5433 |
| Redis | 7-alpine in Docker, host port 6380 |

A laptop, not a server. These numbers are a floor, and they are reported as
measured rather than extrapolated.

## Results

Measured 2026-10-14 with `./scripts/bench.sh <rate> <duration> <pool>`.

### Steady state — 50 submissions/sec, 8 worker goroutines

```
./scripts/bench.sh 50 30s 8
```

| Measurement | Value |
|---|---|
| Runs submitted | 1501 |
| Runs completed | **1501** (none lost) |
| Steps completed | 3002 |
| Throughput | **100 steps/sec** |
| Scheduling latency (claim → first step) | p50 5 ms, p95 25 ms, **p99 50 ms** |
| Queue wait (submit → claim) | p50 10 ms, p99 250 ms |
| Model step duration | p50 100 ms, p99 100 ms |
| Tool step duration | p50 1 ms, p99 1 ms |
| Drain after load stopped | 0 s |

The worker keeps up: the queue never builds, and drain is immediate.

### Saturation — 400 submissions/sec, same 8 goroutines

```
./scripts/bench.sh 400 20s 8
```

| Measurement | Value |
|---|---|
| Runs submitted | 7968 |
| Runs completed | **7968** (none lost) |
| Steps completed | 15936 |
| Throughput | **199 steps/sec** |
| Scheduling latency | p50 10 ms, p95 25 ms, **p99 50 ms** |
| Queue wait | **p50 30 s**, p99 beyond the histogram |
| Drain after load stopped | 60 s |

**The interesting result, and the one worth explaining.** At eight times the
sustainable rate, nothing is lost — the system queues. And the two latencies
move completely differently:

- **Scheduling latency barely changes** (p99 50 ms at both rates). Once a worker
  claims a run, starting it is fast regardless of how much work is waiting.
- **Queue wait explodes** (250 ms → 30 s). Everything that backs up, backs up
  *before* the claim.

That is why they are separate metrics. A single "latency" number would have
averaged a flat 50 ms against a 30-second wait and told you nothing about where
the time went — or which component to scale.

Throughput ceiling is ~199 steps/sec here. The arithmetic bound is 8 goroutines
÷ ~51 ms per run × 2 steps ≈ 314 steps/sec, so roughly a third of the budget
goes to claiming, journaling and projection writes rather than to the work
itself.

No 429s were returned: ~8000 queued runs stayed under the 10,000 high-water
mark. Backpressure is exercised by its own test rather than by this sweep.

## Durability

| Measurement | Value | How |
|---|---|---|
| Duplicate effects under fault injection | **zero** | `make chaos` — 40 runs, 7+ worker `SIGKILL`s, 5 resumptions. Every effect applied exactly once |
| Runs lost under fault injection | **zero** | Same run; also `TestChaosLoopLosesNoRunsAndDuplicatesNoEffects` in CI, 60 runs |
| Runs stranded by a rolling restart | **zero** | `TestRollingRestartStrandsNoRuns` |
| Replay fidelity | **100% over 200 runs** | `anchorctl fidelity`, mixed shapes (plain, retried, `agent_loop`) |

Recovery time after a worker kill is bounded by the lease duration — the
deliberate trade recorded in `decisions.md`, since a dead worker cannot
announce its own death. With the 2-second lease used by the chaos demo, a
killed worker's run is back on the queue within ~2.5 s (lease expiry plus one
reaper tick).

## Before and after

"Cut p99 scheduling latency from X to Y" is a better interview story than a
single final number. Both sides of any optimisation go here, with the commit.

| Change | Metric | Before | After | Commit |
|---|---|---|---|---|
| *(none yet — the first measurement above is the baseline)* | | | | |

## Honesty notes

Three things that would otherwise overstate these figures:

1. **Prometheus quantiles are bucket upper bounds.** A 50 ms model call is
   reported as "p50 100 ms" because 0.1 s is the next bucket boundary. The true
   value lies between 50 and 100 ms. Every quantile here is an upper bound, not
   a point estimate.
2. **Measured on a laptop** under its own Docker daemon, with Postgres, Redis,
   the server, the worker and k6 all competing for eight cores. A dedicated host
   would do better; this is a floor.
3. **Steps/sec counts steps, not useful work.** A step against the mock costs
   50 ms of sleep. The figure describes how fast Anchor can schedule, journal
   and account for steps — which is the thing being measured — not how much an
   agent achieved.
