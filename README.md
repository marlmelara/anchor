# Anchor

[![ci](https://github.com/marlmelara/anchor/actions/workflows/ci.yml/badge.svg)](https://github.com/marlmelara/anchor/actions/workflows/ci.yml)

**A durable execution runtime for AI agents.**

An agent run is a long, expensive, failure-prone sequence of model calls and tool
calls. Most implementations hold that sequence in process memory: if the process
dies at step 14 of 20, the run is lost, the money spent on those 14 steps is
gone, and there is no record of what actually happened.

Anchor is the infrastructure layer that fixes this. It provides three guarantees:

- **Durability** — every step is journaled before its effect is applied. A killed
  worker loses no committed progress; another worker resumes the run mid-flight.
- **Determinism** — any completed run can be replayed against its recorded
  model and tool responses and will follow the identical path to the identical
  terminal state. Non-reproducible agent failures become debuggable traces.
- **Safety** — tool calls execute in resource-capped sandboxes; runs execute
  under token and cost budgets, per-tenant rate limits, and provider circuit
  breakers.

This is the durable-execution pattern popularised by Temporal, Restate and
Inngest, narrowed to agent workloads and implemented from scratch. Temporal
solves replay with workflow-code determinism and a history service; Anchor makes
three simplifications that a single-writer, short-lived agent workload allows —
declarative graphs instead of arbitrary code, one worker per run instead of
per-step parallelism, and a Postgres `SKIP LOCKED` queue instead of a
partitioned log.

---

## Status

Milestone 1 of 8 is complete. See [Milestones](#milestones).

---

## Quickstart

Requires Go 1.27+ and Docker.

```bash
make up        # start Postgres and Redis, wait for health
make migrate   # apply the schema
make test      # run everything, including the database tests
make ci        # run exactly what CI runs, before you push
```

Submit a run:

```bash
echo '{"agent":"research","v":1,"nodes":[]}' > /tmp/research.json
go run ./cmd/anchorctl agent-register -name research -file /tmp/research.json
go run ./cmd/anchorctl submit -agent research -input '{"q":"why is the sky blue"}'
go run ./cmd/anchorctl show -run <run-id>
```

Prove the projections are derivable from the log:

```bash
make verify    # recompute from run_events and report any disagreement
```

Anchor's containers bind host ports **5433** (Postgres) and **6380** (Redis) so
they can run alongside another project's database. Override with `make DSN=...`.

---

## Architecture

```
CLIENTS
  [ TypeScript SDK ]  [ Trace Viewer (Next.js) ]
        | submit / cancel / replay        | reads journal
        v                                 |
CONTROL PLANE
  [ anchor-server (Go) ]
     REST API + SSE streaming
        | enqueue
        v
STATE
  [ PostgreSQL ]
     run_events  (append-only, source of truth)
     runs / steps / leases  (projections)
     queue       (claimed via SKIP LOCKED)
        ^
        | claim / heartbeat / journal
EXECUTION
  [ anchor-worker pool (Go) ]
     |--> LLM providers (HTTP)
     |--> Docker sandbox (one container per tool call)
     |--> Redis (rate-limit buckets, breaker state)
     |--> Prometheus (/metrics)
```

| Component | Language | Owns |
|---|---|---|
| `anchor-server` | Go | HTTP API, SSE streaming, validation, enqueue. Stateless. |
| `anchor-worker` | Go | Claim runs via lease, fold journal, execute steps, journal results, heartbeat, retries, graceful drain. |
| `anchor-sandbox` | Go | Docker API wrapper: one container per tool call, with limits, always removed. |
| `trace-viewer` | TypeScript / Next.js | Read-only UI over the journal: run list, DAG, step detail, replay trigger. |
| `sdk` | TypeScript | `defineAgent()`, `defineTool()`, `submitRun()`, `streamRun()`. |

### How state works

`run_events` is the only authoritative record. `runs` and `steps` are
projections, written in the same transaction as the event that implies them and
rebuildable from the log alone:

```bash
go run ./cmd/anchorctl rebuild          # recompute
go run ./cmd/anchorctl rebuild -verify  # report drift without repairing it
```

CI runs `rebuild -verify` after the full test suite. Projection drift is a build
failure, not a warning — it means the append path and the fold have disagreed,
and one of them is wrong.

### How concurrency works

`UNIQUE (run_id, seq)` is the entire concurrency control. Two workers that both
believe they own a run fold the same log, compute the same next `seq`, and race
to insert. Postgres picks a winner; the loser's transaction aborts and it
releases the run. There is no distributed lock, and the guard sits on the write
itself rather than on the intent to write, so it holds even when the lease logic
is wrong.

### How replay works

Every source of non-determinism is captured at record time and injected at
replay time:

| Source | Recorded as | Injected on replay |
|---|---|---|
| LLM completion | `ModelCallCompleted.payload.response` | returned instead of calling the provider |
| Tool output | `ToolCallCompleted.payload.{output, exit_code}` | returned instead of running the container |
| Current time | every event's `created_at` | `RunState.Now()` returns the recorded timestamp |
| Random values | `StepScheduled.payload.rand_seed` | the same seed reseeds the generator |
| Retry timing | `RetryScheduled.payload.delay_ms` | the delay is skipped, the sequence preserved |
| Go map order | forbidden | never iterate a map to drive control flow |

---

## Design decisions

Every decision that had a real alternative is written up in
[`docs/decisions.md`](docs/decisions.md), with the alternative and what the
choice costs.

---

## Milestones

| # | Deliverable | Status |
|---|---|---|
| M1 | Repo, Docker Compose, CI. Schema and migrations. Event append with `(run_id, seq)` uniqueness. Fold. `rebuild` command. Unit tests on the fold. | **done** |
| M2 | `anchor-server` REST API. `SKIP LOCKED` queue. Worker pool claiming and executing a two-step mock agent. Idempotency keys enforced. Retries with backoff. | next |
| M3 | Leases, heartbeats, expiry takeover. Graceful shutdown. Chaos script. Crash recovery proven. | |
| M4 | Docker sandbox executor with limits; tool schema validation; real LLM provider adapter. | |
| M5 | Budgets, Redis token-bucket rate limiting, circuit breaker, backpressure. Prometheus metrics. | |
| M6 | Deterministic replay end-to-end: `ReplaySource`, replay run type, divergence detection, fidelity report. | |
| M7 | Trace viewer. TypeScript SDK. | |
| M8 | k6 load tests, chaos suite in CI, measured benchmarks, demo video. | |

---

## Benchmarks

Measured against a mock provider with fixed latency, so the numbers describe
Anchor rather than the LLM. Hardware and methodology will be stated here
alongside the results.

| Measurement | Result |
|---|---|
| Throughput (steps/sec at W workers) | pending M8 |
| p99 scheduling latency (claim → start) | pending M8 |
| Recovery time after worker kill | pending M8 |
| Replay fidelity over ≥200 runs | pending M6 |
| Duplicate effects under fault injection | pending M3 |

---

## Scope

Deliberately **out** of scope: multi-region replication, distributed consensus,
a visual workflow builder, multi-tenancy beyond a `tenant_id` column and
per-tenant limits, autoscaling, model fine-tuning or RAG, and auth beyond a
static per-tenant API key. Anchor runs agents; it is not an agent.
