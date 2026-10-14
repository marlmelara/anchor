# Numbers

Every benchmark result that appears in the README, on the resume, or in an
interview answer must have a row here with the exact command that produced it,
the hardware, and the date. A number without a row here does not get stated.

Benchmarks run against a **mock LLM provider with a fixed 50 ms delay**, so the
measurements describe Anchor rather than the model.

## Hardware

| Field | Value |
|---|---|
| Machine | *(fill at M8)* |
| CPU | |
| RAM | |
| OS | |
| Postgres | 17-alpine in Docker, host port 5433 |
| Redis | 7-alpine in Docker, host port 6380 |

## Results

| Measurement | Value | Command | Date | Milestone |
|---|---|---|---|---|
| Throughput (steps/sec at W workers) | — | | | M8 |
| p99 scheduling latency (claim → start) | — | | | M8 |
| p50 / p95 step duration | — | | | M8 |
| Recovery time after `SIGKILL` | — | | | M3 |
| Replay fidelity over ≥200 runs | — | | | M6 |
| Duplicate effects under fault injection | — | | | M3 |
| Real runs executed continuously | — | | | post-M8 |

## Before/after records

"Cut p99 scheduling latency from X to Y by batching the claim transaction" is a
far better interview story than a single final number. Record both sides of any
optimisation here, with the commit that made the change.

| Change | Metric | Before | After | Commit |
|---|---|---|---|---|
| | | | | |
