# Load tests

k6 scripts measuring Anchor, not an LLM.

Every run here uses the **mock provider at a fixed latency**, so the figures
describe Anchor's own cost — queueing, claiming, journaling, projection writes —
rather than a provider's response time. Pointed at a real provider the numbers
would mostly be Anthropic's latency, which says nothing about this system.

## Running

```bash
make bench            # the full sweep, writing results to docs/NUMBERS.md
```

Or by hand:

```bash
# terminal 1
go run ./cmd/anchor-server

# terminal 2
go run ./cmd/anchor-worker -pool 8 -mock-latency 50ms -metrics-addr :9090

# terminal 3
k6 run -e ANCHOR_URL=http://localhost:8080 -e AGENT=bench -e RATE=50 loadtest/submit.js
```

## Why a constant arrival rate

The scenario uses `constant-arrival-rate`, not a fixed pool of virtual users.

With fixed VUs each one waits for its response before sending again, so a
slowing server automatically slows the load. The test backs off exactly when the
system is struggling, and the measurement flatters it. A constant arrival rate
keeps pushing regardless, which is what real traffic does — and it is the only
way to find the point where the queue starts growing.

## Reading the output

- `anchor_submit_latency_ms` — how long the API took to accept a run. This is
  the *write* path only; it does not include execution.
- `anchor_rejected_429` — submissions refused above the queue high-water mark.
  **Not a failure.** It is backpressure working, and a run with 429s is a run
  that found the saturation point.
- Scheduling latency and step duration come from the worker's Prometheus
  endpoint, not from k6 — k6 never sees them, because they happen after the API
  returns.
