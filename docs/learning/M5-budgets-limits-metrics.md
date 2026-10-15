# M5 — Budgets, rate limits, breakers, metrics

> Study material for the M5 checkpoint quiz. Every code block is the real code.

---

## 1. Plain-English summary

Four protections, each stopping a different way a run can go wrong.

A **budget** is a hard cap on what one run may spend. Checked in code before
every model call — never by asking the model to behave, because a model that is
misbehaving is exactly the one that ignores instructions.

A **rate limit** caps how fast calls may happen, per tenant and per provider. It
is a token bucket: tokens refill at a steady rate, each call spends one, and an
empty bucket means wait.

A **circuit breaker** notices a provider is failing and stops calling it for a
while, rather than burning time and money on calls that will fail anyway.

**Metrics** make all of it visible — how long runs wait, what they cost, how
often workers die.

The first three live in Redis because they are fleet-wide decisions. A limit of
ten per second enforced per worker, across twelve workers, is a limit of a
hundred and twenty per second.

*(149 words)*

---

## 2. Data flow: one model call, guarded

1. **`interpret.go` → `modelStep`.** Before anything: is this step already done?
   If so the whole check is skipped — a resumed run must not re-evaluate a gate
   the original run passed.

2. **`budgetWouldBreach`** projects what the call will cost and compares against
   both budgets. Pessimistically: the per-call output ceiling is assumed,
   because the real length is unknown until after the money is spent. Over
   budget → `BudgetExceeded` + `RunFailed`, before any spend.

3. **`Guards.Check`** consults the breaker first, then the tenant bucket, then
   the provider bucket. Breaker first so a refused call does not also spend a
   rate-limit token.

4. **Refused → the run is parked.** `e.parkFor` is set and `errParked` returned;
   `finish` requeues the run with a future `available_at` and releases the
   lease.

5. **Allowed → the call is made**, then `Guards.Record` folds the result into
   the breaker — unless the context was cancelled, because a deploy that
   cancels in-flight work must not trip every breaker in the fleet.

6. **On success**, `pricing.Cost` turns the returned token counts into
   micro-dollars and `ModelCallCompleted` carries `cost_micros`.

---

## 3. Annotated code

### 3.1 Why money is micro-dollars — `internal/pricing`

```go
const MicrosPerDollar int64 = 1_000_000

type Price struct {
	InputPerMTok  int64
	OutputPerMTok int64
}

func Cost(model string, promptTokens, completionTokens int64) (int64, error) {
	p, err := Lookup(model)
	if err != nil {
		return 0, err
	}
	input := promptTokens * p.InputPerMTok / 1_000_000
	output := completionTokens * p.OutputPerMTok / 1_000_000
	return input + output, nil
}
```

The number that forced this. A real call — 36 input, 143 output on Opus 5.5:

```
36  × $4/MTok  = $0.000144
143 × $20/MTok = $0.00286
                 $0.003004
```

**In integer cents that is zero.** A budget counted in cents can therefore never
trigger, which is worse than having no budget: the operator believes they have
one. Micro-dollars give six decimal places, so that call records `3004`.

Three details that matter:

- **Prices are stored per MILLION tokens.** Haiku's input is $0.10/MTok — a
  *tenth* of a micro-dollar per token. Per-million keeps every figure an exact
  integer.
- **Input and output are priced separately.** A single blended rate would
  misprice every run.
- **An unknown model is an error, never zero.** A silent zero is the exact bug
  this package exists to fix.

### 3.2 The token bucket — `internal/ratelimit/bucket.go`

```lua
local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local tokens = tonumber(b[1])
if tokens == nil then tokens = cap; ts = now end

local elapsed = math.max(0, now - ts) / 1000
tokens = math.min(cap, tokens + elapsed * rate)

local allowed = 0
if tokens >= cost then tokens = tokens - cost; allowed = 1 end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
```

- **Why Lua.** Redis runs a script to completion without interleaving anything
  else. Issued as separate Go commands, two workers could both read "one token
  left" and both take it. The test that proves this fires 200 concurrent takes
  at a capacity of 20 and asserts exactly 20 succeed.
- **Lazy refill.** Rather than a timer adding tokens on a schedule, work out how
  many *would* have been added since the last call. Equivalent, and it costs
  nothing while a bucket is idle.
- **The clock is Redis's own `TIME`**, not the caller's. A worker's clock is the
  wrong source: skew between workers would make the same bucket refill at
  different apparent rates depending on who asked.
- **Capacity vs rate** answer different questions. Capacity 10 with rate 2
  allows ten calls immediately, then two per second. One number cannot express
  that.

### 3.3 Fail open or fail closed — the asymmetry

```go
// Bucket
if err != nil {
	if b.cfg.FailOpen { return true, 0, nil }
	return false, 0, fmt.Errorf(...)   // default: DENY
}

// Breaker
if err != nil {
	if b.cfg.FailOpen { return true, StateClosed, nil }   // default: ALLOW
	...
}
```

When Redis is down, these two do **opposite** things, and the reason is what
each one is for:

- A **rate limit is a promise** to someone else's service. Flooding a provider
  during a Redis outage breaks that promise, so it fails **closed**.
- A **breaker is an optimisation** for us — it saves money on calls likely to
  fail. A Redis outage must not halt all work, so it fails **open**.

Budgets follow the limiter: a spending cap that cannot be computed refuses the
call.

### 3.4 The half-open probe — `internal/ratelimit/breaker.go`

```lua
if st == 'open' then
  if now - openedAt < cooldown then return {0, 'open'} end
  redis.call('HSET', key, 'state', 'half-open')
  redis.call('SET', probeKey, '1', 'PX', cooldown, 'NX')
  return {1, 'half-open'}
end
```

The three states, and what each protects:

| State | Behaviour | Protects |
|---|---|---|
| closed | calls pass | nothing; normal |
| open | fail fast, no call made | us (time and money) **and** the provider (room to recover) |
| half-open | exactly ONE probe | the provider, from the whole fleet returning at once |

`SET ... NX` is set-if-absent: a tiny distributed lock. Every worker reaches the
end of the cooldown at the same instant, and exactly one must get through — the
test fires 50 concurrent `Allow` calls and asserts precisely one is admitted.

A **failed probe reopens** the breaker and restarts the cooldown.

### 3.5 Park, do not sleep

```go
if !decision.Allowed {
	e.parkFor = decision.RetryAfter
	return provider.Response{}, errParked
}
```

A rate-limited run is **requeued with a future `available_at`**, not slept on. A
sleeping goroutine holds a worker slot *and* a lease while doing nothing — so
the slot is unavailable for work that could proceed, and the lease can expire
mid-sleep and let another worker claim a run this one still thinks it owns.

The test asserts the parked run has one queue row and **zero** lease rows.

### 3.6 Prometheus series that exist before they happen

```go
for _, status := range []string{"completed", "failed", "cancelled", "needs_review"} {
	m.RunsFinished.WithLabelValues(status)
}
```

A Prometheus `*Vec` exposes **nothing** until a label combination is first
observed. Without this, the "failed runs" panel reads *no data* until the first
failure — which looks identical to a broken scrape, and is exactly when someone
is staring at the dashboard trying to tell the difference.

Only fixed-cardinality labels are pre-created. Model and provider names are open
sets.

---

## 4. New terms

| Term | One sentence |
|---|---|
| Token bucket | Rate-limit algorithm: tokens refill steadily, each action spends one, empty means wait. |
| Capacity vs refill rate | Capacity is the burst allowance; rate is the sustained speed. |
| Lazy refill | Computing how many tokens *would* have been added, instead of a timer adding them. |
| Lua script (Redis) | A program Redis runs atomically, without interleaving other commands. |
| Circuit breaker | Stops calling a failing service for a while, then tests it with one call. |
| Half-open | The probe state: exactly one call is admitted to test recovery. |
| Fail open / fail closed | What a safety component does when it itself breaks: allow everything, or block everything. |
| Micro-dollar | One millionth of a dollar; the unit money is counted in here. |
| Histogram (Prometheus) | A metric bucketing observations so quantiles can be computed. |
| Quantile / p99 | The value 99% of observations beat; shows the tail an average hides. |
| Cardinality | How many distinct label combinations a metric has. |
| Gauge vs counter | A gauge goes up and down; a counter only goes up. |

---

## 5. Decisions made here

In [`../decisions.md`](../decisions.md):

- Money in micro-dollars, not cents
- Payload v2 with v1 still readable
- An unpriced model is an error, not a zero
- Rate limits fail closed; the breaker fails open
- Refusals park the run rather than sleeping
- Breaker checked before the rate limit
- Fixed-cardinality metric series pre-created

---

## 6. Struggles

### The mock was more generous than reality

**Symptom.** The cost budget had never worked. `cents_used` was always zero.

**Cause, in three independent layers.** The Anthropic adapter never set
`Response.Cents` — the API returns token counts, not cost. Nothing in the
interpreter ever *checked* `BudgetCents` either. And the column could not have
held the value anyway, since a real call costs a fraction of a cent.

**Why no test caught it.** The mock provider set `Cents: 1` by hand. Every test
saw plausible accounting. **The fake supplied a value the real thing never
does** — so the tests confirmed the mock's behaviour, not Anchor's.

**Fix.** `Cents` was removed from `provider.Response` entirely. There is now
exactly one way to obtain cost — derive it from token counts — so a mock cannot
invent one.

**What it taught.** When a mock is more capable than the real implementation,
tests stop describing the system. Worth checking directly: *what does the real
adapter populate that the fake does not, and vice versa?*

### A feature that looked implemented from outside

`budget_cents` was accepted by the API, journaled, folded, written to the
projection, and returned by `GET /v1/runs/{id}`. From the outside it looked
finished. It did nothing.

That is worse than a missing feature — a missing one is obvious. Any field
accepted at an API boundary should have a test that proves it *changes
behaviour*, not merely that it round-trips.

### Two tests that asserted the wrong stimulus

The breaker's worker-level test used a 400ms cooldown and then asserted no calls
happened "while open" — but with a 3-second drain the breaker correctly went
half-open and admitted probes. The behaviour was right; the assertion assumed
"open" meant closed-to-all-traffic indefinitely. Fixed by using a long cooldown
and leaving the state machine to the `ratelimit` tests, where it belongs.

Same family as M3's chaos test and M4's memory test: **the check existed and
tested something adjacent to the claim.**

---

## 7. Checkpoint quiz check

The five M5 questions still match the code. Notes:

- **Q4 — "Why does the breaker fail open but budgets fail closed?"** The
  published key is right. Add the rate limiter: it fails **closed** too, with
  the breaker the only one that fails open, because it is the only one that is
  an optimisation rather than a promise.
- **Q5 — "When the rate limit is hit, why reschedule instead of sleeping?"** Add
  the second reason: a sleeping goroutine also holds a **lease**, which can
  expire mid-sleep and let another worker claim a run this one still believes
  it owns.

**Added questions:**

6. Why is money counted in micro-dollars rather than cents?
   *Key: a short real call costs about $0.003, which is zero in integer cents,
   so a cents budget rounds every run to nothing and can never fire — worse than
   no budget, because the operator believes they have one.*
7. The price table has no entry for a model and the run has a spending budget.
   What happens, and why not just charge zero?
   *Key: the run is refused. Charging zero means the budget silently passes over
   that model, so the operator believes a cap is in force while nothing counts
   against it.*
8. Why does a Prometheus `*Vec` need its label combinations pre-created?
   *Key: a Vec exposes nothing until a combination is observed, so a panel reads
   "no data" rather than 0 — indistinguishable from a broken scrape, at exactly
   the moment someone needs to tell them apart.*
