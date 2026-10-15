# Glossary

Every term used in Anchor's code and docs, in plain English. Defining 15 of
these in a sentence each is question 9 of the final exam.

Terms are added here as each milestone introduces them. The **M** column is the
milestone where it first appears, so revision can be scoped to what is built.

| Term | Plain-English meaning | Where it shows up | M |
|---|---|---|---|
| ACID | Atomic, Consistent, Isolated, Durable: the four guarantees of a database transaction | Every Postgres write | M1 |
| Ambiguous window | The moment between starting a side effect and recording it; after a crash there you cannot know whether it happened | Failure semantics | M1 |
| Append-only | A table rows may be inserted into but never updated or deleted | `run_events`, enforced by trigger | M1 |
| Atomic | All-or-nothing: either every part happens or none does | Transactions | M1 |
| Canonical JSON | One deterministic encoding of a value — keys sorted, whitespace removed — so equal values hash equally | `idem.CanonicalJSON` | M1 |
| Constraint | A rule the database enforces, such as `UNIQUE` or `NOT NULL` | `UNIQUE (run_id, seq)` | M1 |
| Deferrable constraint | A foreign key checked at `COMMIT` rather than at each statement | `steps_run_id_fkey`, so rebuild can delete and reinsert | M1 |
| Deterministic | Same inputs always produce the same outputs | The fold; replay | M1 |
| Durable execution | Running long workflows so progress survives crashes, by journaling each step | Anchor's whole purpose | M1 |
| Event | An immutable fact about something that happened | `run_events` rows | M1 |
| Event sourcing | Storing every change as an append-only list and computing state from it | The journal | M1 |
| Fold | Applying a function to each item in order, carrying state forward; Python's `functools.reduce` | `journal.Fold` | M1 |
| Foreign key | A constraint saying a column's value must exist in another table | Deliberately absent from `run_events` | M1 |
| Idempotency key | A unique id for an operation so repeats can be detected and skipped | `steps.idempotency_key` | M1 |
| Idempotent | Doing it twice has the same result as doing it once | Tool definitions | M1 |
| Index | A lookup structure, usually a B-tree, that makes `WHERE` and `ORDER BY` fast | `queue (priority, available_at)` | M1 |
| `jsonb` | Postgres type storing JSON in a binary, queryable form; normalises key order | Event payloads | M1 |
| Migration | A versioned schema change applied in order | `migrations/` | M1 |
| Optimistic concurrency | Assume no conflict, detect one at write time, back off | `UNIQUE (run_id, seq)` | M1 |
| Projection | A read-optimised table derived from events that can be rebuilt from them | `runs`, `steps` | M1 |
| Pure function | Output depends only on inputs; no clock, network, or randomness | The fold | M1 |
| Race condition | A bug where the outcome depends on the timing of concurrent operations | Two workers on one run | M1 |
| Sentinel error | A named error value callers compare against to branch on a specific cause | `ErrSeqConflict`, `ErrNotFound` | M1 |
| Source of truth | The one place whose data wins if copies disagree | `run_events` | M1 |
| Transaction | A group of database operations that succeed or fail together | Append plus projection | M1 |
| Trigger | A function the database runs automatically on insert, update or delete | Append-only enforcement | M1 |
| Upsert | `INSERT ... ON CONFLICT DO UPDATE` or `DO NOTHING` | Projection writes | M1 |
| VACUUM | Postgres reclaiming dead row versions left by updates and deletes | `queue`, which churns | M1 |

| Backpressure | Making producers slow down when consumers cannot keep up | 429 above the high-water mark | M2 |
| Cooperative cancellation | Cancelling by signalling and letting work stop at a safe point, not by killing it | `RunCancelled` | M2 |
| Exponential backoff | Waiting longer after each failed retry: 1s, 2s, 4s | Retries | M2 |
| `FOR UPDATE` | Locks the selected rows until the transaction ends | Claim query | M2 |
| Heartbeat | The periodic renewal that keeps a lease alive | Every 10s | M2 |
| High-water mark | The threshold above which new work is refused | Queue depth | M2 |
| Jitter | Random variation in backoff so clients do not retry in lockstep | Retries | M2 |
| Lease | Time-limited ownership that must be renewed, so it ends if the owner dies | `leases` table | M2 |
| Retryable vs terminal error | Retryable may succeed if tried again; terminal will not | Retry logic | M2 |
| `SKIP LOCKED` | Postgres option to step over rows other transactions have locked | Queue claims | M2 |
| Stateless service | Keeps no data between requests, so copies are interchangeable | `anchor-server` | M2 |
| Worker pool | A fixed number of workers each handling one unit of work at a time | `anchor-worker` | M2 |

| Chaos testing | Deliberately breaking things to prove recovery works | The chaos suite | M3 |
| CTE | A named subquery in SQL, written `WITH name AS (...)` | The reaper | M3 |
| Fault injection | Deliberately causing failures to test behaviour under them | `SIGKILL` loop | M3 |
| Graceful shutdown | Stopping new work, finishing current work, then exiting | `SIGTERM` handling | M3 |
| Lease expiry | Ownership ending automatically because it was not renewed | Crash recovery | M3 |
| Reaper | A background job cleaning up resources whose owner disappeared | `ReapExpiredLeases` | M3 |
| Rolling restart | Replacing processes one at a time so the service stays up | Deploy path | M3 |
| `SIGKILL` / `SIGTERM` | `SIGTERM` asks a process to stop and allows cleanup; `SIGKILL` stops it instantly with none | Chaos vs deploys | M3 |
| Takeover | Another worker claiming a run whose lease expired | `resumed: true` | M3 |
| Zombie worker | One that froze, lost its lease, then resumed believing it still owns the run | Lease safety | M3 |

| Default deny | Refusing by default and permitting only what is explicitly declared | Sandbox networking | M4 |
| Exit 137 | 128 + SIGKILL — conventionally the OOM killer | Memory cap enforcement | M4 |
| JSON Schema | A declarative description of a JSON value's shape | Tool input and output | M4 |
| Linux capability | One of the individual privileges root normally holds, droppable separately | `CapDrop: ALL` | M4 |
| `no-new-privileges` | A flag stopping a process gaining privilege through setuid | Sandbox hardening | M4 |
| OOM killer | The kernel killing a process that exceeds its memory limit | Memory caps | M4 |
| Orphan container | One whose creator died before removing it | `ReapOrphans` | M4 |
| PID limit | A cap on processes in a container, stopping a fork bomb | Sandbox hardening | M4 |
| Sandbox | A restricted environment limiting what code can reach | One container per tool call | M4 |
| Schema validation | Checking data matches a declared shape before using it | `ValidatingRunner` | M4 |
| stdcopy | Docker's multiplexed stream carrying stdout and stderr together | Reading tool output | M4 |

| Cardinality | How many distinct label combinations a metric has | Metric design | M5 |
| Circuit breaker | Stops calling a failing service, then tests it with one call | Per provider | M5 |
| Fail open / fail closed | What a safety component does when it itself breaks | Breaker vs limiter | M5 |
| Gauge vs counter | A gauge moves both ways; a counter only rises | Queue depth vs totals | M5 |
| Half-open | The probe state: exactly one call tests recovery | Breaker | M5 |
| Histogram | A metric bucketing observations so quantiles can be computed | Latencies | M5 |
| Lazy refill | Computing tokens that *would* have been added, rather than a timer | Token bucket | M5 |
| Lua script (Redis) | A program Redis runs atomically, with no interleaving | Bucket and breaker | M5 |
| Micro-dollar | One millionth of a dollar; the unit money is counted in | `micros_used` | M5 |
| Quantile / p99 | The value 99% of observations beat; shows the tail an average hides | Benchmarks | M5 |
| Token bucket | Tokens refill steadily, each action spends one, empty means wait | Rate limits | M5 |

| Divergence | The first point where a replay differs from its original | Replay verification | M6 |
| Divergence point | Where two runs parted, as opposed to everywhere they then differ | Replay report | M6 |
| Event signature | The comparable shape of an event: type, order, step, attempt | `replay.Signature` | M6 |
| Recording | Everything the original run observed, keyed by step and attempt | `replay.Recording` | M6 |
| Replay | Re-running a finished run against recorded inputs instead of live ones | M6 | M6 |
| Replay fidelity | The share of replayed runs reproducing their original exactly | The resume number | M6 |

## Go terms

| Term | Plain-English meaning | Python equivalent | M |
|---|---|---|---|
| Context | A value threaded through calls carrying cancellation, deadlines and request data | `asyncio.timeout()` plus task cancellation | M1 |
| `defer` | Runs a call when the function returns, however it returns | `finally` | M1 |
| Errors as values | Failures are returned, not raised; every call site checks | `try/except`, but explicit | M1 |
| `errors.As` | Checks whether an error, or anything it wraps, is a given *type* | `except SomeError as e` | M1 |
| `errors.Is` | Checks whether an error, or anything it wraps, *is* a specific value | `if e is SENTINEL` | M1 |
| Generics | Functions parameterised by type, written `func f[T any](...)` | `TypeVar` | M1 |
| Pointer receiver | A method on `*T` operates on the original value, not a copy | `self` | M1 |
| Slice | A growable view over an array, written `[]T` | `list` | M1 |
| Struct | A record type with named fields | `@dataclass` | M1 |
| Struct tag | Metadata on a field, e.g. `json:"run_id"`, controlling encoding | `Field(alias=...)` in Pydantic | M1 |
| Zero value | The default a type gets when uninitialised: `0`, `""`, `nil` | Roughly `None`, but typed | M1 |
| Typed nil | A nil pointer inside an interface, which makes the interface itself non-nil | Replay override wiring | M6 |
| Goroutine | A very lightweight thread Go manages; `go f()` starts one | `asyncio.create_task`, but truly parallel | M2 |
| Channel | A typed pipe between goroutines | `asyncio.Queue` | M2 |
| `select` | Waits until one of several channels is ready | `asyncio.wait(..., FIRST_COMPLETED)` | M2 |
| errgroup | A group of goroutines where `Wait` returns the first error | `asyncio.TaskGroup` | M2 |
| Interface | A set of method signatures; any type with them satisfies it, no declaration | `typing.Protocol` | M2 |
| Dependency injection | Passing a component its dependencies rather than having it build them | Same idea | M2 |
