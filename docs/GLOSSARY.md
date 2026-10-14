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
