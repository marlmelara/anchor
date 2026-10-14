# M4 — Sandbox and real provider

> Study material for the M4 checkpoint quiz. Every code block is the real code.

---

## 1. Plain-English summary

Tools are the part of an agent that touches the outside world, and a tool is
code Anchor did not write. So every tool call now runs inside its own **Docker
container** — a process with its own filesystem and its own limits, thrown away
immediately afterwards.

The container gets no network unless the tool asked for it, a memory ceiling, a
fraction of a CPU, a filesystem it cannot write to, a non-root user, and a hard
time limit. Whatever the tool prints is checked against the shape the tool
promised before Anchor believes it.

The model call is also real now. The same interface the mock satisfies is
satisfied by an Anthropic adapter, so the worker code does not change at all —
only which object it was handed.

*(140 words)*

---

## 2. Data flow: one sandboxed tool call

1. **`internal/worker/interpret.go` → `toolStep`** reserves the idempotency key
   and journals `StepScheduled`, exactly as before. Nothing about sandboxing
   changed the durability path.

2. **`tool.ValidatingRunner.Run`** checks the input against the tool's
   `InputSchema` **first**. A model that invented an argument is caught here,
   before a container is started, so a bad call costs nothing.

3. **`sandbox.Docker.Run`** creates the container with every limit applied,
   attaches to it, writes the input JSON to stdin, closes stdin, and starts it.

4. The process reads stdin to EOF, does its work, writes JSON to stdout, exits.
   `stdcopy.StdCopy` demultiplexes Docker's combined stream into stdout and
   stderr, bounded so a runaway tool cannot exhaust the worker's memory.

5. **`ContainerWait`** returns the exit code, or the run context's deadline
   fires first and the call returns a `TimeoutError`.

6. **The deferred removal runs on every path out**, using a *fresh* context.

7. **Back in `ValidatingRunner`**, a zero exit means the output is checked
   against `OutputSchema`. A mismatch is a `SchemaError`, which the interpreter
   turns into `StepFailed`.

---

## 3. Annotated code

### 3.1 The limits — `internal/sandbox/docker.go`

```go
&container.HostConfig{
	NetworkMode:    container.NetworkMode(d.networkFor(def)),
	ReadonlyRootfs: true,
	CapDrop:        []string{"ALL"},
	SecurityOpt:    []string{"no-new-privileges"},
	Resources: container.Resources{
		Memory:    d.memoryFor(def),
		NanoCPUs:  d.cpuFor(def),
		PidsLimit: &d.cfg.PidsLimit,
	},
	AutoRemove: false,
}
```

| Setting | What it stops |
|---|---|
| `NetworkMode: "none"` | Data leaving the host. Default deny: a tool that forgot to declare egress fails loudly rather than silently reaching everything the worker can. |
| `ReadonlyRootfs` | A tool modifying its own image or leaving files behind. |
| `CapDrop: ALL` | Linux capabilities — the special privileges root normally holds. Even root inside the container keeps none. |
| `no-new-privileges` | Gaining privilege through a setuid binary. |
| `Memory` | One tool starving the host. Exceeding it means the kernel's OOM killer, which shows up as **exit 137** (128 + SIGKILL). |
| `NanoCPUs` | One tool slowing every other run. |
| `PidsLimit` | A fork bomb. |
| `User: "65534:65534"` | Acting as root even inside its own namespace. |

`AutoRemove: false` is deliberate — removal is explicit below so a failure to
remove is *visible* rather than silently delegated to the daemon.

### 3.2 The cleanup context — the subtle one

```go
defer func() {
	removeCtx, removeCancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer removeCancel()
	if err := d.cli.ContainerRemove(removeCtx, created.ID, ...); err != nil {
		d.cfg.Logf("remove container %s: %v", created.ID[:12], err)
	}
}()
```

`context.WithoutCancel(ctx)` strips the cancellation while keeping the context's
values. Why it matters: when a tool times out, `runCtx` is **already cancelled**.
A cleanup call made on a cancelled context fails instantly — and the container
leaks, in precisely the case where cleanup matters most.

This is the sandbox's version of a lesson the worker already learned with lease
release: *the path that cleans up after a failure must not depend on the thing
that failed.*

### 3.3 Default-deny networking

```go
func (d *Docker) networkFor(def tool.Definition) string {
	if def.Egress && d.cfg.AllowEgress {
		return "bridge"
	}
	return "none"
}
```

Two gates, both of which must open. The tool declares it needs egress **and**
the deployment permits egress at all. A deployment can refuse network access
fleet-wide regardless of what individual tools ask for — which is what you want
when the tools are not all written by you.

### 3.4 One interface, three implementations — `internal/provider`

```go
type Provider interface {
	Complete(ctx context.Context, req Request) (Response, error)
	Name() string
}
```

In Go an interface is satisfied **implicitly**: any type with these methods
fits, with no `implements` keyword. Python's closest equivalent is
`typing.Protocol`.

Three types satisfy it, and the worker cannot tell them apart:

| Implementation | Used for |
|---|---|
| `Mock` | Tests (deterministic, scriptable) and benchmarks (fixed latency, so numbers describe Anchor rather than the model) |
| `Anthropic` | Real runs |
| `ReplayProvider` (M6) | Replays — answers from the journal instead of the network |

This is **dependency injection**: the worker receives its provider rather than
building one. It is the reason M6 is a new type rather than a rewrite.

### 3.5 Classifying provider errors

```go
func classify(err error) error {
	var apiErr *anthropic.Error
	if !errors.As(err, &apiErr) {
		// No HTTP status: connection failure, DNS, timeout. All worth retrying.
		return &RetryableError{Err: err}
	}
	switch {
	case apiErr.StatusCode == http.StatusTooManyRequests,
		apiErr.StatusCode == http.StatusRequestTimeout,
		apiErr.StatusCode >= 500:
		return &RetryableError{Status: apiErr.StatusCode, Err: err}
	default:
		return err
	}
}
```

- `errors.As` checks whether an error, or anything it *wraps*, is a given type.
  Python: `except SomeError as e`, but it unwraps a chain.
- Classifying **where the error happens** is what lets the retry logic decide
  from a type later, rather than matching substrings three layers up.
- 4xx is terminal: retrying sends the same wrong request and spends the budget
  to get the same answer.

**A refusal is not an error.** It arrives as HTTP 200 with
`stop_reason: "refusal"`. Treating it as success would feed an empty response
into the next step, so `fromSDKMessage` turns it into a `RefusalError` — and a
terminal one, because the same request will be declined again.

---

## 4. New terms

| Term | One sentence |
|---|---|
| Sandbox | A restricted environment limiting what code can reach. |
| Linux capability | One of the individual privileges root normally holds, droppable separately. |
| OOM killer | The kernel killing a process that exceeds its memory limit; shows as exit 137. |
| Exit 137 | 128 + signal 9 (SIGKILL) — conventionally the OOM killer. |
| `no-new-privileges` | A flag stopping a process gaining privilege through setuid. |
| PID limit | A cap on processes in a container, which stops a fork bomb. |
| Read-only root filesystem | The container cannot write to its own image. |
| Orphan container | One whose creator died before removing it. |
| JSON Schema | A declarative description of a JSON value's shape. |
| Schema validation | Checking data matches a declared shape before using it. |
| Default deny | Refusing by default and permitting only what is explicitly declared. |
| Dependency injection | Passing a component its dependencies so they can be swapped. |
| stdcopy | Docker's multiplexed stream format carrying stdout and stderr on one connection. |

---

## 5. Decisions made here

In [`../decisions.md`](../decisions.md):

- Docker containers rather than gVisor, Firecracker, or in-process execution
- Default-deny networking with two gates
- Schema validation wraps the Runner rather than living inside each one
- The official Anthropic SDK rather than hand-rolled HTTP
- The live API check is a script, not a test

---

## 6. Struggles

### A memory-limit test that proved nothing

**Symptom.** The memory cap test failed: a tool allocating "1 GB" under a 64 MB
cap exited cleanly.

**Cause.** The test command was
`head -c 1000000000 /dev/zero | tail -c 1`. That **streams** a gigabyte through
a pipe and buffers one byte. It never allocates anything. The limit was working
perfectly; the test was not testing it.

**Fix.** `dd if=/dev/zero of=/dev/null bs=1G count=1`, which allocates a real
one-gigabyte buffer, invoked **directly as the container's command** rather than
through `sh -c`. A shell wrapper survives the kill of its child and exits 0
itself, which would hide the OOM behind a successful container exit.

**What it taught.** Same shape as M3's chaos test that passed after one kill: a
test asserting the right thing about the wrong stimulus is worse than no test,
because it reads as evidence. Before trusting a limit test, confirm the stimulus
actually does the thing — here, by running the command under `docker run
--memory 64m` directly and checking for exit 137.

### `[]byte` and `json.RawMessage` are not the same type to an interface

`sandbox.Docker.Run` took `[]byte`; `tool.Runner` declares `json.RawMessage`.
They have the same underlying type, so assignment between them is fine — but Go
checks interface satisfaction on the *declared* types, so `*Docker` did not
implement `Runner`. The compiler said so precisely. Worth remembering as the
general rule: identical underlying types do not make identical method
signatures.

---

## 7. Checkpoint quiz check

The five M4 questions still match the code. Notes:

- **Q2 — "Why is `ContainerRemove` called with a fresh context?"** The published
  answer is right. Say the consequence out loud: without it the container leaks
  in exactly the timeout case, which is the case cleanup exists for.
- **Q5 — "Why keep the mock provider after adding the real one?"** Add the
  benchmark reason: with a real provider the numbers measure the model's
  latency, not Anchor's, so they would describe Anthropic rather than this
  project.

**Added questions:**

6. A tool declares `egress: true`. Does it get network access?
   *Key: only if the deployment also permits egress. Two gates, both must open,
   so a deployment can refuse network access fleet-wide regardless of what
   individual tools ask for.*
7. A tool exits 0 but prints output that does not match its declared schema.
   What happens, and why is that better than passing it through?
   *Key: a `SchemaError`, which becomes `StepFailed` with a readable reason.
   Passing it through means malformed data reaches the next model call, and the
   run fails three steps later for reasons that no longer point at the cause.*
8. Why is the real-API check a script rather than a test?
   *Key: it spends real money and needs a credential. CI must be runnable by
   anyone on any branch without either. The wire format is covered by tests
   against a local server; what the script adds is confirmation that the real
   API agrees with that format.*
