# M7 — Trace viewer and SDK

> Study material for the M7 checkpoint quiz. The frontend milestone, explained
> at the same depth as the Go.

---

## 1. Plain-English summary

Two pieces, both TypeScript.

The **SDK** is what someone writes code against. It builds an agent graph as
JSON and submits it. It never runs agent logic — the Go worker does that — so
the SDK is really a typed way to produce data and read results back.

The **trace viewer** is a Next.js website that reads the journal. It lists runs,
shows one run's steps on a timeline, opens a drawer with the exact prompt and
response for any step, and has a Replay button that re-runs it and shows the two
event sequences side by side.

The viewer fetches on the **server** wherever it can, so pages arrive as
finished HTML and the API key never reaches the browser. Only the parts that
genuinely need interactivity run in the browser.

*(147 words)*

---

## 2. Data flow: loading a run page

1. **The browser requests `/runs/<id>`.**

2. **`app/runs/[id]/page.tsx` runs on the server.** No `"use client"`, so this
   is a Server Component: it can `await` data directly while rendering.

3. It calls `lib/anchor.ts`, which fetches `anchor-server` with the API key from
   `process.env`. **That code never ships to the browser**, which is why the key
   stays secret.

4. **Finished HTML goes to the browser** — the run, its steps, its events,
   already rendered. No spinner, no client-side fetch.

5. **Three small Client Components hydrate**: `StepList` (drawer state),
   `ReplayPanel` (button and diff), `LiveTimeline` (SSE).

6. **If the run is still going**, `LiveTimeline` opens an `EventSource` to
   `/api/runs/<id>/stream` — a Route Handler on the Next server, which proxies
   `anchor-server`'s SSE stream. The proxy exists so the browser never needs the
   key.

7. **Each event arrives** and is appended to React state, which re-renders the
   list.

---

## 3. Annotated code

### 3.1 Server Component vs Client Component

```tsx
// app/runs/page.tsx  — no "use client"
export default async function RunsPage() {
  const runs = await listRuns(200);   // runs on the SERVER
  return <table>...</table>;
}
```

```tsx
// components/LiveTimeline.tsx
"use client";
export function LiveTimeline({ runId, initial, live }: Props) {
  const [events, setEvents] = useState<RunEvent[]>(initial);
  useEffect(() => { /* EventSource */ }, [runId, live, initial]);
}
```

The rule: **Server Components are the default**, and a component becomes a
Client Component only when it needs state, effects, event handlers, or a browser
API.

Three consequences worth being able to state:

- **Secrets stay server-side.** `process.env.ANCHOR_API_KEY` read in a Server
  Component never reaches the browser. The same line in a Client Component
  publishes it to every visitor.
- **Faster first paint.** The server already has the data, so the HTML is
  complete. A client-side fetch would mean HTML → JavaScript → fetch → render.
- **Less JavaScript.** Only client components ship. Keeping them small is why
  the run detail page is ~3.4 kB rather than the whole table and timeline.

### 3.2 Why the key needs a proxy

```ts
// app/api/runs/[id]/stream/route.ts
export async function GET(request, { params }) {
  const upstream = await fetch(streamUrl(id, after), {
    headers: anchorHeaders(),      // the key, server-side
    signal: request.signal,        // browser disconnects -> upstream aborts
  });
  return new Response(upstream.body, { headers: { "content-type": "text/event-stream" } });
}
```

`EventSource` cannot send an `Authorization` header — it is a deliberately
minimal API. So either the key goes in the URL (where it lands in logs and
browser history) or the browser talks to something that already has it. The
Route Handler is that something.

`signal: request.signal` matters: when the tab closes, the upstream request is
aborted too, so a closed tab does not leave a stream open on `anchor-server`.

### 3.3 The SSE client, and the mistake everyone makes first

```tsx
const types = ["RunSubmitted", "RunStarted", /* ...all 14 */];
for (const type of types) source.addEventListener(type, onEvent);
```

**A named SSE event never reaches `onmessage`.** The server sends
`event: ModelCallCompleted`, and only a listener registered for that exact name
fires. The generic handler sees nothing, the page stays empty, and the server
looks broken while it is working perfectly. This is the classic first-SSE bug.

```tsx
setEvents((prev) =>
  prev.some((e) => e.seq === event.seq) ? prev : [...prev, event]
);
```

Two things in three lines:

- **The functional update form** `setEvents(prev => ...)` always receives the
  latest state. The alternative, `setEvents([...events, event])`, captures
  `events` from the render the effect was created in — a stale closure, and a
  classic React bug.
- **`[...prev, event]`, not `prev.push(event)`.** React compares state by
  reference. Mutating the existing array leaves the reference unchanged, so no
  re-render happens.
- **Deduping by `seq`** because `EventSource` reconnects on its own after a
  network blip and resends from the last id. Cheaper than making the server
  guarantee exactly-once delivery to a browser.

```tsx
return () => {
  for (const type of types) source.removeEventListener(type, onEvent);
  source.close();
};
```

The **cleanup function**. Without it, navigating away leaves the connection
open, and a browser allows only about six per origin — so a few navigations and
the page silently stops updating.

### 3.4 The discriminated union earns its keep

```ts
export type RunEvent =
  | { seq: number; type: "RunStarted"; payload: RunStartedPayload }
  | { seq: number; type: "ModelCallCompleted"; payload: ModelCallCompletedPayload }
  | /* ...14 variants */;

export function describeEvent(e: RunEvent): string {
  switch (e.type) {
    case "ModelCallCompleted": {
      const tokens = e.payload.prompt_tokens + e.payload.completion_tokens;  // type-checks
      ...
    }
  }
}
```

Inside `case "ModelCallCompleted"`, TypeScript has **narrowed** `e` to that one
variant, so `e.payload.prompt_tokens` is known to exist. Reading it in another
branch is a compile error.

Python's nearest equivalent is a `Union` of `TypedDict`s with `Literal` type
fields.

**Why it matters here:** the SDK and the viewer import the same types, so a
backend change to an event shape breaks `npm run typecheck` rather than the live
page. The failure moves from a user's browser to a build.

### 3.5 `defineTool` refuses a default

```ts
if (typeof def.idempotent !== "boolean") {
  throw new Error(`defineTool: ${def.name} must declare idempotent explicitly`);
}
```

Neither default is safe. `true` would silently permit retrying an effect that
must not repeat — a double charge. `false` would halt runs that were fine, as
`needs_review`, waiting for a human who did not need to be involved.

So the author decides, and that decision is exactly what Anchor acts on when a
worker dies mid-effect.

This is also the bug the chaos suite found in Anchor's own test tool: it
declared `idempotent: true` and was not, so an effect applied twice and looked
like a durability failure when the retry contract was working correctly.

---

## 4. New terms

| Term | One sentence |
|---|---|
| Server Component | A React component rendered only on the server; can use secrets, ships HTML. |
| Client Component | A component shipped to the browser; needed for state, effects, event handlers. |
| `"use client"` | The directive marking a module as a Client Component. |
| Route Handler | A server endpoint inside a Next app, at `app/api/.../route.ts`. |
| Hydration | React attaching interactivity to HTML the server already rendered. |
| Hook | A function like `useState` or `useEffect` adding state or effects to a component. |
| Functional update | `setState(prev => next)`, which always sees the latest state. |
| Stale closure | A callback holding values captured from an earlier render. |
| Key (React) | A stable identity per list item so React updates only what changed. |
| Props | Inputs passed into a component. |
| SSE / `EventSource` | One-way server-to-browser streaming over plain HTTP, with automatic reconnection. |
| Discriminated union | A set of shapes told apart by one literal field. |
| Narrowing | TypeScript inferring a precise variant inside a branch. |
| Tailwind | Utility CSS classes, each applying one small style rule. |

---

## 5. Decisions made here

In [`../decisions.md`](../decisions.md):

- Server Components fetch; client components stay small
- The viewer proxies SSE rather than giving the browser a key
- SSE over WebSockets
- The SDK validates graphs client-side as well as server-side
- `defineTool` requires `idempotent` explicitly
- The viewer computes its own diff rather than trusting a server verdict

---

## 6. Struggles

### A test that could not have failed

Verifying "a run appears in the viewer and updates live" kept showing a
**completed** run, because a mock run finishes in under a second and the page
took two seconds to load. The screenshot looked right — a populated page — but
proved nothing about live updates, only that a finished run renders.

The fix was to stop the worker, queue a run so it stayed `pending`, open the
page (which showed `connecting` and one event), and *then* start a worker. The
page went from 1 event to 9 with no reload.

Same shape as M3's chaos test passing after one kill and M4's memory test whose
command never allocated: **the observation was real, the stimulus was wrong.**

### `pkill` matching the wrong thing

Two attempts at the above failed because `pkill -f 'anchor-worker'` matched
nothing — the binary had been built to `/tmp/aw`, so the process name was `aw`.
The kill reported success, the worker kept running, and the run kept completing.

Worth remembering: a `pkill` that matches nothing exits 1 but looks like it
worked when the output is discarded. Verify with `pgrep` after.

---

## 7. Checkpoint quiz check

The five M7 questions still match the code. Notes:

- **Q1 — "When must a component be a Client Component?"** Add the consequence:
  a Client Component reading `process.env.ANCHOR_API_KEY` publishes it in the
  page source.
- **Q3 — "What does the `useEffect` cleanup do in `LiveTimeline`?"** Say the
  limit out loud: a browser allows about six connections per origin, so leaking
  them stops updates after a few navigations.

**Added questions:**

6. The server sends `event: ModelCallCompleted` and the page stays empty. Why?
   *Key: a named SSE event never reaches `onmessage`. Only a listener registered
   for that exact name fires.*
7. Why does the viewer proxy the SSE stream instead of connecting to
   `anchor-server` directly?
   *Key: `EventSource` cannot send an `Authorization` header, so the key would
   have to go in the URL — into logs and browser history. The Route Handler
   already has it, server-side.*
8. Why does `defineTool` refuse to default `idempotent`?
   *Key: neither default is safe. `true` permits retrying an effect that must
   not repeat; `false` halts runs that were fine. The author's declaration is
   what Anchor acts on when a worker dies mid-effect.*
