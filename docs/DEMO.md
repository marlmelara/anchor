# The demo video

Infrastructure demos badly. A recruiter opening a trace viewer sees a list and
feels nothing. The demo that lands with engineers is the **crash demo**: kill a
worker on camera and watch the run continue.

Ninety seconds, captions only, no narration. It should be the first link in the
README.

## Setup

Two panes side by side.

- **Left:** the trace viewer on a run detail page — `make viewer-dev`, then open
  `http://localhost:3000`
- **Right:** a terminal running the worker pool

```bash
# terminal 1
make up && make migrate
go run ./cmd/anchor-server

# terminal 2 — the pane on camera
go run ./cmd/anchor-worker -pool 2 -id demo-1 -lease 3s -heartbeat 1s -reap 500ms -mock-latency 2s
go run ./cmd/anchor-worker -pool 2 -id demo-2 -lease 3s -heartbeat 1s -reap 500ms -mock-latency 2s
```

A 2-second model call is deliberate: the steps have to be slow enough to see,
and a run has to still be in flight when the kill lands.

## The shots

**1. Start a multi-step run (0:00–0:15)**

```bash
go run ./cmd/anchorctl submit -agent research -input '{"topic":"durable execution"}'
```

Open it in the viewer. Let it get three or four steps in so the timeline is
visibly filling — the live indicator should read `live`.

*Caption: "A run in flight. Every step is written to Postgres before it happens."*

**2. Kill the worker on camera (0:15–0:30)**

```bash
kill -9 $(pgrep -f 'anchor-worker -pool 2 -id demo-1')
```

Show the `kill -9` typed out. `-9` matters: `SIGKILL` gives the process no
chance to clean up, which is the whole point.

*Caption: "kill -9. No cleanup, no warning, no chance to save anything."*

**3. The stall (0:30–0:45)**

The timeline stops. Nothing moves for the lease duration.

*Caption: "The run stalls. Its lease has an expiry, and a dead worker stops
renewing it."*

This pause is the most important shot in the video. Do not cut it short — the
viewer needs to feel that something broke.

**4. The takeover (0:45–1:00)**

The timeline starts moving again. The next event is a `RunStarted` carrying
`resumed: true`, from the *other* worker.

*Caption: "Another worker folds the journal and continues from the exact next
step. No work repeated, none lost."*

Point at the step list: the completed steps are untouched and the run picks up
at the first unfinished one.

**5. The effect count (1:00–1:15)**

```bash
make chaos
```

Or, if the chaos run is already done, show its summary:

```
worker kills          7
runs resumed          5
runs completed        40 / 40
effects (once each)   40
tool executions       43
retried after a kill  3
```

*Caption: "Three tool calls were retried after a kill. Every effect still
applied exactly once."*

That distinction is the claim: **at-least-once delivery plus idempotent effects
is effectively-once.** Executions exceed effects; effects equal runs.

**6. Replay (1:15–1:30)**

Click **Replay this run** in the viewer. Wait for the diff.

*Caption: "Replayed against its recorded responses. Same events, same order,
same result."*

End on the green line: **"The replay reproduced this run exactly."**

## What to cut if it runs long

The replay shot can stand alone as a second, shorter video. The crash sequence
cannot be shortened — the stall and the takeover are the whole story, and
cutting the pause removes the tension that makes the recovery mean anything.

## What not to do

- **No feature tour.** Nobody watches a UI walkthrough.
- **No voiceover.** Captions are readable on mute, which is how most of these
  are watched.
- **Do not fake the kill.** Use a real `kill -9` on a real process, on camera.
  An engineer can tell, and the whole point is that it is real.
