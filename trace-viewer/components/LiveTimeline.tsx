"use client";

import { useEffect, useState } from "react";
import type { RunEvent } from "@anchor/sdk";
import { describeEvent } from "@anchor/sdk";

/**
 * The run's event timeline, updating live.
 *
 * "use client" is required: this holds state, runs an effect, and opens an
 * EventSource, none of which exist on the server. It receives the events the
 * server already had as props, so the page is complete on first paint and this
 * only adds what happens next.
 */
export function LiveTimeline({
  runId,
  initial,
  live,
}: {
  runId: string;
  initial: RunEvent[];
  live: boolean;
}) {
  const [events, setEvents] = useState<RunEvent[]>(initial);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    // A finished run will never produce another event, so opening a stream for
    // it would hold a connection to watch nothing happen.
    if (!live) return;

    const after = initial.at(-1)?.seq ?? 0;
    const source = new EventSource(`/api/runs/${runId}/stream?after=${after}`);

    const onEvent = (msg: MessageEvent<string>) => {
      const event = JSON.parse(msg.data) as RunEvent;
      setEvents((prev) =>
        // EventSource reconnects on its own after a network blip and resends
        // from the last id, so the same event can arrive twice. Deduping by
        // seq is cheaper than making the server guarantee exactly-once
        // delivery to a browser.
        prev.some((e) => e.seq === event.seq)
          ? prev
          : // A new array, not a push: React compares by reference, so
            // mutating prev in place would not re-render.
            [...prev, event],
      );
    };

    // Each event is sent with a NAME, and a named SSE event never reaches the
    // generic onmessage handler. Registering per type is the part most first
    // SSE implementations get wrong.
    const types = [
      "RunSubmitted", "RunStarted", "StepScheduled",
      "ModelCallStarted", "ModelCallCompleted",
      "ToolCallStarted", "ToolCallCompleted",
      "StepFailed", "RetryScheduled", "BudgetExceeded",
      "RunCancelled", "RunCompleted", "RunFailed", "RunNeedsReview",
    ];
    for (const type of types) source.addEventListener(type, onEvent as EventListener);

    source.addEventListener("open", () => setConnected(true));
    source.addEventListener("done", () => {
      setConnected(false);
      source.close();
    });
    source.onerror = () => setConnected(false);

    // The cleanup function. Without it, navigating away leaves the connection
    // open and a browser only allows a handful per origin.
    return () => {
      for (const type of types) source.removeEventListener(type, onEvent as EventListener);
      source.close();
    };
  }, [runId, live, initial]);

  return (
    <div>
      <div className="mb-3 flex items-center gap-2 text-sm text-neutral-400">
        <span>Timeline</span>
        {live && (
          <span className="flex items-center gap-1.5">
            <span
              className={`h-1.5 w-1.5 rounded-full ${connected ? "bg-emerald-400" : "bg-neutral-600"}`}
            />
            <span className="text-xs">{connected ? "live" : "connecting"}</span>
          </span>
        )}
        <span className="text-xs text-neutral-600">{events.length} events</span>
      </div>

      <ol className="space-y-1">
        {events.map((e) => (
          <li
            key={e.seq}
            className="flex items-baseline gap-3 rounded border border-neutral-800 bg-neutral-900/40 px-3 py-1.5"
          >
            <span className="w-8 shrink-0 text-right font-mono text-xs text-neutral-600">
              {e.seq}
            </span>
            <span className="w-44 shrink-0 font-mono text-xs text-sky-400">{e.type}</span>
            <span className="text-sm text-neutral-300">{describeEvent(e)}</span>
            <span className="ml-auto shrink-0 font-mono text-xs text-neutral-600">
              {e.created_at ? new Date(e.created_at).toLocaleTimeString() : ""}
            </span>
          </li>
        ))}
      </ol>
    </div>
  );
}
