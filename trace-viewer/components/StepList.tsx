"use client";

import { useState } from "react";
import type { Step } from "@anchor/sdk";
import { formatMicros } from "@anchor/sdk";
import { StatusBadge } from "./StatusBadge";

/**
 * The step list, with a drawer showing a step's full detail.
 *
 * A Client Component because it holds the open/closed state of the drawer.
 * The steps themselves arrive as props from the server.
 */
export function StepList({ steps }: { steps: Step[] }) {
  const [open, setOpen] = useState<number | null>(null);

  if (steps.length === 0) {
    return <p className="text-sm text-neutral-600">No steps yet.</p>;
  }

  // The slowest step sets the bar scale, so durations are comparable at a
  // glance rather than every bar being full width.
  const durations = steps.map(durationMs);
  const slowest = Math.max(...durations, 1);

  return (
    <div className="space-y-1">
      {steps.map((step, i) => {
        const ms = durations[i] ?? 0;
        const isOpen = open === step.step_index;
        return (
          <div key={step.step_index} className="rounded border border-neutral-800">
            <button
              onClick={() => setOpen(isOpen ? null : step.step_index)}
              className="flex w-full items-center gap-3 px-3 py-2 text-left hover:bg-neutral-900/50"
            >
              <span className="w-6 text-right font-mono text-xs text-neutral-600">
                {step.step_index}
              </span>
              <span
                className={`w-12 shrink-0 rounded px-1.5 py-0.5 text-center text-[10px] uppercase ${
                  step.kind === "model" ? "bg-sky-950 text-sky-300" : "bg-teal-950 text-teal-300"
                }`}
              >
                {step.kind}
              </span>
              <span className="truncate text-sm">{step.name}</span>
              {step.attempt > 0 && (
                <span className="shrink-0 rounded bg-amber-950 px-1.5 py-0.5 text-[10px] text-amber-300">
                  attempt {step.attempt + 1}
                </span>
              )}
              <StatusBadge status={step.status} />

              {/* Duration bar, scaled against the slowest step. */}
              <span className="ml-auto flex items-center gap-2">
                <span className="hidden h-1.5 w-32 overflow-hidden rounded bg-neutral-800 sm:block">
                  <span
                    className="block h-full bg-neutral-600"
                    style={{ width: `${Math.max(2, (ms / slowest) * 100)}%` }}
                  />
                </span>
                <span className="w-16 shrink-0 text-right font-mono text-xs text-neutral-500">
                  {ms > 0 ? `${ms}ms` : "—"}
                </span>
              </span>
            </button>

            {isOpen && (
              <div className="space-y-3 border-t border-neutral-800 px-3 py-3 text-xs">
                <Field label="Idempotency key" mono>{step.idempotency_key}</Field>
                <Field label="Node">{step.node_id || "—"}</Field>
                <Field label="Tokens">{`${step.tokens}`}</Field>
                <Field label="Cost">{formatMicros(step.cost_micros)}</Field>
                <Field label="Random seed" mono>{`${step.rand_seed}`}</Field>
                {step.retry_delays_ms && step.retry_delays_ms.length > 0 && (
                  <Field label="Retry delays">{step.retry_delays_ms.join("ms, ") + "ms"}</Field>
                )}
                {step.error && (
                  <div>
                    <div className="mb-1 text-neutral-500">Error</div>
                    <pre className="overflow-x-auto rounded bg-red-950/40 p-2 text-red-300">
                      {step.error}
                    </pre>
                  </div>
                )}
                {step.model_response !== undefined && (
                  <Json label="Model response" value={step.model_response} />
                )}
                {step.tool_output !== undefined && <Json label="Tool output" value={step.tool_output} />}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function durationMs(step: Step): number {
  if (!step.started_at || !step.finished_at) return 0;
  return new Date(step.finished_at).getTime() - new Date(step.started_at).getTime();
}

function Field({ label, children, mono }: { label: string; children: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex gap-3">
      <span className="w-32 shrink-0 text-neutral-500">{label}</span>
      <span className={mono ? "break-all font-mono text-neutral-300" : "text-neutral-300"}>
        {children}
      </span>
    </div>
  );
}

function Json({ label, value }: { label: string; value: unknown }) {
  return (
    <div>
      <div className="mb-1 text-neutral-500">{label}</div>
      {/* Wide content scrolls inside its own box rather than widening the page. */}
      <pre className="max-h-64 overflow-auto rounded bg-neutral-900 p-2 font-mono text-neutral-300">
        {JSON.stringify(value, null, 2)}
      </pre>
    </div>
  );
}
