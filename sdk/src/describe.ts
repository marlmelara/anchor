import type { RunEvent } from "./types.ts";

/**
 * A one-line human description of an event.
 *
 * Shared by the viewer's timeline and anything logging a stream, so the wording
 * is identical everywhere. More importantly it is where the discriminated union
 * earns its keep: inside each `case`, TypeScript has NARROWED `e` to that one
 * variant, so `e.payload.tokens` type-checks under `ModelCallCompleted` and
 * nowhere else.
 *
 * The consequence is that a backend change to an event shape breaks `npm run
 * typecheck` rather than the live page.
 */
export function describeEvent(e: RunEvent): string {
  switch (e.type) {
    case "RunSubmitted":
      return `submitted to ${e.payload.agent_name} v${e.payload.agent_version}`;
    case "RunStarted":
      return e.payload.resumed
        ? `resumed by ${e.payload.worker_id} after another worker stopped`
        : `started by ${e.payload.worker_id}`;
    case "StepScheduled":
      return `step ${e.payload.step_index} scheduled: ${e.payload.kind} ${e.payload.name}`;
    case "ModelCallStarted":
      return `step ${e.payload.step_index} calling ${e.payload.model ?? "model"}${
        e.payload.attempt > 0 ? ` (attempt ${e.payload.attempt + 1})` : ""
      }`;
    case "ModelCallCompleted": {
      const tokens = e.payload.prompt_tokens + e.payload.completion_tokens;
      const cost = e.payload.cost_unknown
        ? "cost unknown"
        : formatMicros(e.payload.cost_micros ?? 0);
      return `step ${e.payload.step_index} answered: ${tokens} tokens, ${cost}`;
    }
    case "ToolCallStarted":
      return `step ${e.payload.step_index} running tool ${e.payload.tool}`;
    case "ToolCallCompleted":
      return `step ${e.payload.step_index} tool exited ${e.payload.exit_code}`;
    case "StepFailed":
      return `step ${e.payload.step_index} failed${
        e.payload.retryable ? " (retryable)" : ""
      }: ${e.payload.error}`;
    case "RetryScheduled":
      return `step ${e.payload.step_index} retrying in ${e.payload.delay_ms}ms (attempt ${
        e.payload.attempt + 1
      })`;
    case "BudgetExceeded":
      return `${e.payload.resource} budget exceeded: limit ${e.payload.limit}, projected ${e.payload.projected}`;
    case "RunCancelled":
      return `cancelled: ${e.payload.reason}`;
    case "RunCompleted":
      return "completed";
    case "RunFailed":
      return `failed: ${e.payload.error}`;
    case "RunNeedsReview":
      return `halted for review: ${e.payload.reason}`;
  }
}

/**
 * Renders micro-dollars for people.
 *
 * Money is counted in micro-dollars because a single short model call costs a
 * fraction of a cent, which integer cents round to zero.
 */
export function formatMicros(micros: number): string {
  if (micros === 0) return "$0.00";
  const dollars = micros / 1_000_000;
  if (dollars < 0.01) return `$${dollars.toFixed(6).replace(/0+$/, "")}`;
  return `$${dollars.toFixed(2)}`;
}

/** Which events belong to a given step, for grouping a timeline. */
export function stepIndexOf(e: RunEvent): number | null {
  const payload = e.payload as { step_index?: number };
  return typeof payload.step_index === "number" ? payload.step_index : null;
}
