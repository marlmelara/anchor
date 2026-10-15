import type { RunStatus, StepStatus } from "@anchor/sdk";

const colors: Record<string, string> = {
  completed: "bg-emerald-950 text-emerald-300",
  running: "bg-sky-950 text-sky-300",
  pending: "bg-neutral-800 text-neutral-400",
  scheduled: "bg-neutral-800 text-neutral-400",
  retrying: "bg-amber-950 text-amber-300",
  failed: "bg-red-950 text-red-300",
  cancelled: "bg-neutral-800 text-neutral-500",
  // needs_review is its own colour on purpose: it is the state where nobody
  // knows whether a side effect landed, and it needs a human.
  needs_review: "bg-orange-950 text-orange-300",
};

export function StatusBadge({ status }: { status: RunStatus | StepStatus | string }) {
  return (
    <span
      className={`rounded px-2 py-0.5 text-xs font-medium ${colors[status] ?? "bg-neutral-800 text-neutral-400"}`}
    >
      {status.replace("_", " ")}
    </span>
  );
}
