import Link from "next/link";
import { getEvents, getRun } from "@/lib/anchor";
import { LiveTimeline } from "@/components/LiveTimeline";
import { StepList } from "@/components/StepList";
import { ReplayPanel } from "@/components/ReplayPanel";
import { StatusBadge } from "@/components/StatusBadge";
import { formatMicros } from "@anchor/sdk";

/**
 * Run detail. A Server Component that fetches the run and its events, then
 * hands them to small Client Components for the interactive parts.
 *
 * This is the pattern worth knowing: the server does the data fetching and
 * ships HTML, and only the pieces that genuinely need state or browser APIs
 * become client components. Keeping those small keeps the JavaScript bundle
 * small.
 */
export default async function RunPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const [run, events] = await Promise.all([getRun(id), getEvents(id)]);
  const live = !["completed", "failed", "cancelled", "needs_review"].includes(run.status);

  return (
    <div className="space-y-8">
      <header>
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="font-mono text-lg">{run.id}</h1>
          <StatusBadge status={run.status} />
          {run.replay_of && (
            <Link
              href={`/runs/${run.replay_of}`}
              className="rounded bg-violet-950 px-2 py-0.5 text-xs text-violet-300 hover:underline"
            >
              replay of {run.replay_of.slice(0, 8)}
            </Link>
          )}
        </div>
        <p className="mt-1 text-sm text-neutral-500">
          {run.agent} v{run.agent_version}
        </p>
        {run.error && (
          <p className="mt-3 rounded border border-red-900 bg-red-950/40 px-3 py-2 font-mono text-sm text-red-300">
            {run.error}
          </p>
        )}
      </header>

      <section className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <Stat label="Tokens" value={`${run.tokens_used}`} sub={run.budget_tokens ? `of ${run.budget_tokens}` : undefined} />
        <Stat
          label="Cost"
          value={formatMicros(run.micros_used)}
          sub={run.budget_micros ? `cap ${formatMicros(run.budget_micros)}` : undefined}
        />
        <Stat label="Steps" value={`${run.steps?.length ?? 0}`} />
        <Stat
          label="Duration"
          value={
            run.started_at && run.finished_at
              ? `${((new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()) / 1000).toFixed(2)}s`
              : "—"
          }
        />
      </section>

      {!run.replay_of && run.status === "completed" && <ReplayPanel runId={run.id} />}

      <section>
        <h2 className="mb-3 text-sm text-neutral-400">Steps</h2>
        <StepList steps={run.steps ?? []} />
      </section>

      <section>
        <LiveTimeline runId={run.id} initial={events} live={live} />
      </section>
    </div>
  );
}

function Stat({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="rounded border border-neutral-800 px-3 py-2">
      <div className="text-xs text-neutral-500">{label}</div>
      <div className="text-lg tabular-nums">{value}</div>
      {sub && <div className="text-xs text-neutral-600">{sub}</div>}
    </div>
  );
}
