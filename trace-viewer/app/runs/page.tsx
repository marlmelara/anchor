import Link from "next/link";
import { listRuns } from "@/lib/anchor";
import { StatusBadge } from "@/components/StatusBadge";
import { formatMicros } from "@anchor/sdk";

/**
 * The run list. A SERVER Component -- note there is no "use client".
 *
 * It awaits its data directly while rendering and sends finished HTML, so the
 * first paint needs no JavaScript and no loading spinner. It also means the API
 * key is read on the server and never reaches the browser; a Client Component
 * doing this fetch would leak it into the page source.
 */
export default async function RunsPage({
  searchParams,
}: {
  searchParams: Promise<{ status?: string }>;
}) {
  const { status = "" } = await searchParams;
  const runs = await listRuns(200);
  const filtered = status ? runs.filter((r) => r.status === status) : runs;

  return (
    <div>
      <div className="mb-6 flex items-baseline justify-between">
        <h1 className="text-xl font-semibold">Runs</h1>
        <nav className="flex gap-2 text-sm">
          {["", "completed", "failed", "running", "needs_review"].map((s) => (
            <Link
              key={s || "all"}
              href={s ? `/runs?status=${s}` : "/runs"}
              className={`rounded px-2 py-1 ${
                status === s ? "bg-neutral-800 text-neutral-100" : "text-neutral-400 hover:text-neutral-200"
              }`}
            >
              {s || "all"}
            </Link>
          ))}
        </nav>
      </div>

      {filtered.length === 0 ? (
        <p className="text-neutral-500">
          No runs yet. Submit one with{" "}
          <code className="font-mono text-neutral-400">anchorctl submit -agent &lt;name&gt;</code>.
        </p>
      ) : (
        <div className="overflow-x-auto rounded border border-neutral-800">
          <table className="w-full text-sm">
            <thead className="bg-neutral-900 text-left text-neutral-400">
              <tr>
                <th className="px-4 py-2 font-medium">Run</th>
                <th className="px-4 py-2 font-medium">Agent</th>
                <th className="px-4 py-2 font-medium">Status</th>
                <th className="px-4 py-2 text-right font-medium">Tokens</th>
                <th className="px-4 py-2 text-right font-medium">Cost</th>
                <th className="px-4 py-2 font-medium">Started</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r) => (
                // A stable key per row, so React updates only what changed
                // rather than redrawing the table.
                <tr key={r.id} className="border-t border-neutral-800 hover:bg-neutral-900/50">
                  <td className="px-4 py-2 font-mono text-xs">
                    <Link href={`/runs/${r.id}`} className="text-sky-400 hover:underline">
                      {r.id.slice(0, 8)}
                    </Link>
                    {r.replay_of && (
                      <span
                        className="ml-2 rounded bg-violet-950 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-violet-300"
                        title={`replay of ${r.replay_of}`}
                      >
                        replay
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-2">
                    {r.agent_name}
                    <span className="ml-1 text-neutral-600">v{r.agent_version}</span>
                  </td>
                  <td className="px-4 py-2"><StatusBadge status={r.status} /></td>
                  <td className="px-4 py-2 text-right tabular-nums">{r.tokens_used}</td>
                  <td className="px-4 py-2 text-right tabular-nums">{formatMicros(r.micros_used)}</td>
                  <td className="px-4 py-2 text-neutral-500">
                    {r.started_at ? new Date(r.started_at).toLocaleTimeString() : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
