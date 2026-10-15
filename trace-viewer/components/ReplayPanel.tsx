"use client";

import { useState } from "react";

type DiffRow = { index: number; original: string | null; replayed: string | null; same: boolean };
type Diff = { matched: boolean; firstDivergence: number | null; rows: DiffRow[] };

/**
 * The replay button and its diff.
 *
 * Starts a replay, waits for it to finish, then shows the two event sequences
 * side by side with the first divergence highlighted. A clean diff is the
 * project's central claim made visible: the same run, re-executed from its
 * journal, took exactly the same path.
 */
export function ReplayPanel({ runId }: { runId: string }) {
  const [state, setState] = useState<"idle" | "running" | "done" | "error">("idle");
  const [diff, setDiff] = useState<Diff | null>(null);
  const [message, setMessage] = useState("");

  async function startReplay() {
    setState("running");
    setDiff(null);
    setMessage("");
    try {
      const res = await fetch(`/api/runs/${runId}/replay`, { method: "POST" });
      const body = (await res.json()) as { id?: string; error?: string };
      if (!res.ok || !body.id) throw new Error(body.error ?? "could not start the replay");

      // Poll until the replay reaches a terminal state. A replay of a recorded
      // run makes no network calls, so this is normally a second or two.
      const replayId = body.id;
      for (let i = 0; i < 120; i++) {
        await new Promise((r) => setTimeout(r, 500));
        const runRes = await fetch(`/api/runs/${replayId}/status`);
        const run = (await runRes.json()) as { status?: string };
        if (run.status && ["completed", "failed", "cancelled", "needs_review"].includes(run.status)) {
          break;
        }
      }

      const diffRes = await fetch(`/api/runs/${replayId}/diff?original=${runId}`);
      const d = (await diffRes.json()) as Diff;
      setDiff(d);
      setState("done");
      setMessage(
        d.matched
          ? "The replay reproduced this run exactly."
          : `The replay diverged at event ${(d.firstDivergence ?? 0) + 1}.`,
      );
    } catch (err) {
      setState("error");
      setMessage((err as Error).message);
    }
  }

  return (
    <section className="rounded border border-neutral-800 p-4">
      <div className="flex items-center gap-3">
        <button
          onClick={startReplay}
          disabled={state === "running"}
          className="rounded bg-violet-900 px-3 py-1.5 text-sm font-medium text-violet-100 hover:bg-violet-800 disabled:opacity-50"
        >
          {state === "running" ? "Replaying…" : "Replay this run"}
        </button>
        <p className="text-sm text-neutral-400">
          Re-executes the run against its recorded model and tool responses, then diffs the
          two event sequences.
        </p>
      </div>

      {message && (
        <p
          className={`mt-3 text-sm ${
            state === "error" || diff?.matched === false ? "text-red-300" : "text-emerald-300"
          }`}
        >
          {message}
        </p>
      )}

      {diff && (
        <div className="mt-4 overflow-x-auto rounded border border-neutral-800">
          <table className="w-full text-xs">
            <thead className="bg-neutral-900 text-left text-neutral-500">
              <tr>
                <th className="px-3 py-1.5 font-medium">#</th>
                <th className="px-3 py-1.5 font-medium">Original</th>
                <th className="px-3 py-1.5 font-medium">Replay</th>
              </tr>
            </thead>
            <tbody>
              {diff.rows.map((row) => (
                <tr
                  key={row.index}
                  className={`border-t border-neutral-800 font-mono ${
                    row.same
                      ? ""
                      : row.index === diff.firstDivergence
                        ? "bg-red-950/60 text-red-200"
                        : "bg-red-950/20 text-red-300/70"
                  }`}
                >
                  <td className="px-3 py-1 text-neutral-600">{row.index + 1}</td>
                  <td className="px-3 py-1">{row.original ?? "—"}</td>
                  <td className="px-3 py-1">{row.replayed ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
