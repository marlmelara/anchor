import { getEvents } from "@/lib/anchor";
import type { RunEvent } from "@anchor/sdk";

/**
 * Compares a replay's event sequence against its original.
 *
 * Mirrors the Go `replay.Compare`: event TYPE, ORDER, step and attempt are
 * compared; timestamps and ids are not, because a replay happens later by
 * definition and comparing them would make every replay differ at event one.
 *
 * This is the viewer's own read of the journal, not a cached verdict from the
 * server, so the page shows what the two logs actually say.
 */
export async function GET(request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const originalId = new URL(request.url).searchParams.get("original");
  if (!originalId) {
    return Response.json({ error: "original run id is required" }, { status: 400 });
  }

  const [original, replayed] = await Promise.all([getEvents(originalId), getEvents(id)]);

  const rows = [];
  const n = Math.max(original.length, replayed.length);
  let firstDivergence: number | null = null;

  for (let i = 0; i < n; i++) {
    const a = original[i];
    const b = replayed[i];
    const same = a && b && signature(a) === signature(b);
    if (!same && firstDivergence === null) firstDivergence = i;
    rows.push({
      index: i,
      original: a ? signature(a) : null,
      replayed: b ? signature(b) : null,
      same: Boolean(same),
    });
  }

  return Response.json({
    matched: firstDivergence === null && original.length === replayed.length,
    firstDivergence,
    rows,
  });
}

/** The comparable shape of an event: no timestamp, no id. */
function signature(e: RunEvent): string {
  const payload = e.payload as { step_index?: number; attempt?: number };
  const step = typeof payload.step_index === "number" ? `/step=${payload.step_index}` : "";
  const attempt = typeof payload.attempt === "number" ? `/attempt=${payload.attempt}` : "";
  return `${e.type}${step}${attempt}`;
}
