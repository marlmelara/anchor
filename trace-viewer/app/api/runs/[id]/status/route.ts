import { getRun } from "@/lib/anchor";

/** A run's status, for the replay panel to poll. */
export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  try {
    const run = await getRun(id);
    return Response.json({ status: run.status });
  } catch (err) {
    return Response.json({ error: (err as Error).message }, { status: 502 });
  }
}
