import { replayRun } from "@/lib/anchor";

/** Starts a replay. Server-side so the API key stays here. */
export async function POST(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  try {
    return Response.json(await replayRun(id));
  } catch (err) {
    return Response.json({ error: (err as Error).message }, { status: 409 });
  }
}
