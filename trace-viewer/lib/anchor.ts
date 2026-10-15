import type { RunDetail, RunEvent, RunSummary } from "@anchor/sdk";

/**
 * Server-side access to anchor-server.
 *
 * Every function here is called from a Server Component or a Route Handler,
 * never from the browser. That is what keeps ANCHOR_API_KEY secret: a Client
 * Component reading it would ship it to the page.
 */

const baseUrl = process.env.ANCHOR_URL ?? "http://localhost:8080";

function headers(): HeadersInit {
  const key = process.env.ANCHOR_API_KEY;
  return key ? { authorization: `Bearer ${key}` } : {};
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${baseUrl}${path}`, {
    headers: headers(),
    // Run data changes constantly; a cached run list would show a finished run
    // as still running. Correctness beats the cache here.
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`anchor-server ${res.status} for ${path}`);
  }
  return (await res.json()) as T;
}

export async function listRuns(limit = 100): Promise<RunSummary[]> {
  const res = await get<{ runs: RunSummary[] }>(`/v1/runs?limit=${limit}`);
  return res.runs ?? [];
}

export async function getRun(id: string): Promise<RunDetail> {
  return get<RunDetail>(`/v1/runs/${encodeURIComponent(id)}`);
}

export async function getEvents(id: string, after = 0): Promise<RunEvent[]> {
  const res = await get<{ events: RunEvent[] }>(
    `/v1/runs/${encodeURIComponent(id)}/events?after=${after}`,
  );
  return res.events ?? [];
}

export async function replayRun(id: string): Promise<{ id: string; replay_of: string }> {
  const res = await fetch(`${baseUrl}/v1/runs/${encodeURIComponent(id)}/replay`, {
    method: "POST",
    headers: headers(),
    cache: "no-store",
  });
  if (!res.ok) {
    const body = (await res.json().catch(() => ({}))) as { error?: string };
    throw new Error(body.error ?? `replay failed with ${res.status}`);
  }
  return (await res.json()) as { id: string; replay_of: string };
}

/** The upstream SSE URL, used by the route handler that proxies it. */
export function streamUrl(id: string, after: number): string {
  return `${baseUrl}/v1/runs/${encodeURIComponent(id)}/stream?after=${after}`;
}

export { headers as anchorHeaders };
