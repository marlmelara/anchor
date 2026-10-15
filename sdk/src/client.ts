import type { Graph, RunDetail, RunEvent, RunSummary } from "./types.ts";

/** How to reach an Anchor server. */
export interface ClientOptions {
  baseUrl: string;
  /** Bearer token. Resolves the tenant server-side. */
  apiKey?: string;
  /** Per-request timeout. Defaults to 30s. */
  timeoutMs?: number;
  /** Injectable for tests; defaults to global fetch. */
  fetch?: typeof globalThis.fetch;
}

/** An error carrying the HTTP status, so callers can branch on it. */
export class AnchorError extends Error {
  readonly status: number;
  readonly retryAfterSeconds?: number;

  constructor(status: number, message: string, retryAfterSeconds?: number) {
    super(message);
    this.name = "AnchorError";
    this.status = status;
    if (retryAfterSeconds !== undefined) this.retryAfterSeconds = retryAfterSeconds;
  }

  /**
   * Whether retrying the same request could succeed.
   *
   * 429 means the queue is saturated and `retryAfterSeconds` says when to come
   * back. 5xx is the server's problem and may pass. A 4xx is the request
   * itself being wrong, and sending it again produces the same answer.
   */
  get retryable(): boolean {
    return this.status === 429 || this.status === 408 || this.status >= 500;
  }
}

export class Client {
  readonly baseUrl: string;
  readonly #apiKey: string | undefined;
  readonly #timeoutMs: number;
  readonly #fetch: typeof globalThis.fetch;

  constructor(opts: ClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
    this.#apiKey = opts.apiKey;
    this.#timeoutMs = opts.timeoutMs ?? 30_000;
    this.#fetch = opts.fetch ?? globalThis.fetch.bind(globalThis);
  }

  /** Headers for a request. The key never appears in a URL. */
  headers(extra?: Record<string, string>): Record<string, string> {
    return {
      ...(this.#apiKey ? { authorization: `Bearer ${this.#apiKey}` } : {}),
      ...extra,
    };
  }

  async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    // AbortSignal.timeout gives every request a deadline, so a hung server
    // cannot leave a caller waiting forever.
    const res = await this.#fetch(`${this.baseUrl}${path}`, {
      method,
      signal: AbortSignal.timeout(this.#timeoutMs),
      headers: this.headers(body !== undefined ? { "content-type": "application/json" } : undefined),
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });

    if (!res.ok) {
      const retryAfter = res.headers.get("retry-after");
      let message = `${method} ${path} failed with ${res.status}`;
      try {
        const parsed = (await res.json()) as { error?: string };
        if (parsed.error) message = parsed.error;
      } catch {
        // A non-JSON error body is still an error; the status carries the
        // information that matters.
      }
      throw new AnchorError(
        res.status,
        message,
        retryAfter ? Number(retryAfter) : undefined,
      );
    }

    if (res.status === 204) return undefined as T;
    return (await res.json()) as T;
  }
}

/** Registers an agent graph, returning its new version. */
export async function registerAgent(
  client: Client,
  graph: Graph,
): Promise<{ name: string; version: number }> {
  return client.request("POST", "/v1/agents", { name: graph.agent, graph });
}

export interface SubmitOptions {
  input?: unknown;
  /** Pin a specific agent version. Defaults to the latest. */
  agentVersion?: number;
  budgetTokens?: number;
  /** Spending cap in micro-dollars (1e-6 USD). 500000 is fifty cents. */
  budgetMicros?: number;
  priority?: number;
}

/** Submits a run. Returns as soon as it is queued, not when it finishes. */
export async function submitRun(
  client: Client,
  graph: Graph | string,
  opts: SubmitOptions = {},
): Promise<{ id: string; status: string }> {
  const agent = typeof graph === "string" ? graph : graph.agent;
  return client.request("POST", "/v1/runs", {
    agent,
    ...(opts.agentVersion !== undefined ? { agent_version: opts.agentVersion } : {}),
    ...(opts.input !== undefined ? { input: opts.input } : {}),
    ...(opts.budgetTokens !== undefined ? { budget_tokens: opts.budgetTokens } : {}),
    ...(opts.budgetMicros !== undefined ? { budget_micros: opts.budgetMicros } : {}),
    ...(opts.priority !== undefined ? { priority: opts.priority } : {}),
  });
}

export async function getRun(client: Client, runId: string): Promise<RunDetail> {
  return client.request("GET", `/v1/runs/${encodeURIComponent(runId)}`);
}

export async function listRuns(client: Client, limit = 100): Promise<RunSummary[]> {
  const res = await client.request<{ runs: RunSummary[] }>("GET", `/v1/runs?limit=${limit}`);
  return res.runs;
}

export async function getEvents(
  client: Client,
  runId: string,
  after = 0,
): Promise<RunEvent[]> {
  const res = await client.request<{ events: RunEvent[] }>(
    "GET",
    `/v1/runs/${encodeURIComponent(runId)}/events?after=${after}`,
  );
  return res.events ?? [];
}

export async function cancelRun(
  client: Client,
  runId: string,
  reason?: string,
): Promise<RunDetail> {
  return client.request("POST", `/v1/runs/${encodeURIComponent(runId)}/cancel`, { reason });
}

/** Starts a replay of a finished run and returns the NEW run's id. */
export async function replayRun(
  client: Client,
  runId: string,
): Promise<{ id: string; replay_of: string }> {
  return client.request("POST", `/v1/runs/${encodeURIComponent(runId)}/replay`);
}

/**
 * Yields a run's events as they happen, finishing when the run is terminal.
 *
 * An async generator, the same idea as Python's `async def` with `yield`.
 * Callers consume it with `for await (const event of streamRun(...))`.
 *
 * Polls rather than holding an EventSource, because this runs in Node as well
 * as the browser and `EventSource` is browser-only. The viewer uses the SSE
 * endpoint directly for the same data; this is the portable path.
 */
export async function* streamRun(
  client: Client,
  runId: string,
  opts: { after?: number; pollMs?: number; signal?: AbortSignal } = {},
): AsyncGenerator<RunEvent> {
  let after = opts.after ?? 0;
  const pollMs = opts.pollMs ?? 500;

  while (!opts.signal?.aborted) {
    const batch = await getEvents(client, runId, after);
    for (const event of batch) {
      yield event;
      after = event.seq;
      if (
        event.type === "RunCompleted" ||
        event.type === "RunFailed" ||
        event.type === "RunCancelled" ||
        event.type === "RunNeedsReview"
      ) {
        return;
      }
    }
    await new Promise<void>((resolve) => setTimeout(resolve, pollMs));
  }
}
