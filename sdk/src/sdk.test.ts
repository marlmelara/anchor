import { describe, it } from "node:test";
import assert from "node:assert/strict";

import { agentLoop, defineAgent, defineTool, modelCall, toolCall } from "./agent.ts";
import { AnchorError, Client, getRun, submitRun } from "./client.ts";
import { describeEvent, formatMicros } from "./describe.ts";
import type { RunEvent } from "./types.ts";

describe("defineAgent", () => {
  it("builds the spec's example graph", () => {
    const g = defineAgent({
      name: "research",
      nodes: [
        agentLoop({ id: "n1", model: "claude-opus-5-5", maxIterations: 8, tools: ["web_fetch"] }),
        modelCall({ id: "n2", model: "claude-opus-5-5", prompt: "Format {{n1.output}} as JSON" }),
      ],
    });
    assert.equal(g.agent, "research");
    assert.equal(g.v, 1);
    assert.equal(g.nodes.length, 2);
    assert.equal(g.nodes[0]?.max_iterations, 8);
  });

  // Node ids address output in templates, so a duplicate makes {{n1.output}}
  // ambiguous.
  it("rejects a duplicate node id", () => {
    assert.throws(
      () =>
        defineAgent({
          name: "a",
          nodes: [
            modelCall({ id: "n1", model: "m", prompt: "x" }),
            modelCall({ id: "n1", model: "m", prompt: "y" }),
          ],
        }),
      /duplicate node id/,
    );
  });

  it("rejects an empty graph and a missing name", () => {
    assert.throws(() => defineAgent({ name: "a", nodes: [] }), /at least one node/);
    assert.throws(() => defineAgent({ name: "", nodes: [modelCall({ id: "n1", model: "m", prompt: "x" })] }), /name is required/);
  });

  it("rejects a node missing its requirements", () => {
    assert.throws(
      () => defineAgent({ name: "a", nodes: [toolCall({ id: "n1", tool: "" })] }),
      /tool_call requires a tool/,
    );
    assert.throws(
      () => defineAgent({ name: "a", nodes: [agentLoop({ id: "n1", model: "m", maxIterations: 0 })] }),
      /max_iterations >= 1/,
    );
  });

  it("rejects an invalid node id", () => {
    assert.throws(
      () => defineAgent({ name: "a", nodes: [modelCall({ id: "9bad", model: "m", prompt: "x" })] }),
      /must start with a letter/,
    );
  });
});

describe("defineTool", () => {
  // Neither default is safe: true would silently permit retrying an effect that
  // must not repeat, false would halt runs that were fine.
  it("requires idempotent to be declared explicitly", () => {
    assert.throws(
      () => defineTool({ name: "send_email" } as never),
      /must declare idempotent explicitly/,
    );
  });

  it("accepts a fully declared tool", () => {
    const t = defineTool({ name: "web_fetch", idempotent: true, image: "anchor/web-fetch:1" });
    assert.equal(t.idempotent, true);
  });
});

describe("Client", () => {
  const fakeFetch = (status: number, body: unknown, headers: Record<string, string> = {}) =>
    (async () =>
      new Response(JSON.stringify(body), {
        status,
        headers: { "content-type": "application/json", ...headers },
      })) as unknown as typeof globalThis.fetch;

  it("sends the api key as a bearer header, never in the url", () => {
    const client = new Client({ baseUrl: "http://x", apiKey: "secret-key" });
    const headers = client.headers();
    assert.equal(headers["authorization"], "Bearer secret-key");
    assert.ok(!client.baseUrl.includes("secret-key"));
  });

  it("strips a trailing slash from the base url", () => {
    assert.equal(new Client({ baseUrl: "http://x/" }).baseUrl, "http://x");
  });

  it("surfaces the server's error message", async () => {
    const client = new Client({ baseUrl: "http://x", fetch: fakeFetch(404, { error: "no such run" }) });
    await assert.rejects(() => getRun(client, "abc"), (err: unknown) => {
      assert.ok(err instanceof AnchorError);
      assert.equal(err.status, 404);
      assert.equal(err.message, "no such run");
      return true;
    });
  });

  // 429 means the queue is saturated; the caller should come back, not give up.
  it("marks 429 retryable and carries retry-after", async () => {
    const client = new Client({
      baseUrl: "http://x",
      fetch: fakeFetch(429, { error: "queue is full" }, { "retry-after": "5" }),
    });
    await assert.rejects(() => submitRun(client, "a"), (err: unknown) => {
      assert.ok(err instanceof AnchorError);
      assert.equal(err.retryable, true);
      assert.equal(err.retryAfterSeconds, 5);
      return true;
    });
  });

  // A 4xx is the request being wrong; sending it again gives the same answer.
  it("marks 400 terminal", async () => {
    const client = new Client({ baseUrl: "http://x", fetch: fakeFetch(400, { error: "bad graph" }) });
    await assert.rejects(() => submitRun(client, "a"), (err: unknown) => {
      assert.ok(err instanceof AnchorError);
      assert.equal(err.retryable, false);
      return true;
    });
  });
});

describe("describeEvent", () => {
  it("narrows each variant of the union", () => {
    const events: RunEvent[] = [
      { seq: 1, type: "RunStarted", created_at: "", payload: { v: 2, worker_id: "w-1", resumed: false } },
      { seq: 2, type: "RunStarted", created_at: "", payload: { v: 2, worker_id: "w-2", resumed: true } },
      {
        seq: 3,
        type: "ModelCallCompleted",
        created_at: "",
        payload: { v: 2, step_index: 0, attempt: 0, model: "m", response: {}, prompt_tokens: 36, completion_tokens: 143, cost_micros: 3004 },
      },
      { seq: 4, type: "RunCompleted", created_at: "", payload: { v: 2, output: {} } },
    ];
    assert.match(describeEvent(events[0]!), /started by w-1/);
    assert.match(describeEvent(events[1]!), /resumed by w-2/);
    assert.match(describeEvent(events[2]!), /179 tokens/);
    assert.equal(describeEvent(events[3]!), "completed");
  });
});

describe("formatMicros", () => {
  // The number that forced micro-dollars: a real call costs about $0.003,
  // which integer cents round to zero.
  it("renders a sub-cent cost without rounding it away", () => {
    assert.equal(formatMicros(0), "$0.00");
    assert.equal(formatMicros(3004), "$0.003004");
    assert.equal(formatMicros(1_000_000), "$1.00");
    assert.equal(formatMicros(500_000), "$0.50");
  });
});
