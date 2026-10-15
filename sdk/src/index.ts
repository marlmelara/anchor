/**
 * Anchor: a durable execution runtime for AI agents.
 *
 * ```ts
 * import { Client, defineAgent, agentLoop, modelCall, registerAgent, submitRun, streamRun } from "@anchor/sdk";
 *
 * const client = new Client({ baseUrl: "http://localhost:8080", apiKey: process.env.ANCHOR_API_KEY });
 *
 * const research = defineAgent({
 *   name: "research",
 *   nodes: [
 *     agentLoop({ id: "n1", model: "claude-opus-5-5", maxIterations: 8, tools: ["web_fetch"] }),
 *     modelCall({ id: "n2", model: "claude-opus-5-5", prompt: "Format {{n1.output}} as JSON" }),
 *   ],
 * });
 *
 * await registerAgent(client, research);
 * const run = await submitRun(client, research, { input: { topic: "durable execution" } });
 * for await (const event of streamRun(client, run.id)) console.log(event.type);
 * ```
 */
export * from "./types.ts";
export * from "./agent.ts";
export * from "./client.ts";
export { describeEvent, formatMicros, stepIndexOf } from "./describe.ts";
