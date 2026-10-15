/**
 * The SDK quickstart: define an agent, submit a run, stream its events.
 *
 *   ANCHOR_URL=http://localhost:8080 node --experimental-strip-types examples/quickstart.ts
 */
import {
  Client,
  agentLoop,
  defineAgent,
  defineTool,
  describeEvent,
  modelCall,
  registerAgent,
  streamRun,
  submitRun,
} from "../src/index.ts";

const client = new Client({
  baseUrl: process.env.ANCHOR_URL ?? "http://localhost:8080",
  ...(process.env.ANCHOR_API_KEY ? { apiKey: process.env.ANCHOR_API_KEY } : {}),
});

// Tools declare whether repeating them is harmless. That decision is what
// Anchor acts on when a worker dies after starting the tool but before
// recording the result.
export const echo = defineTool({ name: "echo", idempotent: true });

const research = defineAgent({
  name: "research",
  nodes: [
    agentLoop({
      id: "n1",
      model: "claude-opus-5-5",
      maxIterations: 4,
      tools: ["echo"],
      system: "You are a research assistant.",
    }),
    modelCall({
      id: "n2",
      model: "claude-opus-5-5",
      prompt: "Summarise {{n1.output}} in one sentence.",
    }),
  ],
});

const { version } = await registerAgent(client, research);
console.log(`registered ${research.agent} v${version}`);

const run = await submitRun(client, research, {
  input: { topic: "durable execution" },
  budgetTokens: 50_000,
  budgetMicros: 500_000, // fifty cents
});
console.log(`submitted ${run.id}`);

for await (const event of streamRun(client, run.id)) {
  console.log(`  ${String(event.seq).padStart(2)} ${describeEvent(event)}`);
}
