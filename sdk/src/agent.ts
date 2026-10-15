import type { Graph, Node, ToolDefinition } from "./types.ts";

/**
 * Builders for an agent graph.
 *
 * Every function here produces plain JSON. Nothing in this file executes agent
 * logic, and nothing in the SDK ever will -- the Go worker interprets the graph
 * node by node. That is the single most important structural decision in
 * Anchor, because it is what makes replay provable: the node sequence is fixed
 * and versioned, so a replay walks the same nodes by construction rather than
 * by policing user code for clock and random-number reads.
 */

/** The graph schema version this SDK emits. */
export const GRAPH_VERSION = 1;

/** One LLM call with a rendered prompt template. */
export function modelCall(opts: {
  id: string;
  model: string;
  prompt: string;
  system?: string;
}): Node {
  return { type: "model_call", ...opts };
}

/** One sandboxed tool invocation. */
export function toolCall(opts: { id: string; tool: string; input?: unknown }): Node {
  return { type: "tool_call", ...opts };
}

/**
 * The core node: call the model with tools exposed; if it picks a tool, run it,
 * append the result, repeat. Bounded by `maxIterations` and the run budget.
 */
export function agentLoop(opts: {
  id: string;
  model: string;
  maxIterations: number;
  tools?: string[];
  system?: string;
}): Node {
  const { maxIterations, ...rest } = opts;
  return { type: "agent_loop", max_iterations: maxIterations, ...rest };
}

/** Picks the next node from a condition over earlier output. */
export function branch(opts: {
  id: string;
  condition: string;
  ifTrue?: string;
  ifFalse?: string;
}): Node {
  const { ifTrue, ifFalse, ...rest } = opts;
  return {
    type: "branch",
    ...rest,
    ...(ifTrue !== undefined ? { if_true: ifTrue } : {}),
    ...(ifFalse !== undefined ? { if_false: ifFalse } : {}),
  };
}

/**
 * Assembles a graph and checks the rules the server also checks.
 *
 * Validating here as well as server-side is deliberate duplication: a mistake
 * caught in the editor costs nothing, and the same mistake caught after
 * submission has already consumed a round trip. The server remains the
 * authority -- a client is never trusted -- but a client that catches its own
 * errors is a better client.
 */
export function defineAgent(opts: { name: string; nodes: Node[] }): Graph {
  if (!opts.name) throw new Error("defineAgent: name is required");
  if (opts.nodes.length === 0) throw new Error("defineAgent: at least one node is required");

  const seen = new Set<string>();
  for (const node of opts.nodes) {
    if (!/^[a-zA-Z][a-zA-Z0-9_]{0,63}$/.test(node.id)) {
      throw new Error(
        `defineAgent: node id ${JSON.stringify(node.id)} must start with a letter and contain only letters, digits and underscores`,
      );
    }
    if (seen.has(node.id)) {
      // Node ids address output in templates ({{n1.output}}), so a duplicate
      // would make a reference ambiguous.
      throw new Error(`defineAgent: duplicate node id ${JSON.stringify(node.id)}`);
    }
    seen.add(node.id);
  }

  for (const node of opts.nodes) {
    switch (node.type) {
      case "model_call":
        if (!node.prompt) throw new Error(`node ${node.id}: model_call requires a prompt`);
        if (!node.model) throw new Error(`node ${node.id}: model_call requires a model`);
        break;
      case "tool_call":
        if (!node.tool) throw new Error(`node ${node.id}: tool_call requires a tool`);
        break;
      case "agent_loop":
        if (!node.model) throw new Error(`node ${node.id}: agent_loop requires a model`);
        if (!node.max_iterations || node.max_iterations < 1) {
          throw new Error(`node ${node.id}: agent_loop requires max_iterations >= 1`);
        }
        break;
      case "branch":
        if (!node.condition) throw new Error(`node ${node.id}: branch requires a condition`);
        for (const target of [node.if_true, node.if_false]) {
          if (target && !seen.has(target)) {
            throw new Error(`node ${node.id}: branch targets unknown node ${JSON.stringify(target)}`);
          }
        }
        break;
      case "map":
        if (!node.over) throw new Error(`node ${node.id}: map requires an 'over' expression`);
        break;
    }
  }

  return { agent: opts.name, v: GRAPH_VERSION, nodes: opts.nodes };
}

/**
 * Declares a tool.
 *
 * `idempotent` is required rather than defaulting, because neither default is
 * safe. Defaulting to true would silently permit retrying an effect that must
 * not repeat; defaulting to false would halt runs that were fine. The author
 * has to decide, and that decision is the one Anchor acts on when a worker dies
 * mid-effect.
 */
export function defineTool(def: ToolDefinition): ToolDefinition {
  if (!def.name) throw new Error("defineTool: name is required");
  if (typeof def.idempotent !== "boolean") {
    throw new Error(
      `defineTool: ${def.name} must declare idempotent explicitly -- it decides whether Anchor retries the tool or halts the run when a worker dies mid-effect`,
    );
  }
  return def;
}
