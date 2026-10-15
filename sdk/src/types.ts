/**
 * The wire types Anchor's API speaks.
 *
 * These are the contract between the Go server and every TypeScript consumer.
 * Keeping one definition means a change to an event shape breaks the BUILD of
 * the SDK and the viewer, rather than the live page.
 */

/** The closed set of node types an agent graph may contain. */
export type NodeType = "model_call" | "tool_call" | "agent_loop" | "branch" | "map";

/** One node in an agent's graph. */
export interface Node {
  id: string;
  type: NodeType;
  /** model_call */
  prompt?: string;
  /** model_call, agent_loop */
  model?: string;
  system?: string;
  /** agent_loop */
  tools?: string[];
  max_iterations?: number;
  /** tool_call */
  tool?: string;
  input?: unknown;
  /** branch */
  condition?: string;
  if_true?: string;
  if_false?: string;
  /** map */
  over?: string;
  child?: string;
}

/** A complete agent definition. An agent is DATA, never code. */
export interface Graph {
  agent: string;
  v: number;
  nodes: Node[];
}

/** A tool an agent may call. */
export interface ToolDefinition {
  name: string;
  description?: string;
  /**
   * Whether running this twice is harmless.
   *
   * This is not documentation -- it decides what Anchor does when a worker dies
   * after starting the tool but before recording the result. An idempotent tool
   * is retried; a non-idempotent one halts the run for a human rather than
   * guessing whether the effect landed.
   *
   * Declaring `true` is a promise the implementation has to keep. A tool that
   * says it is idempotent and is not will apply its effect twice.
   */
  idempotent: boolean;
  input_schema?: unknown;
  output_schema?: unknown;
  image?: string;
  command?: string[];
  egress?: boolean;
}

export type RunStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "needs_review";

export type StepKind = "model" | "tool";

export type StepStatus =
  | "scheduled"
  | "running"
  | "completed"
  | "failed"
  | "retrying"
  | "needs_review";

export interface Step {
  step_index: number;
  kind: StepKind;
  name: string;
  node_id: string;
  idempotency_key: string;
  status: StepStatus;
  attempt: number;
  rand_seed: number;
  started_at?: string;
  finished_at?: string;
  tokens: number;
  cost_micros: number;
  error?: string;
  model_response?: unknown;
  tool_output?: unknown;
  retry_delays_ms?: number[];
}

export interface RunSummary {
  id: string;
  tenant_id: string;
  agent_name: string;
  agent_version: number;
  status: RunStatus;
  tokens_used: number;
  micros_used: number;
  created_at: string;
  started_at?: string;
  finished_at?: string;
  error?: string;
  replay_of?: string;
}

export interface RunDetail {
  id: string;
  status: RunStatus;
  agent: string;
  agent_version: number;
  created_at: string;
  started_at?: string;
  finished_at?: string;
  tokens_used: number;
  micros_used: number;
  budget_tokens: number;
  budget_micros: number;
  next_seq: number;
  steps: Step[] | null;
  error?: string;
  output?: unknown;
  replay_of?: string;
}

/**
 * Every event type the journal can hold.
 *
 * A discriminated union: one object shape per `type`, told apart by that
 * literal field. TypeScript narrows on it, so reading `tokens` inside a
 * `ModelCallCompleted` branch type-checks while reading it elsewhere does not.
 * A backend change to an event shape then breaks the build rather than the
 * page.
 */
export type RunEvent =
  | { seq: number; type: "RunSubmitted"; created_at: string; payload: RunSubmittedPayload }
  | { seq: number; type: "RunStarted"; created_at: string; payload: RunStartedPayload }
  | { seq: number; type: "StepScheduled"; created_at: string; payload: StepScheduledPayload }
  | { seq: number; type: "ModelCallStarted"; created_at: string; payload: StepRefPayload }
  | { seq: number; type: "ModelCallCompleted"; created_at: string; payload: ModelCallCompletedPayload }
  | { seq: number; type: "ToolCallStarted"; created_at: string; payload: ToolCallStartedPayload }
  | { seq: number; type: "ToolCallCompleted"; created_at: string; payload: ToolCallCompletedPayload }
  | { seq: number; type: "StepFailed"; created_at: string; payload: StepFailedPayload }
  | { seq: number; type: "RetryScheduled"; created_at: string; payload: RetryScheduledPayload }
  | { seq: number; type: "BudgetExceeded"; created_at: string; payload: BudgetExceededPayload }
  | { seq: number; type: "RunCancelled"; created_at: string; payload: RunCancelledPayload }
  | { seq: number; type: "RunCompleted"; created_at: string; payload: RunCompletedPayload }
  | { seq: number; type: "RunFailed"; created_at: string; payload: RunFailedPayload }
  | { seq: number; type: "RunNeedsReview"; created_at: string; payload: RunNeedsReviewPayload };

export type EventType = RunEvent["type"];

interface Versioned {
  v: number;
}
export interface RunSubmittedPayload extends Versioned {
  tenant_id: string;
  agent_name: string;
  agent_version: number;
  input: unknown;
  budget_tokens: number;
  budget_micros?: number;
  replay_of?: string;
}
export interface RunStartedPayload extends Versioned {
  worker_id: string;
  resumed: boolean;
}
export interface StepScheduledPayload extends Versioned {
  step_index: number;
  kind: StepKind;
  name: string;
  node_id: string;
  idempotency_key: string;
  rand_seed: number;
}
export interface StepRefPayload extends Versioned {
  step_index: number;
  attempt: number;
  model?: string;
  request?: unknown;
}
export interface ModelCallCompletedPayload extends Versioned {
  step_index: number;
  attempt: number;
  model: string;
  response: unknown;
  prompt_tokens: number;
  completion_tokens: number;
  cost_micros?: number;
  cost_unknown?: boolean;
}
export interface ToolCallStartedPayload extends Versioned {
  step_index: number;
  attempt: number;
  tool: string;
  input: unknown;
}
export interface ToolCallCompletedPayload extends Versioned {
  step_index: number;
  attempt: number;
  exit_code: number;
  output: unknown;
  stderr?: string;
}
export interface StepFailedPayload extends Versioned {
  step_index: number;
  attempt: number;
  error: string;
  retryable: boolean;
  needs_review?: boolean;
}
export interface RetryScheduledPayload extends Versioned {
  step_index: number;
  attempt: number;
  delay_ms: number;
}
export interface BudgetExceededPayload extends Versioned {
  resource: "tokens" | "micros" | "cents";
  limit: number;
  used: number;
  projected: number;
}
export interface RunCancelledPayload extends Versioned {
  reason: string;
  requested_by: string;
}
export interface RunCompletedPayload extends Versioned {
  output: unknown;
}
export interface RunFailedPayload extends Versioned {
  error: string;
  step_index?: number;
}
export interface RunNeedsReviewPayload extends Versioned {
  reason: string;
  step_index?: number;
}
