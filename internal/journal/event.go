// Package journal defines Anchor's event log and the fold that turns it into
// run state.
//
// run_events is the source of truth for every run. Nothing in Anchor stores
// mutable run state as the primary record: the `runs` and `steps` tables are
// projections that can be dropped and recomputed from this log alone (see
// anchorctl rebuild). Every non-deterministic value a run observed -- model
// responses, tool output, timestamps, random seeds, retry delays -- is captured
// in an event payload at record time so a replay can inject it instead of
// reaching for the network.
package journal

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CurrentPayloadVersion is the highest payload schema version this build knows
// how to fold. Every payload carries "v". A run recorded under an older version
// must still replay correctly, so the fold accepts any version <= this one and
// refuses anything newer rather than silently misreading it.
const CurrentPayloadVersion = 1

// EventType is the discriminator stored in run_events.type.
type EventType string

const (
	TypeRunSubmitted       EventType = "RunSubmitted"
	TypeRunStarted         EventType = "RunStarted"
	TypeStepScheduled      EventType = "StepScheduled"
	TypeModelCallStarted   EventType = "ModelCallStarted"
	TypeModelCallCompleted EventType = "ModelCallCompleted"
	TypeToolCallStarted    EventType = "ToolCallStarted"
	TypeToolCallCompleted  EventType = "ToolCallCompleted"
	TypeStepFailed         EventType = "StepFailed"
	TypeRetryScheduled     EventType = "RetryScheduled"
	TypeBudgetExceeded     EventType = "BudgetExceeded"
	TypeRunCancelled       EventType = "RunCancelled"
	TypeRunCompleted       EventType = "RunCompleted"
	TypeRunFailed          EventType = "RunFailed"
)

// Event is one row of run_events.
//
// (RunID, Seq) is unique. Seq is a per-run monotonic integer starting at 0 with
// no gaps; it is both the fold's ordering key and the optimistic-concurrency
// guard that keeps two workers from both advancing the same run.
type Event struct {
	ID        int64           `json:"id"`
	RunID     uuid.UUID       `json:"run_id"`
	Seq       int             `json:"seq"`
	Type      EventType       `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

// StepKind distinguishes the two side-effecting node executions.
type StepKind string

const (
	StepKindModel StepKind = "model"
	StepKindTool  StepKind = "tool"
)

// ---------------------------------------------------------------------------
// Payloads
//
// Each payload embeds Versioned so it carries "v" and can report its version to
// the decoder. Fields are additive across versions: a v1 fold reading a v1
// payload must never depend on a field a later version introduced.
// ---------------------------------------------------------------------------

// Versioned is embedded in every payload to carry the schema version.
type Versioned struct {
	V int `json:"v"`
}

func (p Versioned) payloadVersion() int { return p.V }

type versionedPayload interface{ payloadVersion() int }

// RunSubmittedPayload opens every run. It is always seq 0.
type RunSubmittedPayload struct {
	Versioned
	TenantID     string          `json:"tenant_id"`
	AgentName    string          `json:"agent_name"`
	AgentVersion int             `json:"agent_version"`
	Input        json.RawMessage `json:"input"`
	BudgetTokens int64           `json:"budget_tokens"`
	BudgetCents  int64           `json:"budget_cents"`
	// ReplayOf is set when this run reproduces another run against a
	// ReplaySource rather than live providers.
	ReplayOf *uuid.UUID `json:"replay_of,omitempty"`
}

// RunStartedPayload records which worker took the run. A run can be started
// more than once across its life (crash, lease takeover); each start is its own
// event and the latest one names the current owner.
type RunStartedPayload struct {
	Versioned
	WorkerID string `json:"worker_id"`
	// Resumed is true when a worker picked up a run that had already started,
	// i.e. this is a crash recovery rather than a first start.
	Resumed bool `json:"resumed"`
}

// StepScheduledPayload is written BEFORE the effect is attempted. This ordering
// -- journal, then act -- is what makes crash recovery possible: a step that
// has a StepScheduled but no terminal event is exactly the ambiguous window,
// and recovery resolves it by checking the idempotency key.
//
// A step is scheduled once. Retries bump Attempt via RetryScheduled and reuse
// the same StepIndex, IdempotencyKey and RandSeed, so a retried step stays one
// logical step with one deterministic seed.
type StepScheduledPayload struct {
	Versioned
	StepIndex      int      `json:"step_index"`
	Kind           StepKind `json:"kind"`
	Name           string   `json:"name"`
	NodeID         string   `json:"node_id"`
	IdempotencyKey string   `json:"idempotency_key"`
	// RandSeed is the recorded seed for any randomness this step needs. On
	// replay the generator is reseeded with the same value.
	RandSeed int64 `json:"rand_seed"`
}

// ModelCallStartedPayload records the exact request sent to the provider. The
// prompt is stored verbatim because "what did we actually send" is the first
// question asked when a run misbehaves.
type ModelCallStartedPayload struct {
	Versioned
	StepIndex int             `json:"step_index"`
	Attempt   int             `json:"attempt"`
	Model     string          `json:"model"`
	Request   json.RawMessage `json:"request"`
}

// ModelCallCompletedPayload captures the full provider response. This is the
// single largest source of non-determinism in an agent run, and on replay it is
// returned instead of calling the provider.
type ModelCallCompletedPayload struct {
	Versioned
	StepIndex        int             `json:"step_index"`
	Attempt          int             `json:"attempt"`
	Model            string          `json:"model"`
	Response         json.RawMessage `json:"response"`
	PromptTokens     int64           `json:"prompt_tokens"`
	CompletionTokens int64           `json:"completion_tokens"`
	Cents            int64           `json:"cents"`
}

// TotalTokens is what counts against the run's token budget.
func (p ModelCallCompletedPayload) TotalTokens() int64 {
	return p.PromptTokens + p.CompletionTokens
}

// ToolCallStartedPayload records the tool invocation as sent to the sandbox.
type ToolCallStartedPayload struct {
	Versioned
	StepIndex int             `json:"step_index"`
	Attempt   int             `json:"attempt"`
	Tool      string          `json:"tool"`
	Input     json.RawMessage `json:"input"`
}

// ToolCallCompletedPayload captures everything the container produced. On
// replay this is returned instead of running the container.
type ToolCallCompletedPayload struct {
	Versioned
	StepIndex int `json:"step_index"`
	Attempt   int `json:"attempt"`
	ExitCode  int `json:"exit_code"`
	// Output is the tool's stdout parsed and validated against the tool's
	// declared JSON schema. Stdout that fails validation produces StepFailed,
	// not a ToolCallCompleted.
	Output json.RawMessage `json:"output"`
	Stderr string          `json:"stderr"`
}

// StepFailedPayload ends an attempt. Retryable distinguishes a transient
// failure (timeout, 429, 5xx) from a terminal one (validation, 4xx, budget).
type StepFailedPayload struct {
	Versioned
	StepIndex int    `json:"step_index"`
	Attempt   int    `json:"attempt"`
	Error     string `json:"error"`
	Retryable bool   `json:"retryable"`
	// NeedsReview is set when a non-idempotent tool crashed inside the
	// ambiguous window. Anchor refuses to guess whether the effect landed and
	// halts the run for a human instead.
	NeedsReview bool `json:"needs_review"`
}

// RetryScheduledPayload records the backoff decision. DelayMS is recorded so a
// replay can skip the wait while preserving the event sequence exactly.
type RetryScheduledPayload struct {
	Versioned
	StepIndex int   `json:"step_index"`
	Attempt   int   `json:"attempt"`
	DelayMS   int64 `json:"delay_ms"`
}

// BudgetExceededPayload is written when a projected cost would breach the run
// budget. Budgets are enforced in code before the call is made, never by asking
// the model to behave.
type BudgetExceededPayload struct {
	Versioned
	// Resource is "tokens" or "cents".
	Resource  string `json:"resource"`
	Limit     int64  `json:"limit"`
	Used      int64  `json:"used"`
	Projected int64  `json:"projected"`
}

// RunCancelledPayload marks a cooperative cancellation. The worker observes it
// at the next step boundary and aborts in-flight work through its context.
type RunCancelledPayload struct {
	Versioned
	Reason      string `json:"reason"`
	RequestedBy string `json:"requested_by"`
}

// RunCompletedPayload is hard-terminal.
type RunCompletedPayload struct {
	Versioned
	Output json.RawMessage `json:"output"`
}

// RunFailedPayload is hard-terminal.
type RunFailedPayload struct {
	Versioned
	Error string `json:"error"`
	// StepIndex is the step that killed the run, when one did.
	StepIndex *int `json:"step_index,omitempty"`
	// NeedsReview promotes the run to needs_review instead of failed.
	NeedsReview bool `json:"needs_review"`
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

// decodePayload unmarshals a payload and enforces the version contract.
func decodePayload[T versionedPayload](raw json.RawMessage) (T, error) {
	var p T
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, fmt.Errorf("unmarshal payload: %w", err)
	}
	v := p.payloadVersion()
	if v < 1 {
		return p, fmt.Errorf("payload is missing its %q version field", "v")
	}
	if v > CurrentPayloadVersion {
		return p, fmt.Errorf(
			"payload version %d is newer than this build understands (max %d)",
			v, CurrentPayloadVersion)
	}
	return p, nil
}

// MarshalPayload stamps the current version onto a payload and encodes it.
// Every writer goes through here so no event can be appended without a version.
func MarshalPayload(p any) (json.RawMessage, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	// Verify the caller actually set v; a zero version would poison the log.
	var probe struct {
		V int `json:"v"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("probe payload version: %w", err)
	}
	if probe.V < 1 {
		return nil, fmt.Errorf("payload %T was marshalled without a version; set Versioned{V: journal.CurrentPayloadVersion}", p)
	}
	return raw, nil
}
