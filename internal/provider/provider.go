// Package provider is the seam between the worker and an LLM.
//
// The worker never calls a provider directly; it holds a ModelProvider
// interface. That one indirection is what lets the same worker code do three
// different jobs: live runs against Anthropic, benchmarks against a mock with
// fixed latency, and replays that answer from the journal instead of the
// network. In Go an interface is satisfied implicitly -- any type with these
// methods fits, with no "implements" keyword, like typing.Protocol in Python.
package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Role is who said a message.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// Message is one turn of conversation.
type Message struct {
	Role Role   `json:"role"`
	Text string `json:"text"`
	// ToolName and ToolResult are set on RoleTool messages, which carry a tool's
	// output back into the conversation.
	ToolName   string          `json:"tool_name,omitempty"`
	ToolResult json.RawMessage `json:"tool_result,omitempty"`
}

// ToolSpec describes a tool to the model.
type ToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

// Request is everything sent to the model. It is journaled verbatim in
// ModelCallStarted, because "what did we actually send" is the first question
// asked when a run misbehaves.
type Request struct {
	Model    string     `json:"model"`
	System   string     `json:"system,omitempty"`
	Messages []Message  `json:"messages"`
	Tools    []ToolSpec `json:"tools,omitempty"`

	// StepIndex identifies which step this call belongs to. A replay provider
	// uses it to find the recorded response.
	StepIndex int `json:"step_index"`
	// Attempt is which try this is for that step. A replay needs it as well as
	// the index: a step that failed once and then succeeded must replay the
	// failure too, or the replay produces a shorter event sequence than the
	// original and diverges for a reason that is not a real bug.
	Attempt int `json:"attempt"`
	// MaxTokens bounds one call; the run budget bounds the whole run.
	MaxTokens int `json:"max_tokens,omitempty"`
}

// ToolCall is the model asking for a tool to be run.
type ToolCall struct {
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

// Response is what came back. Raw holds the provider's entire body so a replay
// can return exactly what the original run saw, not a lossy summary of it.
type Response struct {
	Text     string    `json:"text,omitempty"`
	ToolCall *ToolCall `json:"tool_call,omitempty"`

	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`

	// No cost field. The provider API returns token counts and never cost, so
	// a Cents/Cost field here could only ever be invented by whoever built the
	// Response -- which is exactly how a mock came to report a cost the real
	// adapter never did, leaving the spending budget untested. Cost is derived
	// in one place, internal/pricing, from these token counts and the model.
	Raw json.RawMessage `json:"raw,omitempty"`
}

// TotalTokens is what counts against the run's token budget.
func (r Response) TotalTokens() int64 { return r.PromptTokens + r.CompletionTokens }

// Provider is the interface the worker holds.
type Provider interface {
	// Complete performs one model call. It must honour ctx: a cancelled run has
	// to abort an in-flight request rather than wait for it.
	Complete(ctx context.Context, req Request) (Response, error)
	// Name identifies the provider for circuit-breaker and metric keys.
	Name() string
}

// RetryableError marks a failure worth trying again: a timeout, a 429, a 5xx.
// Sorting errors into retryable and terminal at the point they occur is what
// lets the retry logic decide correctly later, instead of guessing from a
// string.
type RetryableError struct {
	Status int
	Err    error
}

func (e *RetryableError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("retryable provider error (status %d): %v", e.Status, e.Err)
	}
	return fmt.Sprintf("retryable provider error: %v", e.Err)
}

func (e *RetryableError) Unwrap() error { return e.Err }

// IsRetryable reports whether err, or anything it wraps, is retryable.
func IsRetryable(err error) bool {
	var r *RetryableError
	return errors.As(err, &r)
}
