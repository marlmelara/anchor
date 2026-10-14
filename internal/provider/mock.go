package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Mock is a deterministic stand-in for a real LLM.
//
// It exists for two different jobs. In tests it makes runs predictable: a
// scripted sequence of replies means an assertion can name the exact events a
// run should produce. In benchmarks it removes the model from the measurement
// entirely -- with a fixed latency, throughput and scheduling-latency numbers
// describe Anchor's overhead rather than a provider's response time, which is
// the only way those numbers mean anything on a resume.
type Mock struct {
	// Latency is applied to every call. The build spec fixes it at 50ms for
	// benchmarks so results are comparable run to run.
	Latency time.Duration

	// Script is consumed in order. When it runs out, Default is returned.
	Script  []Response
	Default Response

	// FailFirst makes the first n calls fail with a retryable error, so retry
	// and backoff paths can be exercised without a flaky network.
	FailFirst int

	mu     sync.Mutex
	calls  int
	failed int
}

// NewMock returns a mock that answers every call with a fixed final text.
func NewMock(text string) *Mock {
	return &Mock{
		Latency: 50 * time.Millisecond,
		Default: Response{
			Text:             text,
			PromptTokens:     10,
			CompletionTokens: 5,
			Raw:              json.RawMessage(`{"mock":true}`),
		},
	}
}

func (m *Mock) Name() string { return "mock" }

// Calls reports how many times Complete was invoked. Tests assert on this to
// prove a step ran exactly once despite retries or a worker being killed.
func (m *Mock) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *Mock) Complete(ctx context.Context, req Request) (Response, error) {
	m.mu.Lock()
	m.calls++
	n := m.calls
	shouldFail := m.failed < m.FailFirst
	if shouldFail {
		m.failed++
	}
	var scripted *Response
	if idx := n - 1; idx < len(m.Script) {
		scripted = &m.Script[idx]
	}
	latency := m.Latency
	def := m.Default
	m.mu.Unlock()

	// Honour cancellation while "working". A real provider aborts its HTTP
	// request here; the mock has to behave the same way or cancellation tests
	// would pass against the mock and fail in production.
	if latency > 0 {
		timer := time.NewTimer(latency)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return Response{}, ctx.Err()
		case <-timer.C:
		}
	}

	if shouldFail {
		return Response{}, &RetryableError{
			Status: 503,
			Err:    fmt.Errorf("mock provider: injected failure %d of %d", n, m.FailFirst),
		}
	}
	if scripted != nil {
		return *scripted, nil
	}
	return def, nil
}

// TextReply builds a scripted final answer.
func TextReply(text string, promptTokens, completionTokens int64) Response {
	return Response{
		Text:             text,
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		Raw:              json.RawMessage(fmt.Sprintf(`{"mock":true,"text":%q}`, text)),
	}
}

// ToolReply builds a scripted response in which the model asks for a tool.
func ToolReply(tool string, input json.RawMessage, promptTokens, completionTokens int64) Response {
	return Response{
		ToolCall:         &ToolCall{Name: tool, Input: input},
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		Raw:              json.RawMessage(fmt.Sprintf(`{"mock":true,"tool":%q}`, tool)),
	}
}
