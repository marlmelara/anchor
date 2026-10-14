package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// MockRunner executes tools in-process. M4 replaces it with the Docker sandbox.
type MockRunner struct {
	Latency time.Duration

	mu    sync.RWMutex
	funcs map[string]func(ctx context.Context, input json.RawMessage) (Result, error)
}

func NewMockRunner() *MockRunner {
	return &MockRunner{funcs: map[string]func(context.Context, json.RawMessage) (Result, error){}}
}

// Handle registers an implementation for a tool name.
func (m *MockRunner) Handle(name string, fn func(ctx context.Context, input json.RawMessage) (Result, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.funcs[name] = fn
}

func (m *MockRunner) Run(ctx context.Context, def Definition, input json.RawMessage) (Result, error) {
	m.mu.RLock()
	fn, ok := m.funcs[def.Name]
	latency := m.Latency
	m.mu.RUnlock()

	if latency > 0 {
		timer := time.NewTimer(latency)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-timer.C:
		}
	}
	if !ok {
		return Result{}, fmt.Errorf("no mock implementation for tool %q", def.Name)
	}
	return fn(ctx, input)
}

// Counter is the tool the effect-safety test is built on.
//
// It counts how many times its effect actually fired. The chaos suite kills
// workers repeatedly and then asserts the count equals the number of logical
// steps -- which is the only way to demonstrate "zero duplicate effects under
// fault injection" rather than assert it.
type Counter struct {
	n atomic.Int64
}

// Count is the number of times the effect has fired.
func (c *Counter) Count() int64 { return c.n.Load() }

// Definition returns a tool definition for this counter.
//
// Idempotent is false on purpose: a counter is the clearest possible example of
// an effect that must not repeat, so it exercises the strict path rather than
// the forgiving one.
func (c *Counter) Definition(name string) Definition {
	return Definition{
		Name:        name,
		Description: "increments a counter; used to prove effects do not duplicate",
		Idempotent:  false,
		OutputSchema: json.RawMessage(
			`{"type":"object","properties":{"count":{"type":"integer"}},"required":["count"]}`),
	}
}

// Func is the implementation to register with a MockRunner.
func (c *Counter) Func() func(context.Context, json.RawMessage) (Result, error) {
	return func(ctx context.Context, input json.RawMessage) (Result, error) {
		n := c.n.Add(1)
		return Result{
			Output:   json.RawMessage(fmt.Sprintf(`{"count":%d}`, n)),
			ExitCode: 0,
		}, nil
	}
}

// Echo returns its input, for tests that need a tool with no side effect.
func Echo(name string) (Definition, func(context.Context, json.RawMessage) (Result, error)) {
	def := Definition{
		Name:       name,
		Idempotent: true,
	}
	fn := func(ctx context.Context, input json.RawMessage) (Result, error) {
		if len(input) == 0 {
			input = json.RawMessage(`{}`)
		}
		return Result{Output: input, ExitCode: 0}, nil
	}
	return def, fn
}
