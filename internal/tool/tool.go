// Package tool is the seam between the worker and tool execution.
//
// M4 replaces the runner here with one Docker container per call. The interface
// is defined now so the worker is written against it from the start and the
// sandbox is a swap rather than a rewrite.
package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Definition describes a tool the model may call.
type Definition struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`

	// Idempotent says whether running this tool twice is harmless. It decides
	// what happens when a worker dies inside the ambiguous window -- after the
	// effect was attempted but before the result was journaled. An idempotent
	// tool is retried; a non-idempotent one halts the run for review rather
	// than guessing whether the effect landed.
	Idempotent bool `json:"idempotent"`

	InputSchema  json.RawMessage `json:"input_schema,omitempty"`
	OutputSchema json.RawMessage `json:"output_schema,omitempty"`

	// Image, Command, Timeout and the resource caps are honoured by the Docker
	// runner; the mock runner ignores them.
	Image string `json:"image,omitempty"`
	// Command overrides the image's default entrypoint. The process must read
	// its JSON input from stdin and write JSON to stdout.
	Command     []string      `json:"command,omitempty"`
	Timeout     time.Duration `json:"timeout,omitempty"`
	MemoryBytes int64         `json:"memory_bytes,omitempty"`
	NanoCPUs    int64         `json:"nano_cpus,omitempty"`
	Egress      bool          `json:"egress,omitempty"`
}

// Result is what a tool produced.
type Result struct {
	Output   json.RawMessage `json:"output"`
	ExitCode int             `json:"exit_code"`
	Stderr   string          `json:"stderr,omitempty"`
}

// Runner executes tools.
type Runner interface {
	Run(ctx context.Context, def Definition, input json.RawMessage) (Result, error)
}

// Registry maps tool names to definitions.
type Registry struct {
	mu    sync.RWMutex
	tools map[string]Definition
}

func NewRegistry() *Registry {
	return &Registry{tools: map[string]Definition{}}
}

func (r *Registry) Register(d Definition) error {
	if d.Name == "" {
		return fmt.Errorf("tool name is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[d.Name] = d
	return nil
}

func (r *Registry) Get(name string) (Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.tools[name]
	return d, ok
}

// Names returns the registered tool names in sorted order.
//
// Sorted, not map order: Go deliberately randomises map iteration, so returning
// them unsorted would make two runs of the same graph expose tools to the model
// in different orders -- a source of non-determinism that replay could not
// reproduce.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.tools))
	for name := range r.tools {
		out = append(out, name)
	}
	sortStrings(out)
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
