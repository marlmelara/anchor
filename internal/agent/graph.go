// Package agent defines the declarative graph an Anchor agent is made of.
//
// An agent is data, not code: the SDK produces this JSON, the Go worker
// interprets it node by node. Nothing here executes user logic, and that is the
// single most important structural decision in the project -- replay depends on
// it. Because the node sequence is fixed and versioned, a replay walks the same
// nodes by construction, rather than by policing user code for calls to the
// clock or a random number generator.
package agent

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// NodeType is the closed set of things a graph can do. Adding one is a
// deliberate act: every node type is a shape the interpreter, the trace viewer
// and the replay verifier all have to understand forever.
type NodeType string

const (
	// NodeModelCall is one LLM call with a rendered prompt template.
	NodeModelCall NodeType = "model_call"
	// NodeToolCall is one sandboxed tool invocation.
	NodeToolCall NodeType = "tool_call"
	// NodeAgentLoop is the core node: call the model with tools exposed; if it
	// picks a tool, run it, append the result, and repeat.
	NodeAgentLoop NodeType = "agent_loop"
	// NodeBranch picks the next node from a condition over earlier output.
	NodeBranch NodeType = "branch"
	// NodeMap runs one child node over each item of an array, sequentially.
	NodeMap NodeType = "map"
)

// Node is one step in the plan. Fields are shared across types rather than
// split into a union, because the graph is stored as jsonb and a flat shape
// keeps it queryable.
type Node struct {
	ID   string   `json:"id"`
	Type NodeType `json:"type"`

	// model_call
	Prompt string `json:"prompt,omitempty"`

	// model_call, agent_loop
	Model  string `json:"model,omitempty"`
	System string `json:"system,omitempty"`

	// agent_loop
	Tools         []string `json:"tools,omitempty"`
	MaxIterations int      `json:"max_iterations,omitempty"`

	// tool_call
	Tool  string          `json:"tool,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`

	// branch
	Condition string `json:"condition,omitempty"`
	IfTrue    string `json:"if_true,omitempty"`
	IfFalse   string `json:"if_false,omitempty"`

	// map
	Over  string `json:"over,omitempty"`
	Child string `json:"child,omitempty"`
}

// Graph is a complete agent definition as stored in agents.graph.
type Graph struct {
	Agent string `json:"agent"`
	V     int    `json:"v"`
	Nodes []Node `json:"nodes"`
}

// CurrentVersion is the graph schema version this build writes.
const CurrentVersion = 1

// MaxIterationsCap bounds agent_loop regardless of what the graph asks for. A
// graph is user input; without a ceiling here, one agent could spend an entire
// token budget in a loop the author did not intend.
const MaxIterationsCap = 50

// Parse decodes and validates a graph.
//
// Validation happens once, at registration, rather than during a run: a graph
// that fails here is rejected at the API boundary, where the caller can fix it,
// instead of failing a run halfway through and burning the money already spent.
func Parse(raw json.RawMessage) (*Graph, error) {
	var g Graph
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&g); err != nil {
		return nil, fmt.Errorf("decode graph: %w", err)
	}
	if err := g.Validate(); err != nil {
		return nil, err
	}
	return &g, nil
}

var nodeIDPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,63}$`)

// Validate checks every structural rule the interpreter later relies on, so the
// interpreter can assume a well-formed graph and never has to decide what a
// malformed one means mid-run.
func (g *Graph) Validate() error {
	if g.Agent == "" {
		return fmt.Errorf("graph: agent name is required")
	}
	if g.V < 1 {
		return fmt.Errorf("graph: v is required and must be >= 1")
	}
	if g.V > CurrentVersion {
		return fmt.Errorf("graph: v=%d is newer than this build understands (max %d)", g.V, CurrentVersion)
	}
	if len(g.Nodes) == 0 {
		return fmt.Errorf("graph: at least one node is required")
	}

	seen := make(map[string]NodeType, len(g.Nodes))
	for i, n := range g.Nodes {
		if !nodeIDPattern.MatchString(n.ID) {
			return fmt.Errorf("graph: node %d has invalid id %q (letters, digits, underscore; must start with a letter)", i, n.ID)
		}
		if _, dup := seen[n.ID]; dup {
			// Node ids address output in templates ({{n1.output}}), so a
			// duplicate would make a reference ambiguous.
			return fmt.Errorf("graph: duplicate node id %q", n.ID)
		}
		seen[n.ID] = n.Type
	}

	for _, n := range g.Nodes {
		if err := n.validate(seen); err != nil {
			return fmt.Errorf("graph: node %q: %w", n.ID, err)
		}
	}
	return nil
}

func (n Node) validate(nodes map[string]NodeType) error {
	switch n.Type {
	case NodeModelCall:
		if n.Prompt == "" {
			return fmt.Errorf("model_call requires a prompt")
		}
		if n.Model == "" {
			return fmt.Errorf("model_call requires a model")
		}
	case NodeToolCall:
		if n.Tool == "" {
			return fmt.Errorf("tool_call requires a tool")
		}
		if len(n.Input) > 0 && !json.Valid(n.Input) {
			return fmt.Errorf("tool_call input is not valid JSON")
		}
	case NodeAgentLoop:
		if n.Model == "" {
			return fmt.Errorf("agent_loop requires a model")
		}
		if n.MaxIterations < 1 {
			return fmt.Errorf("agent_loop requires max_iterations >= 1")
		}
		if n.MaxIterations > MaxIterationsCap {
			return fmt.Errorf("agent_loop max_iterations %d exceeds the cap of %d", n.MaxIterations, MaxIterationsCap)
		}
	case NodeBranch:
		if n.Condition == "" {
			return fmt.Errorf("branch requires a condition")
		}
		for _, target := range []string{n.IfTrue, n.IfFalse} {
			if target == "" {
				continue
			}
			if _, ok := nodes[target]; !ok {
				return fmt.Errorf("branch targets unknown node %q", target)
			}
		}
	case NodeMap:
		if n.Over == "" {
			return fmt.Errorf("map requires an 'over' expression")
		}
		if _, ok := nodes[n.Child]; !ok {
			return fmt.Errorf("map targets unknown child node %q", n.Child)
		}
	default:
		return fmt.Errorf("unknown node type %q", n.Type)
	}
	return nil
}

// Node returns a node by id.
func (g *Graph) Node(id string) (Node, bool) {
	for _, n := range g.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return Node{}, false
}

// ---------------------------------------------------------------------------
// Template rendering
// ---------------------------------------------------------------------------

// templateRef matches {{n1.output}} and {{input.topic}}.
var templateRef = regexp.MustCompile(`\{\{\s*([a-zA-Z][a-zA-Z0-9_]*)\.([a-zA-Z0-9_.]+)\s*\}\}`)

// Render substitutes {{node.field}} references in a prompt template.
//
// Deliberately not text/template: that would let a graph author embed
// conditionals and range loops, which is executable logic by another name and
// would reintroduce exactly the non-determinism the declarative model exists to
// avoid. Substitution only.
//
// scope maps a reference root ("n1", "input") to the JSON value it stands for.
func Render(tmpl string, scope map[string]json.RawMessage) (string, error) {
	var firstErr error
	out := templateRef.ReplaceAllStringFunc(tmpl, func(match string) string {
		groups := templateRef.FindStringSubmatch(match)
		root, path := groups[1], groups[2]

		raw, ok := scope[root]
		if !ok {
			if firstErr == nil {
				firstErr = fmt.Errorf("template references %q, which is not available here", root)
			}
			return match
		}
		val, err := lookup(raw, path)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("template %s: %w", match, err)
			}
			return match
		}
		return val
	})
	if firstErr != nil {
		return "", firstErr
	}
	return out, nil
}

// lookup walks a dotted path into a JSON value and renders the result as text.
func lookup(raw json.RawMessage, path string) (string, error) {
	var cur any
	if err := json.Unmarshal(raw, &cur); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}

	for _, part := range strings.Split(path, ".") {
		// "output" on a value that is not an object means the value itself:
		// a model_call whose result is a bare string still answers {{n1.output}}.
		obj, ok := cur.(map[string]any)
		if !ok {
			if part == "output" {
				continue
			}
			return "", fmt.Errorf("cannot read %q from a non-object", part)
		}
		next, ok := obj[part]
		if !ok {
			return "", fmt.Errorf("no field %q", part)
		}
		cur = next
	}

	// Strings interpolate bare; everything else keeps its JSON form so a model
	// sees real JSON rather than Go's map formatting.
	if s, ok := cur.(string); ok {
		return s, nil
	}
	encoded, err := json.Marshal(cur)
	if err != nil {
		return "", fmt.Errorf("encode: %w", err)
	}
	return string(encoded), nil
}
