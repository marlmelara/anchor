package agent

import (
	"encoding/json"
	"strings"
	"testing"
)

func parseErr(t *testing.T, raw, want string) {
	t.Helper()
	_, err := Parse(json.RawMessage(raw))
	if err == nil {
		t.Fatalf("Parse accepted an invalid graph; wanted an error containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not contain %q", err.Error(), want)
	}
}

func TestParseAcceptsTheSpecExample(t *testing.T) {
	g, err := Parse(json.RawMessage(`{
	  "agent": "research", "v": 1,
	  "nodes": [
	    {"id": "n1", "type": "agent_loop", "max_iterations": 8, "model": "claude-sonnet-4-6",
	     "system": "You are a research assistant.", "tools": ["web_fetch", "summarize"]},
	    {"id": "n2", "type": "model_call", "model": "claude-sonnet-4-6", "prompt": "Format {{n1.output}} as JSON"}
	  ]
	}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(g.Nodes) != 2 {
		t.Fatalf("len(Nodes) = %d, want 2", len(g.Nodes))
	}
	if g.Nodes[0].MaxIterations != 8 {
		t.Errorf("max_iterations = %d, want 8", g.Nodes[0].MaxIterations)
	}
}

// Node ids address output in templates, so a duplicate makes {{n1.output}}
// ambiguous.
func TestParseRejectsDuplicateNodeID(t *testing.T) {
	parseErr(t, `{"agent":"a","v":1,"nodes":[
	  {"id":"n1","type":"model_call","model":"m","prompt":"x"},
	  {"id":"n1","type":"model_call","model":"m","prompt":"y"}]}`, "duplicate node id")
}

func TestParseRejectsUnknownNodeType(t *testing.T) {
	parseErr(t, `{"agent":"a","v":1,"nodes":[{"id":"n1","type":"summon_demon"}]}`, "unknown node type")
}

func TestParseRejectsUnknownFields(t *testing.T) {
	// A typo in a field name would otherwise be silently ignored and the node
	// would run with a default the author never intended.
	parseErr(t, `{"agent":"a","v":1,"nodes":[
	  {"id":"n1","type":"model_call","model":"m","prompt":"x","promt":"typo"}]}`, "unknown field")
}

func TestParseRejectsMissingRequirements(t *testing.T) {
	parseErr(t, `{"agent":"a","v":1,"nodes":[{"id":"n1","type":"model_call","model":"m"}]}`,
		"model_call requires a prompt")
	parseErr(t, `{"agent":"a","v":1,"nodes":[{"id":"n1","type":"agent_loop","model":"m"}]}`,
		"max_iterations >= 1")
	parseErr(t, `{"agent":"a","v":1,"nodes":[{"id":"n1","type":"tool_call"}]}`,
		"tool_call requires a tool")
}

// A graph is user input. Without a ceiling, one agent_loop could spend an
// entire token budget in a loop its author never intended.
func TestParseRejectsRunawayLoop(t *testing.T) {
	parseErr(t, `{"agent":"a","v":1,"nodes":[
	  {"id":"n1","type":"agent_loop","model":"m","max_iterations":100000}]}`, "exceeds the cap")
}

func TestParseRejectsDanglingBranchTarget(t *testing.T) {
	parseErr(t, `{"agent":"a","v":1,"nodes":[
	  {"id":"n1","type":"branch","condition":"$.x","if_true":"nowhere"}]}`, "unknown node")
}

func TestParseRejectsFutureGraphVersion(t *testing.T) {
	parseErr(t, `{"agent":"a","v":99,"nodes":[{"id":"n1","type":"model_call","model":"m","prompt":"x"}]}`,
		"newer than this build understands")
}

func TestParseRejectsEmptyGraph(t *testing.T) {
	parseErr(t, `{"agent":"a","v":1,"nodes":[]}`, "at least one node")
}

// ---------------------------------------------------------------------------
// Template rendering
// ---------------------------------------------------------------------------

func TestRenderSubstitutesNodeOutput(t *testing.T) {
	scope := map[string]json.RawMessage{
		"n1":    json.RawMessage(`{"output":"rayleigh scattering"}`),
		"input": json.RawMessage(`{"topic":"sky"}`),
	}
	got, err := Render("Format {{n1.output}} about {{input.topic}} as JSON", scope)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := "Format rayleigh scattering about sky as JSON"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderKeepsNonStringsAsJSON(t *testing.T) {
	scope := map[string]json.RawMessage{"n1": json.RawMessage(`{"output":{"score":0.9}}`)}
	got, err := Render("{{n1.output}}", scope)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	// A model should see real JSON, not Go's map formatting.
	if got != `{"score":0.9}` {
		t.Errorf("got %q, want JSON", got)
	}
}

func TestRenderFailsOnUnknownReference(t *testing.T) {
	_, err := Render("{{n9.output}}", map[string]json.RawMessage{})
	if err == nil {
		t.Fatal("Render accepted a reference to a node that does not exist")
	}
	if !strings.Contains(err.Error(), "n9") {
		t.Errorf("error %q should name the missing reference", err.Error())
	}
}

func TestRenderFailsOnUnknownField(t *testing.T) {
	scope := map[string]json.RawMessage{"n1": json.RawMessage(`{"output":"x"}`)}
	if _, err := Render("{{n1.nope}}", scope); err == nil {
		t.Error("Render accepted a reference to a field that does not exist")
	}
}

// Substitution only, never text/template: conditionals and loops in a prompt
// would be executable logic by another name, and would reintroduce the
// non-determinism the declarative model exists to avoid.
func TestRenderLeavesNonReferenceBracesAlone(t *testing.T) {
	scope := map[string]json.RawMessage{"n1": json.RawMessage(`{"output":"x"}`)}
	got, err := Render(`{"literal": "{{n1.output}}", "untouched": "{}"}`, scope)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(got, `"literal": "x"`) || !strings.Contains(got, `"untouched": "{}"`) {
		t.Errorf("got %q", got)
	}
}

func TestRenderIsIdempotentWithNoReferences(t *testing.T) {
	got, err := Render("no references here", nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got != "no references here" {
		t.Errorf("got %q", got)
	}
}
