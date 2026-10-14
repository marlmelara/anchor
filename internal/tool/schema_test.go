package tool_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/marlmelara/anchor/internal/tool"
)

const weatherSchema = `{
  "type": "object",
  "properties": {
    "temp_c": {"type": "number"},
    "summary": {"type": "string"}
  },
  "required": ["temp_c", "summary"],
  "additionalProperties": false
}`

func weatherTool() tool.Definition {
	return tool.Definition{
		Name:         "weather",
		Idempotent:   true,
		OutputSchema: json.RawMessage(weatherSchema),
	}
}

func TestValidatorAcceptsConformingOutput(t *testing.T) {
	v := tool.NewValidator()
	if err := v.ValidateOutput(weatherTool(), json.RawMessage(`{"temp_c":12.5,"summary":"grey"}`)); err != nil {
		t.Errorf("valid output rejected: %v", err)
	}
}

// A tool that returns the wrong shape has failed, not crashed. Catching it here
// is what stops one bad tool response becoming an unexplainable agent run three
// steps later.
func TestValidatorRejectsMissingRequiredField(t *testing.T) {
	v := tool.NewValidator()
	err := v.ValidateOutput(weatherTool(), json.RawMessage(`{"summary":"grey"}`))
	var schemaErr *tool.SchemaError
	if !errors.As(err, &schemaErr) {
		t.Fatalf("err = %v, want a SchemaError", err)
	}
	if schemaErr.Tool != "weather" {
		t.Errorf("Tool = %q, want weather", schemaErr.Tool)
	}
}

func TestValidatorRejectsWrongType(t *testing.T) {
	v := tool.NewValidator()
	err := v.ValidateOutput(weatherTool(), json.RawMessage(`{"temp_c":"hot","summary":"grey"}`))
	if err == nil {
		t.Error("a string where a number was declared was accepted")
	}
}

func TestValidatorRejectsUndeclaredFields(t *testing.T) {
	v := tool.NewValidator()
	err := v.ValidateOutput(weatherTool(),
		json.RawMessage(`{"temp_c":1,"summary":"x","injected":"surprise"}`))
	if err == nil {
		t.Error("additionalProperties:false did not reject an extra field")
	}
}

func TestValidatorRejectsNonJSONOutput(t *testing.T) {
	v := tool.NewValidator()
	err := v.ValidateOutput(weatherTool(), json.RawMessage(`not json at all`))
	if err == nil {
		t.Fatal("non-JSON output was accepted")
	}
	var schemaErr *tool.SchemaError
	if !errors.As(err, &schemaErr) {
		t.Errorf("err = %v, want a SchemaError", err)
	}
}

// A tool with no declared schema validates trivially. This is a deliberate
// escape hatch, and the cost is that such a tool's output is trusted.
func TestValidatorPassesToolsWithNoSchema(t *testing.T) {
	v := tool.NewValidator()
	def := tool.Definition{Name: "anything"}
	if err := v.ValidateOutput(def, json.RawMessage(`{"literally":"anything"}`)); err != nil {
		t.Errorf("a tool with no schema was rejected: %v", err)
	}
}

func TestValidatorRejectsAnUnparseableSchema(t *testing.T) {
	v := tool.NewValidator()
	def := tool.Definition{Name: "broken", OutputSchema: json.RawMessage(`{"type":`)}
	if err := v.ValidateOutput(def, json.RawMessage(`{}`)); err == nil {
		t.Error("a tool declaring a malformed schema was accepted")
	}
}

func TestValidatorChecksInputBeforeTheToolRuns(t *testing.T) {
	v := tool.NewValidator()
	def := tool.Definition{
		Name: "fetch",
		InputSchema: json.RawMessage(
			`{"type":"object","properties":{"url":{"type":"string"}},"required":["url"]}`),
	}
	if err := v.ValidateInput(def, json.RawMessage(`{"url":"https://example.com"}`)); err != nil {
		t.Errorf("valid input rejected: %v", err)
	}
	if err := v.ValidateInput(def, json.RawMessage(`{"ur1":"typo"}`)); err == nil {
		t.Error("a model that invented an argument name was not caught")
	}
}

// ---------------------------------------------------------------------------

type stubRunner struct {
	result tool.Result
	err    error
	calls  int
}

func (s *stubRunner) Run(ctx context.Context, def tool.Definition, input json.RawMessage) (tool.Result, error) {
	s.calls++
	return s.result, s.err
}

func TestValidatingRunnerRejectsBadOutput(t *testing.T) {
	inner := &stubRunner{result: tool.Result{Output: json.RawMessage(`{"summary":"no temp"}`)}}
	r := tool.NewValidatingRunner(inner)

	_, err := r.Run(context.Background(), weatherTool(), json.RawMessage(`{}`))
	var schemaErr *tool.SchemaError
	if !errors.As(err, &schemaErr) {
		t.Fatalf("err = %v, want a SchemaError", err)
	}
}

// A tool that already failed should not also be told its output is the wrong
// shape; the exit code is the real story and a second error would bury it.
func TestValidatingRunnerSkipsValidationOnNonZeroExit(t *testing.T) {
	inner := &stubRunner{result: tool.Result{ExitCode: 3, Output: json.RawMessage(`panic: oh no`)}}
	r := tool.NewValidatingRunner(inner)

	res, err := r.Run(context.Background(), weatherTool(), json.RawMessage(`{}`))
	if err != nil {
		t.Errorf("err = %v, want nil so the caller sees the exit code", err)
	}
	if res.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3", res.ExitCode)
	}
}

func TestValidatingRunnerRejectsBadInputWithoutRunningTheTool(t *testing.T) {
	inner := &stubRunner{result: tool.Result{Output: json.RawMessage(`{}`)}}
	r := tool.NewValidatingRunner(inner)
	def := tool.Definition{
		Name: "fetch",
		InputSchema: json.RawMessage(
			`{"type":"object","properties":{"url":{"type":"string"}},"required":["url"]}`),
	}

	if _, err := r.Run(context.Background(), def, json.RawMessage(`{}`)); err == nil {
		t.Fatal("invalid input was accepted")
	}
	if inner.calls != 0 {
		t.Errorf("the tool ran %d time(s) despite invalid input; validation must happen first", inner.calls)
	}
}
