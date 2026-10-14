package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// SchemaError means a tool's output did not match its declared shape.
//
// It is deliberately a distinct type: a tool that returns the wrong shape has
// FAILED, it has not crashed. The worker turns this into a StepFailed event
// with a readable reason, rather than feeding malformed data to the next model
// call -- which is how one bad tool response becomes an unexplainable agent
// run three steps later.
type SchemaError struct {
	Tool   string
	Reason string
}

func (e *SchemaError) Error() string {
	return fmt.Sprintf("tool %q returned output that does not match its schema: %s", e.Tool, e.Reason)
}

// Validator compiles and caches tool schemas.
//
// Compilation is not free and a tool's schema never changes within a process,
// so each is compiled once. The cache is keyed by tool name.
type Validator struct {
	mu       sync.RWMutex
	compiled map[string]*jsonschema.Schema
}

func NewValidator() *Validator {
	return &Validator{compiled: map[string]*jsonschema.Schema{}}
}

// ValidateOutput checks a tool's output against its declared OutputSchema.
//
// A tool with no declared schema validates trivially. That is a deliberate
// escape hatch rather than an oversight: requiring a schema everywhere would
// make simple tools tedious to write, and the cost is that an undeclared tool's
// output is trusted.
func (v *Validator) ValidateOutput(def Definition, output json.RawMessage) error {
	if len(def.OutputSchema) == 0 {
		return nil
	}

	schema, err := v.schemaFor(def)
	if err != nil {
		return err
	}

	// The output has to be valid JSON before it can be checked against a shape.
	var decoded any
	if len(output) == 0 {
		output = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		return &SchemaError{Tool: def.Name, Reason: fmt.Sprintf("output is not valid JSON: %v", err)}
	}

	if err := schema.Validate(decoded); err != nil {
		return &SchemaError{Tool: def.Name, Reason: compactValidationError(err)}
	}
	return nil
}

// ValidateInput checks an input against a tool's declared InputSchema, so a
// model that invents arguments is caught before a container is started.
func (v *Validator) ValidateInput(def Definition, input json.RawMessage) error {
	if len(def.InputSchema) == 0 {
		return nil
	}
	key := def.Name + "\x00input"

	v.mu.RLock()
	schema, ok := v.compiled[key]
	v.mu.RUnlock()
	if !ok {
		var err error
		schema, err = compile(def.Name+"-input", def.InputSchema)
		if err != nil {
			return err
		}
		v.mu.Lock()
		v.compiled[key] = schema
		v.mu.Unlock()
	}

	var decoded any
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(input, &decoded); err != nil {
		return &SchemaError{Tool: def.Name, Reason: fmt.Sprintf("input is not valid JSON: %v", err)}
	}
	if err := schema.Validate(decoded); err != nil {
		return &SchemaError{Tool: def.Name, Reason: compactValidationError(err)}
	}
	return nil
}

func (v *Validator) schemaFor(def Definition) (*jsonschema.Schema, error) {
	v.mu.RLock()
	schema, ok := v.compiled[def.Name]
	v.mu.RUnlock()
	if ok {
		return schema, nil
	}

	schema, err := compile(def.Name, def.OutputSchema)
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	v.compiled[def.Name] = schema
	v.mu.Unlock()
	return schema, nil
}

func compile(name string, raw json.RawMessage) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("tool %q has an unparseable schema: %w", name, err)
	}
	c := jsonschema.NewCompiler()
	resource := "anchor://tool/" + name
	if err := c.AddResource(resource, doc); err != nil {
		return nil, fmt.Errorf("tool %q has an invalid schema: %w", name, err)
	}
	schema, err := c.Compile(resource)
	if err != nil {
		return nil, fmt.Errorf("tool %q has an invalid schema: %w", name, err)
	}
	return schema, nil
}

// compactValidationError flattens the library's multi-line output into one line
// that fits in an event payload and a log message.
func compactValidationError(err error) string {
	msg := strings.ReplaceAll(err.Error(), "\n", "; ")
	msg = strings.Join(strings.Fields(msg), " ")
	const max = 400
	if len(msg) > max {
		msg = msg[:max] + "..."
	}
	return msg
}

// ValidatingRunner wraps a Runner and enforces schemas around it.
//
// Validation sits here rather than inside each Runner so the Docker sandbox,
// the mock runner, and any future runner all get it identically, and so the
// rule is written once.
type ValidatingRunner struct {
	Inner     Runner
	Validator *Validator
}

// NewValidatingRunner wraps inner with schema enforcement.
func NewValidatingRunner(inner Runner) *ValidatingRunner {
	return &ValidatingRunner{Inner: inner, Validator: NewValidator()}
}

func (r *ValidatingRunner) Run(ctx context.Context, def Definition, input json.RawMessage) (Result, error) {
	if err := r.Validator.ValidateInput(def, input); err != nil {
		return Result{}, err
	}
	res, err := r.Inner.Run(ctx, def, input)
	if err != nil {
		return res, err
	}
	// A non-zero exit is already a failure; its stdout is not expected to match
	// the success schema.
	if res.ExitCode != 0 {
		return res, nil
	}
	if err := r.Validator.ValidateOutput(def, res.Output); err != nil {
		return res, err
	}
	return res, nil
}
