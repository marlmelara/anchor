package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// Anthropic calls the real Claude API.
//
// It satisfies the same Provider interface as the mock, so the worker is
// identical in both cases -- which is the whole reason the interface exists.
// Swapping this for Mock is how benchmarks measure Anchor rather than the
// model, and swapping it for the replay source (M6) is how a recorded run
// reproduces without touching the network.
type Anthropic struct {
	client anthropic.Client

	// MaxTokens bounds one call when the request does not set its own.
	MaxTokens int
}

// AnthropicConfig configures the adapter.
type AnthropicConfig struct {
	// APIKey is optional. Left empty, the SDK resolves credentials itself --
	// ANTHROPIC_API_KEY, then ANTHROPIC_AUTH_TOKEN, then a profile written by
	// `ant auth login`. An unset env var does not mean there are no credentials.
	APIKey string
	// BaseURL points the client at a different host. Tests use it to aim at a
	// local server; it is also how a proxy or a self-hosted gateway is reached.
	BaseURL string
	// Timeout bounds a single HTTP request. The worker also wraps each call in
	// its own context deadline; whichever fires first wins.
	Timeout time.Duration
	// MaxRetries is the SDK's own retry count. Anchor retries at the step level
	// too and journals each attempt, so this stays low -- a retry the SDK
	// performs internally is invisible to the journal.
	MaxRetries int
}

// NewAnthropic builds the adapter.
func NewAnthropic(cfg AnthropicConfig) *Anthropic {
	opts := []option.RequestOption{}
	if cfg.APIKey != "" {
		opts = append(opts, option.WithAPIKey(cfg.APIKey))
	}
	if cfg.BaseURL != "" {
		opts = append(opts, option.WithBaseURL(cfg.BaseURL))
	}
	if cfg.Timeout > 0 {
		opts = append(opts, option.WithRequestTimeout(cfg.Timeout))
	}
	// Default 0, not the SDK's 2: Anchor's own retry logic journals every
	// attempt, and an invisible internal retry would make the event log
	// disagree with what actually happened on the wire.
	opts = append(opts, option.WithMaxRetries(cfg.MaxRetries))

	return &Anthropic{client: anthropic.NewClient(opts...), MaxTokens: 4096}
}

func (a *Anthropic) Name() string { return "anthropic" }

// Complete performs one model call.
func (a *Anthropic) Complete(ctx context.Context, req Request) (Response, error) {
	if req.Model == "" {
		return Response{}, errors.New("anthropic: model is required")
	}
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = a.MaxTokens
	}

	params := anthropic.MessageNewParams{
		Model:     anthropic.Model(req.Model),
		MaxTokens: int64(maxTokens),
		Messages:  toSDKMessages(req.Messages),
	}
	if req.System != "" {
		params.System = []anthropic.TextBlockParam{{Text: req.System}}
	}
	if len(req.Tools) > 0 {
		params.Tools = toSDKTools(req.Tools)
	}

	msg, err := a.client.Messages.New(ctx, params)
	if err != nil {
		return Response{}, classify(err)
	}

	return fromSDKMessage(msg)
}

// toSDKMessages maps Anchor's conversation onto the SDK's.
//
// Tool results become user messages carrying a tool_result block, which is the
// shape the API expects -- a tool's output is something the user side of the
// conversation reports back, not something the assistant said.
func toSDKMessages(messages []Message) []anthropic.MessageParam {
	out := make([]anthropic.MessageParam, 0, len(messages))
	for _, m := range messages {
		switch m.Role {
		case RoleAssistant:
			if m.Text == "" {
				continue
			}
			out = append(out, anthropic.NewAssistantMessage(anthropic.NewTextBlock(m.Text)))
		case RoleTool:
			result := string(m.ToolResult)
			if result == "" {
				result = "{}"
			}
			out = append(out, anthropic.NewUserMessage(
				anthropic.NewToolResultBlock(m.ToolName, result, false),
			))
		default:
			out = append(out, anthropic.NewUserMessage(anthropic.NewTextBlock(m.Text)))
		}
	}
	return out
}

func toSDKTools(specs []ToolSpec) []anthropic.ToolUnionParam {
	out := make([]anthropic.ToolUnionParam, 0, len(specs))
	for _, s := range specs {
		schema := anthropic.ToolInputSchemaParam{}
		if len(s.InputSchema) > 0 {
			var parsed map[string]any
			if err := json.Unmarshal(s.InputSchema, &parsed); err == nil {
				if props, ok := parsed["properties"]; ok {
					schema.Properties = props
				}
				if req, ok := parsed["required"].([]any); ok {
					required := make([]string, 0, len(req))
					for _, r := range req {
						if name, ok := r.(string); ok {
							required = append(required, name)
						}
					}
					schema.Required = required
				}
			}
		}
		out = append(out, anthropic.ToolUnionParam{
			OfTool: &anthropic.ToolParam{
				Name:        s.Name,
				Description: anthropic.String(s.Description),
				InputSchema: schema,
			},
		})
	}
	return out
}

// fromSDKMessage extracts what Anchor needs and keeps the whole body.
//
// Raw carries the complete response, not a summary of it. That is what a replay
// returns in place of calling the provider, so anything dropped here is lost to
// every future replay of this run.
func fromSDKMessage(msg *anthropic.Message) (Response, error) {
	resp := Response{
		PromptTokens:     msg.Usage.InputTokens,
		CompletionTokens: msg.Usage.OutputTokens,
		Raw:              json.RawMessage(msg.RawJSON()),
	}

	for _, block := range msg.Content {
		switch b := block.AsAny().(type) {
		case anthropic.TextBlock:
			resp.Text += b.Text
		case anthropic.ToolUseBlock:
			// Only the first tool call is acted on. Anchor's execution model is
			// one step per effect, and the step index is assigned by the walk,
			// so a parallel batch would need several steps from one response --
			// a change to the interpreter, not just to this adapter.
			if resp.ToolCall == nil {
				resp.ToolCall = &ToolCall{
					Name:  b.Name,
					Input: json.RawMessage(b.Input),
				}
			}
		}
	}

	// A refusal is an HTTP 200 with a stop reason, not an error. Treating it as
	// success would feed an empty response into the next step.
	if msg.StopReason == anthropic.StopReasonRefusal {
		return resp, &RefusalError{
			Category:    string(msg.StopDetails.Category),
			Explanation: msg.StopDetails.Explanation,
		}
	}

	return resp, nil
}

// RefusalError means a safety classifier declined the request. It is terminal,
// not retryable: the same request will be declined again.
type RefusalError struct {
	Category    string
	Explanation string
}

func (e *RefusalError) Error() string {
	if e.Explanation != "" {
		return fmt.Sprintf("provider refused the request (%s): %s", e.Category, e.Explanation)
	}
	return fmt.Sprintf("provider refused the request (%s)", e.Category)
}

// classify sorts provider errors into retryable and terminal.
//
// Doing this at the point the error occurs is what lets the retry logic decide
// correctly later, instead of string-matching a message three layers up.
func classify(err error) error {
	var apiErr *anthropic.Error
	if !errors.As(err, &apiErr) {
		// No HTTP status: a connection failure, a DNS problem, a timeout. All
		// worth trying again.
		return &RetryableError{Err: err}
	}

	switch {
	case apiErr.StatusCode == http.StatusTooManyRequests,
		apiErr.StatusCode == http.StatusRequestTimeout,
		apiErr.StatusCode >= 500:
		return &RetryableError{Status: apiErr.StatusCode, Err: err}
	default:
		// 400, 401, 403, 404: the request itself is wrong. Retrying sends the
		// same wrong request and wastes the budget.
		return err
	}
}
