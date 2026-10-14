package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marlmelara/anchor/internal/provider"
)

// These tests point the real SDK at a local server speaking the Anthropic wire
// format. That checks everything Anchor owns -- request mapping, response
// extraction, raw capture, error classification -- without spending money or
// needing a key in CI. What it cannot check is whether the real API agrees with
// this wire format; that is what the live check in the README is for.
func serve(t *testing.T, handler http.HandlerFunc) *provider.Anthropic {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return provider.NewAnthropic(provider.AnthropicConfig{
		APIKey:  "test-key-not-real",
		BaseURL: srv.URL,
	})
}

func jsonResponse(t *testing.T, status int, body string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

const textReply = `{
  "id": "msg_01abc",
  "type": "message",
  "role": "assistant",
  "model": "claude-opus-5-5",
  "content": [{"type": "text", "text": "Rayleigh scattering."}],
  "stop_reason": "end_turn",
  "usage": {"input_tokens": 120, "output_tokens": 80}
}`

func TestAnthropicExtractsTextAndUsage(t *testing.T) {
	p := serve(t, jsonResponse(t, 200, textReply))

	resp, err := p.Complete(context.Background(), provider.Request{
		Model:    "claude-opus-5-5",
		Messages: []provider.Message{{Role: provider.RoleUser, Text: "why is the sky blue"}},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Text != "Rayleigh scattering." {
		t.Errorf("Text = %q", resp.Text)
	}
	if resp.PromptTokens != 120 || resp.CompletionTokens != 80 {
		t.Errorf("usage = %d/%d, want 120/80", resp.PromptTokens, resp.CompletionTokens)
	}
	if resp.TotalTokens() != 200 {
		t.Errorf("TotalTokens = %d, want 200", resp.TotalTokens())
	}
}

// Raw must hold the complete body, not a summary. Whatever is dropped here is
// lost to every future replay of this run.
func TestAnthropicKeepsTheWholeResponseBody(t *testing.T) {
	p := serve(t, jsonResponse(t, 200, textReply))

	resp, err := p.Complete(context.Background(), provider.Request{
		Model:    "claude-opus-5-5",
		Messages: []provider.Message{{Role: provider.RoleUser, Text: "x"}},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(resp.Raw, &raw); err != nil {
		t.Fatalf("Raw is not JSON: %v", err)
	}
	for _, field := range []string{"id", "model", "content", "stop_reason", "usage"} {
		if _, ok := raw[field]; !ok {
			t.Errorf("Raw is missing %q; a replay would not see what the original run saw", field)
		}
	}
}

func TestAnthropicExtractsAToolCall(t *testing.T) {
	const body = `{
	  "id": "msg_02", "type": "message", "role": "assistant", "model": "claude-opus-5-5",
	  "content": [
	    {"type": "text", "text": "Let me look that up."},
	    {"type": "tool_use", "id": "toolu_01", "name": "web_fetch", "input": {"url": "https://example.com"}}
	  ],
	  "stop_reason": "tool_use",
	  "usage": {"input_tokens": 50, "output_tokens": 20}
	}`
	p := serve(t, jsonResponse(t, 200, body))

	resp, err := p.Complete(context.Background(), provider.Request{
		Model:    "claude-opus-5-5",
		Messages: []provider.Message{{Role: provider.RoleUser, Text: "look it up"}},
		Tools:    []provider.ToolSpec{{Name: "web_fetch", Description: "fetch a page"}},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.ToolCall == nil {
		t.Fatal("ToolCall is nil")
	}
	if resp.ToolCall.Name != "web_fetch" {
		t.Errorf("tool = %q, want web_fetch", resp.ToolCall.Name)
	}
	var input map[string]string
	if err := json.Unmarshal(resp.ToolCall.Input, &input); err != nil {
		t.Fatalf("tool input is not JSON: %v", err)
	}
	if input["url"] != "https://example.com" {
		t.Errorf("input = %v", input)
	}
	if resp.Text != "Let me look that up." {
		t.Errorf("text alongside the tool call was lost: %q", resp.Text)
	}
}

// 429 and 5xx may succeed if tried again; 4xx will not. Sorting them here is
// what lets the retry logic decide correctly, instead of string-matching a
// message three layers up.
func TestAnthropicClassifiesRetryableStatuses(t *testing.T) {
	cases := []struct {
		status    int
		retryable bool
	}{
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
		{http.StatusServiceUnavailable, true},
		{http.StatusBadRequest, false},
		{http.StatusUnauthorized, false},
		{http.StatusNotFound, false},
	}
	for _, tc := range cases {
		p := serve(t, jsonResponse(t, tc.status,
			`{"type":"error","error":{"type":"x","message":"boom"}}`))

		_, err := p.Complete(context.Background(), provider.Request{
			Model:    "claude-opus-5-5",
			Messages: []provider.Message{{Role: provider.RoleUser, Text: "x"}},
		})
		if err == nil {
			t.Errorf("status %d: no error returned", tc.status)
			continue
		}
		if got := provider.IsRetryable(err); got != tc.retryable {
			t.Errorf("status %d: IsRetryable = %v, want %v (err: %v)", tc.status, got, tc.retryable, err)
		}
	}
}

// A connection failure has no HTTP status at all, and is always worth retrying.
func TestAnthropicTreatsConnectionFailuresAsRetryable(t *testing.T) {
	p := provider.NewAnthropic(provider.AnthropicConfig{
		APIKey:  "test-key-not-real",
		BaseURL: "http://127.0.0.1:1", // nothing listens here
	})
	_, err := p.Complete(context.Background(), provider.Request{
		Model:    "claude-opus-5-5",
		Messages: []provider.Message{{Role: provider.RoleUser, Text: "x"}},
	})
	if err == nil {
		t.Fatal("connecting to a closed port succeeded")
	}
	if !provider.IsRetryable(err) {
		t.Errorf("a connection failure was classified as terminal: %v", err)
	}
}

// A refusal arrives as HTTP 200 with a stop reason. Treating it as success
// would feed an empty response into the next step.
func TestAnthropicSurfacesARefusal(t *testing.T) {
	const body = `{
	  "id": "msg_03", "type": "message", "role": "assistant", "model": "claude-opus-5-5",
	  "content": [],
	  "stop_reason": "refusal",
	  "stop_details": {"type": "refusal", "category": "cyber", "explanation": "declined"},
	  "usage": {"input_tokens": 10, "output_tokens": 0}
	}`
	p := serve(t, jsonResponse(t, 200, body))

	_, err := p.Complete(context.Background(), provider.Request{
		Model:    "claude-opus-5-5",
		Messages: []provider.Message{{Role: provider.RoleUser, Text: "x"}},
	})
	var refusal *provider.RefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("err = %v, want a RefusalError", err)
	}
	if refusal.Category != "cyber" {
		t.Errorf("Category = %q, want cyber", refusal.Category)
	}
	// Terminal, not retryable: the same request would be declined again.
	if provider.IsRetryable(err) {
		t.Error("a refusal was classified as retryable")
	}
}

// The system prompt, the conversation and the tool list must all reach the
// wire. A silently dropped system prompt is the kind of bug that looks like a
// model quality problem.
func TestAnthropicSendsSystemMessagesAndTools(t *testing.T) {
	var captured map[string]any
	p := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&captured)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(textReply))
	})

	_, err := p.Complete(context.Background(), provider.Request{
		Model:     "claude-opus-5-5",
		System:    "You are a research assistant.",
		MaxTokens: 2048,
		Messages: []provider.Message{
			{Role: provider.RoleUser, Text: "find X"},
			{Role: provider.RoleAssistant, Text: "searching"},
			{Role: provider.RoleTool, ToolName: "web_fetch", ToolResult: json.RawMessage(`{"body":"hi"}`)},
		},
		Tools: []provider.ToolSpec{{
			Name:        "web_fetch",
			Description: "fetch a page",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"url":{"type":"string"}},"required":["url"]}`),
		}},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if captured["model"] != "claude-opus-5-5" {
		t.Errorf("model = %v", captured["model"])
	}
	if captured["max_tokens"] != float64(2048) {
		t.Errorf("max_tokens = %v, want 2048", captured["max_tokens"])
	}
	sys, _ := json.Marshal(captured["system"])
	if !strings.Contains(string(sys), "research assistant") {
		t.Errorf("system prompt did not reach the wire: %s", sys)
	}
	msgs, ok := captured["messages"].([]any)
	if !ok || len(msgs) != 3 {
		t.Fatalf("messages = %v, want 3", captured["messages"])
	}
	tools, _ := json.Marshal(captured["tools"])
	if !strings.Contains(string(tools), "web_fetch") {
		t.Errorf("tools did not reach the wire: %s", tools)
	}
	if !strings.Contains(string(tools), "url") {
		t.Errorf("the tool's input schema was dropped: %s", tools)
	}
}

func TestAnthropicRequiresAModel(t *testing.T) {
	p := serve(t, jsonResponse(t, 200, textReply))
	_, err := p.Complete(context.Background(), provider.Request{
		Messages: []provider.Message{{Role: provider.RoleUser, Text: "x"}},
	})
	if err == nil {
		t.Error("a request with no model was accepted")
	}
}

func TestAnthropicHonoursContextCancellation(t *testing.T) {
	p := serve(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // never answer
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := p.Complete(ctx, provider.Request{
		Model:    "claude-opus-5-5",
		Messages: []provider.Message{{Role: provider.RoleUser, Text: "x"}},
	})
	if err == nil {
		t.Fatal("a cancelled context still produced a response")
	}
}

func TestAnthropicName(t *testing.T) {
	p := provider.NewAnthropic(provider.AnthropicConfig{APIKey: "x"})
	if p.Name() != "anthropic" {
		t.Errorf("Name = %q", p.Name())
	}
}
