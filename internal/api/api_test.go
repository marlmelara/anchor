package api_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marlmelara/anchor/internal/api"
	"github.com/marlmelara/anchor/internal/metrics"
	"github.com/marlmelara/anchor/internal/store"
	"github.com/marlmelara/anchor/internal/testsupport"
)

func testServer(t *testing.T, cfg api.Config) (*httptest.Server, *store.Store) {
	t.Helper()
	s := testsupport.Store(t, "test_api")
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.New()
	}
	srv := httptest.NewServer(api.New(s, cfg).Routes())
	t.Cleanup(srv.Close)
	return srv, s
}

func post(t *testing.T, srv *httptest.Server, path, body string) (int, string) {
	t.Helper()
	resp, err := srv.Client().Post(srv.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

const simpleGraph = `{"agent":"api-test","v":1,"nodes":[
  {"id":"n1","type":"model_call","model":"mock","prompt":"hello"}]}`

func registerAndSubmit(t *testing.T, srv *httptest.Server) {
	t.Helper()
	status, body := post(t, srv, "/v1/agents",
		fmt.Sprintf(`{"name":"api-test","graph":%s}`, simpleGraph))
	if status != http.StatusCreated {
		t.Fatalf("register agent: %d %s", status, body)
	}
}

// The M5 acceptance criterion: submits return 429 above the high-water mark.
//
// Backpressure is telling producers to slow down when consumers cannot keep up.
// The alternative -- accepting work that will never run -- is worse, because the
// caller believes it was accepted.
func TestSubmitReturns429AboveTheHighWaterMark(t *testing.T) {
	m := metrics.New()
	srv, _ := testServer(t, api.Config{QueueHighWaterMark: 3, Metrics: m})
	registerAndSubmit(t, srv)

	// Fill the queue to the mark. Nothing is consuming, so depth only grows.
	for i := range 3 {
		status, body := post(t, srv, "/v1/runs", `{"agent":"api-test"}`)
		if status != http.StatusAccepted {
			t.Fatalf("submit %d: %d %s", i, status, body)
		}
	}

	status, body := post(t, srv, "/v1/runs", `{"agent":"api-test"}`)
	if status != http.StatusTooManyRequests {
		t.Fatalf("submit at the high-water mark: %d %s, want 429", status, body)
	}
	if !strings.Contains(body, "high-water mark") {
		t.Errorf("429 body does not explain why: %s", body)
	}
}

// A 429 must tell the caller when to come back, or every client invents its own
// retry interval and they all return together.
func TestRejectedSubmitSetsRetryAfter(t *testing.T) {
	srv, _ := testServer(t, api.Config{QueueHighWaterMark: 1})
	registerAndSubmit(t, srv)

	if status, body := post(t, srv, "/v1/runs", `{"agent":"api-test"}`); status != http.StatusAccepted {
		t.Fatalf("first submit: %d %s", status, body)
	}
	resp, err := srv.Client().Post(srv.URL+"/v1/runs", "application/json",
		strings.NewReader(`{"agent":"api-test"}`))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("no Retry-After header on a 429")
	}
}

func TestRejectedSubmitIsCounted(t *testing.T) {
	m := metrics.New()
	srv, _ := testServer(t, api.Config{QueueHighWaterMark: 1, Metrics: m})
	registerAndSubmit(t, srv)
	_, _ = post(t, srv, "/v1/runs", `{"agent":"api-test"}`)
	_, _ = post(t, srv, "/v1/runs", `{"agent":"api-test"}`)

	resp, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "anchor_submits_rejected_total 1") {
		t.Errorf("the rejection was not counted; /metrics said:\n%s", excerpt(string(body), "submits_rejected"))
	}
}

func TestMetricsEndpointExposesTheCollectors(t *testing.T) {
	srv, _ := testServer(t, api.Config{})
	resp, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	// The histograms the resume numbers come from must be present even before
	// anything has been observed, or a dashboard panel shows "no data" rather
	// than zero and nobody can tell the difference from a broken scrape.
	for _, want := range []string{
		"anchor_scheduling_latency_seconds",
		"anchor_queue_wait_seconds",
		"anchor_step_duration_seconds",
		"anchor_queue_depth",
		"anchor_runs_finished_total",
		"anchor_runs_started_total",
		"anchor_steps_failed_total",
		"anchor_rate_limited_total",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics is missing %s", want)
		}
	}

	// Specifically: a zero-valued series must exist for every status, not just
	// the ones that have happened.
	for _, status := range []string{"completed", "failed", "cancelled", "needs_review"} {
		want := fmt.Sprintf(`anchor_runs_finished_total{status="%s"} 0`, status)
		if !strings.Contains(string(body), want) {
			t.Errorf("no zero series for status %q; a dashboard would show 'no data' rather than 0", status)
		}
	}
}

func TestSubmitRejectsAnUnknownAgent(t *testing.T) {
	srv, _ := testServer(t, api.Config{})
	status, _ := post(t, srv, "/v1/runs", `{"agent":"never-registered"}`)
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", status)
	}
}

// A graph that fails validation must be rejected at the boundary, where the
// caller can fix it -- not halfway through a run, after money has been spent.
func TestRegisterRejectsAnInvalidGraph(t *testing.T) {
	srv, _ := testServer(t, api.Config{})
	status, body := post(t, srv, "/v1/agents",
		`{"name":"bad","graph":{"agent":"bad","v":1,"nodes":[{"id":"n1","type":"model_call"}]}}`)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d %s, want 400", status, body)
	}
	if !strings.Contains(body, "prompt") {
		t.Errorf("the error does not say what was wrong: %s", body)
	}
}

func TestSubmitAcceptsAMicroDollarBudget(t *testing.T) {
	srv, s := testServer(t, api.Config{})
	registerAndSubmit(t, srv)

	status, body := post(t, srv, "/v1/runs",
		`{"agent":"api-test","budget_micros":500000,"budget_tokens":1000}`)
	if status != http.StatusAccepted {
		t.Fatalf("submit: %d %s", status, body)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}

	resp, err := srv.Client().Get(srv.URL + "/v1/runs/" + created.ID)
	if err != nil {
		t.Fatalf("GET run: %v", err)
	}
	defer resp.Body.Close()
	detail, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(detail), `"budget_micros":500000`) {
		t.Errorf("the budget did not round-trip: %s", detail)
	}
	_ = s
}

func excerpt(body, needle string) string {
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	return "(not present)"
}
