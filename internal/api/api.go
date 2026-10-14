// Package api is Anchor's HTTP surface.
//
// The server is stateless: it validates, writes to Postgres, and returns. It
// never talks to a worker directly -- the two coordinate only through the
// database, which is why either side can crash and restart without the other
// noticing.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/marlmelara/anchor/internal/agent"
	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/store"
)

// Config tunes the server.
type Config struct {
	// QueueHighWaterMark is the queue depth above which submissions are
	// refused with 429. This is backpressure at the front door: when consumers
	// cannot keep up, producers are told to slow down rather than letting work
	// pile until something breaks.
	QueueHighWaterMark int64
	// APIKeys maps a bearer token to a tenant id. Auth beyond a static key per
	// tenant is explicitly out of scope.
	APIKeys map[string]string
	Logger  *slog.Logger
}

// Server holds the handlers.
type Server struct {
	store *store.Store
	cfg   Config
	log   *slog.Logger
}

func New(s *store.Store, cfg Config) *Server {
	if cfg.QueueHighWaterMark <= 0 {
		cfg.QueueHighWaterMark = 10000
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Server{store: s, cfg: cfg, log: cfg.Logger}
}

// Routes builds the mux. Go 1.22+ patterns carry the method and path
// parameters, so no third-party router is needed.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /v1/agents", s.auth(s.handleRegisterAgent))
	mux.HandleFunc("POST /v1/runs", s.auth(s.handleSubmitRun))
	mux.HandleFunc("GET /v1/runs", s.auth(s.handleListRuns))
	mux.HandleFunc("GET /v1/runs/{id}", s.auth(s.handleGetRun))
	mux.HandleFunc("GET /v1/runs/{id}/events", s.auth(s.handleRunEvents))
	mux.HandleFunc("POST /v1/runs/{id}/cancel", s.auth(s.handleCancelRun))
	return mux
}

// tenantKey is the context key carrying the authenticated tenant.
type tenantKey struct{}

// auth resolves the bearer token to a tenant. With no keys configured the
// server runs open, which is only acceptable for local development.
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant := "default"
		if len(s.cfg.APIKeys) > 0 {
			header := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if len(header) <= len(prefix) || header[:len(prefix)] != prefix {
				writeError(w, http.StatusUnauthorized, "missing bearer token")
				return
			}
			resolved, ok := s.cfg.APIKeys[header[len(prefix):]]
			if !ok {
				writeError(w, http.StatusUnauthorized, "unknown api key")
				return
			}
			tenant = resolved
		}
		next(w, r.WithContext(withTenant(r.Context(), tenant)))
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	depth, err := s.store.QueueDepth(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "queue_depth": depth})
}

// ---------------------------------------------------------------------------

type registerAgentRequest struct {
	Name  string          `json:"name"`
	Graph json.RawMessage `json:"graph"`
}

func (s *Server) handleRegisterAgent(w http.ResponseWriter, r *http.Request) {
	var req registerAgentRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	// Validate at the boundary. A graph that fails here is rejected while the
	// caller can still fix it, instead of failing a run halfway through and
	// burning whatever was already spent.
	if _, err := agent.Parse(req.Graph); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	version, err := s.store.RegisterAgent(r.Context(), req.Name, req.Graph)
	if err != nil {
		s.log.Error("register agent", "name", req.Name, "err", err)
		writeError(w, http.StatusInternalServerError, "could not register agent")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"name": req.Name, "version": version})
}

type submitRunRequest struct {
	Agent        string          `json:"agent"`
	AgentVersion int             `json:"agent_version,omitempty"`
	Input        json.RawMessage `json:"input,omitempty"`
	BudgetTokens int64           `json:"budget_tokens,omitempty"`
	BudgetCents  int64           `json:"budget_cents,omitempty"`
	Priority     int             `json:"priority,omitempty"`
}

func (s *Server) handleSubmitRun(w http.ResponseWriter, r *http.Request) {
	var req submitRunRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Agent == "" {
		writeError(w, http.StatusBadRequest, "agent is required")
		return
	}

	// Backpressure. Returning 429 with Retry-After is the honest answer when
	// the system is saturated; accepting the work and never running it is not.
	depth, err := s.store.QueueDepth(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	if depth >= s.cfg.QueueHighWaterMark {
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusTooManyRequests,
			fmt.Sprintf("queue depth %d is at the high-water mark %d", depth, s.cfg.QueueHighWaterMark))
		return
	}

	version := req.AgentVersion
	if version == 0 {
		version, err = s.store.LatestAgentVersion(r.Context(), req.Agent)
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, fmt.Sprintf("unknown agent %q", req.Agent))
			return
		}
		if err != nil {
			s.log.Error("latest agent version", "agent", req.Agent, "err", err)
			writeError(w, http.StatusInternalServerError, "could not resolve agent version")
			return
		}
	}

	st, err := s.store.SubmitRun(r.Context(), store.SubmitRequest{
		TenantID:     tenantFrom(r.Context()),
		AgentName:    req.Agent,
		AgentVersion: version,
		Input:        req.Input,
		BudgetTokens: req.BudgetTokens,
		BudgetCents:  req.BudgetCents,
		Priority:     req.Priority,
	})
	if err != nil {
		s.log.Error("submit run", "agent", req.Agent, "err", err)
		writeError(w, http.StatusInternalServerError, "could not submit run")
		return
	}
	writeJSON(w, http.StatusAccepted, runView(st))
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	// Folded from the log, not read from the projection: a run detail must be
	// authoritative, and a stale cache row would be worse than a slow answer.
	st, err := s.store.LoadState(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such run")
		return
	}
	if err != nil {
		s.log.Error("load run", "run", id, "err", err)
		writeError(w, http.StatusInternalServerError, "could not load run")
		return
	}
	if st.TenantID != tenantFrom(r.Context()) {
		// 404 rather than 403: existence itself is not something one tenant
		// should be able to probe for another.
		writeError(w, http.StatusNotFound, "no such run")
		return
	}
	writeJSON(w, http.StatusOK, runDetail(st))
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			limit = parsed
		}
	}
	runs, err := s.store.ListRuns(r.Context(), tenantFrom(r.Context()), limit)
	if err != nil {
		s.log.Error("list runs", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list runs")
		return
	}
	if runs == nil {
		runs = []store.RunSummary{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) handleRunEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	after := 0
	if raw := r.URL.Query().Get("after"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			after = parsed
		}
	}
	events, err := s.store.LoadEvents(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such run")
		return
	}
	if err != nil {
		s.log.Error("load events", "run", id, "err", err)
		writeError(w, http.StatusInternalServerError, "could not load events")
		return
	}
	out := make([]journal.Event, 0, len(events))
	for _, e := range events {
		if e.Seq > after {
			out = append(out, e)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out})
}

type cancelRequest struct {
	Reason string `json:"reason,omitempty"`
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	var req cancelRequest
	if r.ContentLength > 0 && !decodeBody(w, r, &req) {
		return
	}
	if req.Reason == "" {
		req.Reason = "cancelled via API"
	}

	st, err := s.store.LoadState(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such run")
		return
	}
	if err != nil {
		s.log.Error("load run", "run", id, "err", err)
		writeError(w, http.StatusInternalServerError, "could not load run")
		return
	}
	if st.TenantID != tenantFrom(r.Context()) {
		writeError(w, http.StatusNotFound, "no such run")
		return
	}
	if st.Terminal() {
		writeJSON(w, http.StatusOK, runView(st))
		return
	}

	// Cancellation is cooperative. This writes the intent; the worker observes
	// it at the next step boundary and cancels its context, which aborts the
	// in-flight model call or tool container. Nothing is killed from here.
	err = s.store.Append(r.Context(), st, store.PendingEvent{
		Type: journal.TypeRunCancelled,
		Payload: journal.RunCancelledPayload{
			Versioned:   journal.Versioned{V: journal.CurrentPayloadVersion},
			Reason:      req.Reason,
			RequestedBy: tenantFrom(r.Context()),
		},
	})
	if errors.Is(err, store.ErrSeqConflict) {
		// A worker advanced the run between the read and the write. The client
		// should retry; the run is still cancellable.
		writeError(w, http.StatusConflict, "run advanced concurrently, retry the cancel")
		return
	}
	if err != nil {
		s.log.Error("cancel run", "run", id, "err", err)
		writeError(w, http.StatusInternalServerError, "could not cancel run")
		return
	}
	writeJSON(w, http.StatusOK, runView(st))
}

// ---------------------------------------------------------------------------

func runView(st *journal.RunState) map[string]any {
	return map[string]any{
		"id":            st.RunID,
		"status":        st.Status,
		"agent":         st.AgentName,
		"agent_version": st.AgentVersion,
		"created_at":    st.CreatedAt,
	}
}

func runDetail(st *journal.RunState) map[string]any {
	view := runView(st)
	view["tokens_used"] = st.TokensUsed
	view["cents_used"] = st.CentsUsed
	view["budget_tokens"] = st.BudgetTokens
	view["budget_cents"] = st.BudgetCents
	view["started_at"] = st.StartedAt
	view["finished_at"] = st.FinishedAt
	view["steps"] = st.Steps
	view["next_seq"] = st.NextSeq
	if st.Error != "" {
		view["error"] = st.Error
	}
	if len(st.TerminalOutput) > 0 {
		view["output"] = st.TerminalOutput
	}
	if st.ReplayOf != nil {
		view["replay_of"] = st.ReplayOf
	}
	return view
}

func pathUUID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "run id is not a uuid")
		return uuid.Nil, false
	}
	return id, true
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	// Cap the body: an unbounded read is a trivial memory-exhaustion vector.
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// ReadTimeout and friends for the caller's http.Server.
const (
	ReadHeaderTimeout = 5 * time.Second
	ReadTimeout       = 30 * time.Second
	WriteTimeout      = 60 * time.Second
	IdleTimeout       = 120 * time.Second
)
