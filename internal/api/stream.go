package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/store"
)

// streamPoll is how often the server looks for new events.
//
// Polling, not LISTEN/NOTIFY. It is the simple version and it is honest about
// its cost: a run's events arrive up to this late. The upgrade is Postgres
// LISTEN/NOTIFY from the worker on each append, which would push instantly --
// worth doing when the viewer has enough concurrent watchers that polling
// becomes the dominant query load.
const streamPoll = 400 * time.Millisecond

// streamIdleTimeout closes a stream that has seen nothing for this long on a
// run that is already finished, so a forgotten browser tab does not hold a
// connection open forever.
const streamIdleTimeout = 5 * time.Minute

// handleStreamRun streams a run's events as Server-Sent Events.
//
// SSE rather than WebSockets: this is one-way, server to browser, which is all
// the viewer needs. It runs over plain HTTP, reconnects on its own, and needs
// no protocol upgrade or separate server. A WebSocket would be a second
// transport to operate for no extra capability.
func (s *Server) handleStreamRun(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}

	// Flushing is what makes a stream a stream. Without it the response is
	// buffered and the browser sees nothing until the handler returns, which
	// for a live run is never.
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is not supported by this server")
		return
	}

	st, err := s.store.LoadState(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such run")
		return
	}
	if err != nil {
		s.log.Error("stream: load run", "run", id, "err", err)
		writeError(w, http.StatusInternalServerError, "could not load run")
		return
	}
	if st.TenantID != tenantFrom(r.Context()) {
		writeError(w, http.StatusNotFound, "no such run")
		return
	}

	// A reconnecting EventSource sends Last-Event-ID; ?after= is the explicit
	// form. Resuming from a seq rather than replaying everything is what makes
	// a dropped connection cheap.
	after := 0
	if raw := r.URL.Query().Get("after"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			after = parsed
		}
	}
	if raw := r.Header.Get("Last-Event-ID"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > after {
			after = parsed
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Tells nginx and friends not to buffer, which would defeat the flush.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ticker := time.NewTicker(streamPoll)
	defer ticker.Stop()
	deadline := time.NewTimer(streamIdleTimeout)
	defer deadline.Stop()

	for {
		events, err := s.store.LoadEvents(r.Context(), id)
		if err != nil {
			if r.Context().Err() != nil {
				return // the browser went away
			}
			s.log.Error("stream: load events", "run", id, "err", err)
			writeSSE(w, flusher, "error", after, map[string]string{"error": "could not read events"})
			return
		}

		var newest int
		for _, e := range events {
			if e.Seq <= after {
				continue
			}
			writeSSE(w, flusher, string(e.Type), e.Seq, e)
			newest = e.Seq
		}
		if newest > after {
			after = newest
			deadline.Reset(streamIdleTimeout)
		}

		// A terminal run has nothing more to send. Closing the stream tells the
		// browser to stop reconnecting; EventSource would otherwise retry
		// forever against a run that will never change.
		folded, err := journal.Fold(events)
		if err == nil && folded.Terminal() {
			writeSSE(w, flusher, "done", after, map[string]any{
				"status": folded.Status, "seq": after,
			})
			return
		}

		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			writeSSE(w, flusher, "timeout", after, map[string]string{
				"reason": "no activity; reconnect with ?after=" + strconv.Itoa(after),
			})
			return
		case <-ticker.C:
		}
	}
}

// writeSSE emits one Server-Sent Event.
//
// The format is plain text: an optional id, an event name, a data line, then a
// BLANK line that terminates the message. The blank line is not optional -- a
// browser buffers an unterminated message indefinitely.
//
// The event NAME matters: a named event fires only a listener registered for
// that name, and the generic onmessage handler never sees it. That trips up
// most first SSE implementations, so the viewer registers by name.
func writeSSE(w http.ResponseWriter, flusher http.Flusher, event string, id int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", id, event, body)
	flusher.Flush()
}
