// Package idem derives the idempotency key that guards every side-effecting
// step in Anchor.
//
// The key is the whole of Anchor's answer to "what happens if a worker dies
// between journaling a step and its effect landing". Before executing a step the
// worker inserts the key into steps.idempotency_key, which carries a global
// unique index. A conflict means this exact step already ran, so the worker
// reads the recorded result instead of running it a second time.
//
// Because the key is derived rather than random, a worker that crashes and a
// worker that takes over compute the same key for the same logical step without
// having to coordinate.
package idem

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"strconv"

	"github.com/google/uuid"
)

// Key returns sha256(run_id || step_index || canonical_json(input)) as hex.
//
// All three components are load-bearing:
//
//   - run_id scopes the key to one run. Two runs doing identical work are
//     different effects and must both happen.
//   - step_index scopes it within the run. An agent_loop that calls the same
//     tool with the same input on two iterations means it twice.
//   - the canonicalised input makes the key describe the effect, not just its
//     position, so a step whose input changed is a different effect.
//
// Deliberately absent: the attempt number. Retries of the same logical step
// share a key, which is exactly what makes a retry safe to run after a crash.
func Key(runID uuid.UUID, stepIndex int, input json.RawMessage) (string, error) {
	canon, err := CanonicalJSON(input)
	if err != nil {
		return "", fmt.Errorf("canonicalise step input: %w", err)
	}
	h := sha256.New()
	// Length-prefix each field. Without it, run "a" + step "11" and run "a1" +
	// step "1" would hash identically -- a collision that would silently skip a
	// real side effect.
	writeField(h, runID[:])
	writeField(h, []byte(strconv.Itoa(stepIndex)))
	writeField(h, canon)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeField(h hash.Hash, b []byte) {
	// sha256's Write never returns an error, so the results are discarded here
	// rather than threaded through every caller.
	_, _ = h.Write([]byte(strconv.Itoa(len(b))))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write(b)
}

// CanonicalJSON re-encodes JSON into a single deterministic form: object keys
// sorted, no insignificant whitespace, no HTML escaping, numeric literals kept
// exactly as written.
//
// Two inputs that mean the same thing must produce the same key, or a retry
// after a crash would look like a brand new effect and run twice.
func CanonicalJSON(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	// UseNumber keeps 1e3 and 1000 distinct rather than routing both through
	// float64, where large integers would also lose precision.
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("trailing data after JSON value")
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Go's encoder sorts map keys, which is what makes this canonical. HTML
	// escaping is disabled so the bytes depend only on the value.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	// Encode appends a newline.
	return bytes.TrimSuffix(buf.Bytes(), []byte{'\n'}), nil
}
