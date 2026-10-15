package replay

import (
	"encoding/json"
	"fmt"

	"github.com/marlmelara/anchor/internal/journal"
)

// DivergenceKind classifies how a replay differed from its original.
//
// The kind matters more than the message: it says WHICH guarantee broke, and
// each one points at a different cause.
type DivergenceKind string

const (
	// DivergenceEventSequence: the replay produced a different event, or a
	// different order. The most common kind, and the one that means the graph
	// walk itself took a different path.
	DivergenceEventSequence DivergenceKind = "event_sequence"
	// DivergenceUnexpectedCall: the replay asked for something the original
	// never did -- a model call at a step that does not exist, or one tool call
	// too many.
	DivergenceUnexpectedCall DivergenceKind = "unexpected_call"
	// DivergenceWrongStepKind: a model call where the original ran a tool, or
	// the reverse.
	DivergenceWrongStepKind DivergenceKind = "wrong_step_kind"
	// DivergenceWrongTool: the right step, the wrong tool.
	DivergenceWrongTool DivergenceKind = "wrong_tool"
	// DivergenceTerminalOutput: identical events, different final answer.
	// Rare and serious -- it means something outside the journal leaked into
	// the result.
	DivergenceTerminalOutput DivergenceKind = "terminal_output"
	// DivergenceStatus: the replay ended in a different state.
	DivergenceStatus DivergenceKind = "status"
	// DivergenceLength: one run has events the other does not.
	DivergenceLength DivergenceKind = "length"
)

// Divergence is the FIRST point at which a replay differed.
//
// Only the first is reported. Once a replay has diverged, everything after it
// is downstream of that single difference, and listing it all buries the cause
// under its consequences. The first divergence is the bug; the rest is weather.
type Divergence struct {
	Kind      DivergenceKind `json:"kind"`
	StepIndex int            `json:"step_index,omitempty"`
	// Seq is where in the event sequence the two runs parted.
	Seq int `json:"seq,omitempty"`
	// Original and Replayed are the two event signatures at that point, when
	// the divergence is a sequence mismatch.
	Original string `json:"original,omitempty"`
	Replayed string `json:"replayed,omitempty"`
	Detail   string `json:"detail"`
}

func (d *Divergence) Error() string {
	if d.Original != "" || d.Replayed != "" {
		return fmt.Sprintf("replay diverged (%s) at seq %d: original %q, replay %q -- %s",
			d.Kind, d.Seq, d.Original, d.Replayed, d.Detail)
	}
	return fmt.Sprintf("replay diverged (%s): %s", d.Kind, d.Detail)
}

// Compare checks a replay's events against the original's and returns the first
// difference, or nil when they match.
//
// What is compared, and what is deliberately NOT:
//
//   - Compared: the event TYPE, its ORDER, and which step and attempt it is
//     about. Those are the shape of the execution, and reproducing them is the
//     entire determinism claim.
//   - Not compared: timestamps and database ids. A replay happens later by
//     definition, so comparing times would make every replay diverge at the
//     first event and the check would be worthless.
//   - Not compared: the model's response bytes. The replay is HANDED those, so
//     comparing them would only confirm the recording was read correctly --
//     testing the test, not the system.
//
// Terminal output IS compared, because it is computed rather than handed over:
// the same events producing a different final answer means something outside
// the journal reached the result.
func Compare(original *Recording, replayEvents []journal.Event) *Divergence {
	replaySigs := make([]Signature, 0, len(replayEvents))
	for _, e := range replayEvents {
		sig, err := SignatureOf(e)
		if err != nil {
			return &Divergence{
				Kind:   DivergenceEventSequence,
				Seq:    e.Seq,
				Detail: err.Error(),
			}
		}
		replaySigs = append(replaySigs, sig)
	}

	// A replay's RunSubmitted carries replay_of and is its own event; the two
	// runs are compared from their first event onward regardless.
	n := min(len(original.Events), len(replaySigs))
	for i := range n {
		o, r := original.Events[i], replaySigs[i]
		// Plain == is correct now that the signature holds no pointers.
		if o.Type != r.Type || o.StepIndex != r.StepIndex || o.Attempt != r.Attempt {
			return &Divergence{
				Kind:      DivergenceEventSequence,
				Seq:       r.Seq,
				StepIndex: r.StepIndex,
				Original:  o.String(),
				Replayed:  r.String(),
				Detail:    "the replay produced a different event at this position",
			}
		}
	}

	if len(replaySigs) != len(original.Events) {
		longer, shorter := "replay", "original"
		if len(original.Events) > len(replaySigs) {
			longer, shorter = "original", "replay"
		}
		return &Divergence{
			Kind: DivergenceLength,
			Seq:  n,
			Detail: fmt.Sprintf(
				"the two runs agree for %d events, then the %s continues (%d events) while the %s stops (%d)",
				n, longer, max(len(original.Events), len(replaySigs)),
				shorter, min(len(original.Events), len(replaySigs))),
		}
	}

	return nil
}

// CompareFinal checks the folded end state of a replay against the original.
//
// Run after Compare: identical events with a different result is a distinct and
// more alarming finding than a different event sequence, because it means the
// difference came from somewhere the journal never saw.
func CompareFinal(original *Recording, replayed *journal.RunState) *Divergence {
	if replayed.Status != original.Status {
		return &Divergence{
			Kind:     DivergenceStatus,
			Original: string(original.Status),
			Replayed: string(replayed.Status),
			Detail:   "the replay ended in a different state",
		}
	}
	if !sameJSON(original.TerminalOutput, replayed.TerminalOutput) {
		return &Divergence{
			Kind:     DivergenceTerminalOutput,
			Original: string(original.TerminalOutput),
			Replayed: string(replayed.TerminalOutput),
			Detail:   "identical events produced a different final output, so something outside the journal reached the result",
		}
	}
	return nil
}

// sameJSON compares two payloads by MEANING, not bytes.
//
// jsonb normalises key order and drops duplicate keys, so two semantically
// identical payloads can differ byte-for-byte after a database round trip.
// Comparing raw bytes would report divergences that are purely an artifact of
// storage.
func sameJSON(a, b json.RawMessage) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	var av, bv any
	if err := json.Unmarshal(nonEmpty(a), &av); err != nil {
		return false
	}
	if err := json.Unmarshal(nonEmpty(b), &bv); err != nil {
		return false
	}
	// Re-encoding through the same encoder sorts map keys on both sides.
	ab, err := json.Marshal(av)
	if err != nil {
		return false
	}
	bb, err := json.Marshal(bv)
	if err != nil {
		return false
	}
	return string(ab) == string(bb)
}

func nonEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`null`)
	}
	return raw
}
