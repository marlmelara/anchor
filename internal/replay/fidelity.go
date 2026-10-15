package replay

import (
	"fmt"
	"sort"
	"strings"
)

// Result is the outcome of replaying one run.
type Result struct {
	OriginalRunID string      `json:"original_run_id"`
	ReplayRunID   string      `json:"replay_run_id"`
	Matched       bool        `json:"matched"`
	Divergence    *Divergence `json:"divergence,omitempty"`
	// Err is set when the replay could not be completed at all -- a crash, a
	// timeout, a missing agent version. Distinct from a divergence: one is a
	// fidelity failure, the other is a harness failure, and conflating them
	// would let infrastructure problems masquerade as determinism problems.
	Err string `json:"error,omitempty"`
}

// Report is the fidelity of a corpus of replays.
//
// "X% fidelity over N runs" is the headline number, and it is only meaningful
// alongside N and the breakdown. A single percentage hides whether the failures
// are one systemic cause or N unrelated ones -- and the honest version of this
// number includes the failures, not just the successes.
type Report struct {
	Total    int      `json:"total"`
	Matched  int      `json:"matched"`
	Diverged int      `json:"diverged"`
	Errored  int      `json:"errored"`
	Results  []Result `json:"results"`
}

// Fidelity is the share of replays that reproduced their original exactly.
//
// Runs that ERRORED are excluded from the denominator: a replay that could not
// run says nothing about determinism. They are reported separately so the
// exclusion is visible rather than quietly improving the number.
func (r *Report) Fidelity() float64 {
	comparable := r.Matched + r.Diverged
	if comparable == 0 {
		return 0
	}
	return float64(r.Matched) / float64(comparable) * 100
}

// Add folds one result into the report.
func (r *Report) Add(res Result) {
	r.Total++
	switch {
	case res.Err != "":
		r.Errored++
	case res.Matched:
		r.Matched++
	default:
		r.Diverged++
	}
	r.Results = append(r.Results, res)
}

// ByKind counts divergences by cause, which is what actually guides a fix. Ten
// failures of one kind is one bug; ten of ten kinds is a broken design.
func (r *Report) ByKind() map[DivergenceKind]int {
	out := map[DivergenceKind]int{}
	for _, res := range r.Results {
		if res.Divergence != nil {
			out[res.Divergence.Kind]++
		}
	}
	return out
}

// String renders the report for a terminal.
func (r *Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "replay fidelity: %.1f%% over %d comparable run(s)\n",
		r.Fidelity(), r.Matched+r.Diverged)
	fmt.Fprintf(&b, "  matched   %d\n", r.Matched)
	fmt.Fprintf(&b, "  diverged  %d\n", r.Diverged)
	if r.Errored > 0 {
		fmt.Fprintf(&b, "  errored   %d  (excluded from fidelity: a replay that could not run says nothing about determinism)\n", r.Errored)
	}

	if kinds := r.ByKind(); len(kinds) > 0 {
		b.WriteString("\ndivergences by cause:\n")
		names := make([]string, 0, len(kinds))
		for k := range kinds {
			names = append(names, string(k))
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(&b, "  %-18s %d\n", name, kinds[DivergenceKind(name)])
		}
	}

	// The first few divergences in full: a count tells you there is a problem,
	// the detail tells you what it is.
	shown := 0
	for _, res := range r.Results {
		if res.Divergence == nil || shown >= 5 {
			continue
		}
		if shown == 0 {
			b.WriteString("\nfirst divergences:\n")
		}
		fmt.Fprintf(&b, "  run %s: %s\n", res.OriginalRunID, res.Divergence.Error())
		shown++
	}
	for _, res := range r.Results {
		if res.Err != "" {
			fmt.Fprintf(&b, "\n  run %s could not be replayed: %s\n", res.OriginalRunID, res.Err)
			break
		}
	}
	return b.String()
}
