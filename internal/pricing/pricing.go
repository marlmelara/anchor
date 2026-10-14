// Package pricing turns token counts into money.
//
// The provider API returns token counts, never cost -- it has no idea what you
// pay, since that depends on the model, on input versus output, and on your
// tier. Cost is therefore something Anchor computes, and a budget denominated
// in money cannot work without this table.
//
// Money is counted in MICRO-DOLLARS: one millionth of a dollar, so $1 is
// 1,000,000. Cents are too coarse -- a single short model call costs a fraction
// of a cent, so an integer-cent counter rounds almost every real run to zero and
// a cost budget built on it can never trigger. Floating point is never used for
// money.
package pricing

import (
	"fmt"
	"sort"
	"strings"
)

// MicrosPerDollar is the unit scale: $1 == 1,000,000 micro-dollars.
const MicrosPerDollar int64 = 1_000_000

// MicrosPerCent is used to read payloads written before this unit existed.
const MicrosPerCent int64 = 10_000

// Price is what one million tokens costs, in micro-dollars.
//
// Stored per million tokens rather than per token because some models cost a
// fraction of a micro-dollar per token -- Haiku's input is $0.10 per million,
// which is a tenth of a micro-dollar each. Per-million keeps every figure an
// exact integer.
type Price struct {
	InputPerMTok  int64
	OutputPerMTok int64
}

// dollarsPerMTok is a readability helper for the table below.
func dollarsPerMTok(in, out float64) Price {
	return Price{
		InputPerMTok:  int64(in * float64(MicrosPerDollar)),
		OutputPerMTok: int64(out * float64(MicrosPerDollar)),
	}
}

// table is the price list, keyed by exact model id.
//
// Deliberately exact-match and deliberately incomplete: an unknown model is an
// error rather than a zero, because silently costing nothing is precisely the
// bug this package exists to fix. Prices change, so this needs reviewing
// against the published list rather than trusted indefinitely.
//
// Current as of 2026-10-14.
var table = map[string]Price{
	"claude-opus-5-5":   dollarsPerMTok(4, 20),
	"claude-opus-5":     dollarsPerMTok(5, 25),
	"claude-opus-4-8":   dollarsPerMTok(5, 25),
	"claude-opus-4-7":   dollarsPerMTok(5, 25),
	"claude-opus-4-6":   dollarsPerMTok(5, 25),
	"claude-sonnet-5-5": dollarsPerMTok(2, 10),
	"claude-sonnet-5":   dollarsPerMTok(2, 10),
	"claude-sonnet-4-6": dollarsPerMTok(3, 15),
	"claude-haiku-5-5":  dollarsPerMTok(0.10, 0.50),
	"claude-haiku-4-5":  dollarsPerMTok(1, 5),
	"claude-fable-5-1":  dollarsPerMTok(10, 50),
	"claude-fable-5":    dollarsPerMTok(10, 50),

	// The mock provider is free. Benchmarks measure Anchor, not spend.
	"mock": {InputPerMTok: 0, OutputPerMTok: 0},
}

// ErrUnknownModel means there is no price for this model.
//
// Callers must not treat it as zero. A run with a cost budget against an
// unpriced model cannot have that budget enforced, and continuing anyway would
// mean a budget that silently does nothing -- which is worse than no budget,
// because the operator believes they have one.
type ErrUnknownModel struct {
	Model string
}

func (e *ErrUnknownModel) Error() string {
	return fmt.Sprintf("no price for model %q; add it to internal/pricing or the cost budget cannot be enforced", e.Model)
}

// Lookup returns the price for a model.
func Lookup(model string) (Price, error) {
	if p, ok := table[model]; ok {
		return p, nil
	}
	return Price{}, &ErrUnknownModel{Model: model}
}

// Known lists every priced model, sorted. Sorted rather than map order because
// Go randomises map iteration, and a command that prints a different order each
// run is needlessly hard to diff.
func Known() []string {
	out := make([]string, 0, len(table))
	for model := range table {
		out = append(out, model)
	}
	sort.Strings(out)
	return out
}

// Cost returns what a completed call cost, in micro-dollars.
//
// Integer arithmetic throughout. The division truncates, so the figure can be
// up to one micro-dollar below the true cost per call -- a millionth of a
// dollar, which no budget cares about, and which is preferable to the rounding
// errors floating point would introduce across thousands of calls.
func Cost(model string, promptTokens, completionTokens int64) (int64, error) {
	p, err := Lookup(model)
	if err != nil {
		return 0, err
	}
	if promptTokens < 0 || completionTokens < 0 {
		return 0, fmt.Errorf("token counts cannot be negative (got %d prompt, %d completion)", promptTokens, completionTokens)
	}
	input := promptTokens * p.InputPerMTok / 1_000_000
	output := completionTokens * p.OutputPerMTok / 1_000_000
	return input + output, nil
}

// Projected estimates what a call is ABOUT to cost, before it is made.
//
// Deliberately pessimistic: the completion length is unknown until after the
// money is spent, so the per-call output ceiling is assumed. A budget check
// that guessed low would let the call through and discover the breach
// afterwards, which is exactly what a budget exists to prevent.
func Projected(model string, promptTokens, maxOutputTokens int64) (int64, error) {
	return Cost(model, promptTokens, maxOutputTokens)
}

// FormatMicros renders micro-dollars for humans, e.g. "$0.003004".
func FormatMicros(micros int64) string {
	neg := micros < 0
	if neg {
		micros = -micros
	}
	dollars := micros / MicrosPerDollar
	frac := micros % MicrosPerDollar
	s := fmt.Sprintf("$%d.%06d", dollars, frac)
	// Trim trailing zeros but keep at least two decimal places.
	s = strings.TrimRight(s, "0")
	if idx := strings.IndexByte(s, '.'); idx >= 0 && len(s)-idx < 3 {
		s += strings.Repeat("0", 3-(len(s)-idx))
	}
	if neg {
		return "-" + s
	}
	return s
}
