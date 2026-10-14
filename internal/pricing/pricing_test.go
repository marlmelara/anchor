package pricing

import (
	"errors"
	"testing"
)

// The number that started this: the real live-check call, 36 input and 143
// output tokens on Opus 5.5 at $4/$20 per million.
//
//	36  * $4  / 1M = $0.000144
//	143 * $20 / 1M = $0.00286
//	                 $0.003004  ->  3004 micro-dollars
//
// In integer cents that is 0, which is why the unit changed.
func TestCostOfTheRealLiveCheckCall(t *testing.T) {
	got, err := Cost("claude-opus-5-5", 36, 143)
	if err != nil {
		t.Fatalf("Cost: %v", err)
	}
	if got != 3004 {
		t.Errorf("Cost = %d micro-dollars, want 3004", got)
	}
	if cents := got / MicrosPerCent; cents != 0 {
		t.Errorf("sanity: this call should round to 0 whole cents, got %d", cents)
	}
}

func TestCostIsZeroForTheMockProvider(t *testing.T) {
	got, err := Cost("mock", 1_000_000, 1_000_000)
	if err != nil {
		t.Fatalf("Cost: %v", err)
	}
	if got != 0 {
		t.Errorf("Cost = %d, want 0; benchmarks must measure Anchor, not spend", got)
	}
}

// Input and output are priced differently, so swapping them must change the
// answer. A single blended rate would quietly misprice every run.
func TestInputAndOutputArePricedSeparately(t *testing.T) {
	in, err := Cost("claude-opus-5-5", 1_000_000, 0)
	if err != nil {
		t.Fatalf("Cost: %v", err)
	}
	out, err := Cost("claude-opus-5-5", 0, 1_000_000)
	if err != nil {
		t.Fatalf("Cost: %v", err)
	}
	if in != 4*MicrosPerDollar {
		t.Errorf("1M input tokens = %d, want %d ($4)", in, 4*MicrosPerDollar)
	}
	if out != 20*MicrosPerDollar {
		t.Errorf("1M output tokens = %d, want %d ($20)", out, 20*MicrosPerDollar)
	}
	if in == out {
		t.Error("input and output cost the same; they are priced differently")
	}
}

// Haiku's input is $0.10 per million, a tenth of a micro-dollar per token.
// Prices are stored per MILLION tokens precisely so that stays an exact integer.
func TestSubMicroDollarPerTokenPricesAreExact(t *testing.T) {
	got, err := Cost("claude-haiku-5-5", 1_000_000, 0)
	if err != nil {
		t.Fatalf("Cost: %v", err)
	}
	want := int64(0.10 * float64(MicrosPerDollar))
	if got != want {
		t.Errorf("1M Haiku input tokens = %d, want %d ($0.10)", got, want)
	}
}

// An unknown model must be an ERROR, never zero. A silent zero is what makes a
// budget look enforced while nothing counts against it.
func TestUnknownModelIsAnErrorNotZero(t *testing.T) {
	_, err := Cost("claude-something-unreleased", 1000, 1000)
	var unknown *ErrUnknownModel
	if !errors.As(err, &unknown) {
		t.Fatalf("err = %v, want ErrUnknownModel", err)
	}
	if unknown.Model != "claude-something-unreleased" {
		t.Errorf("Model = %q", unknown.Model)
	}
}

// Exact match only: a prefix or a date suffix is a different model with
// possibly different pricing, and guessing would misprice it.
func TestModelMatchingIsExact(t *testing.T) {
	for _, model := range []string{
		"claude-opus-5-5-20260401",
		"anthropic.claude-opus-5-5",
		"Claude-Opus-5-5",
		"claude-opus",
	} {
		if _, err := Cost(model, 1, 1); err == nil {
			t.Errorf("%q was priced; matching must be exact", model)
		}
	}
}

// The pre-call projection must assume the output ceiling. A check that guessed
// low would let the call through and find the breach afterwards.
func TestProjectedAssumesTheOutputCeiling(t *testing.T) {
	projected, err := Projected("claude-opus-5-5", 100, 4096)
	if err != nil {
		t.Fatalf("Projected: %v", err)
	}
	actual, err := Cost("claude-opus-5-5", 100, 50)
	if err != nil {
		t.Fatalf("Cost: %v", err)
	}
	if projected <= actual {
		t.Errorf("projection %d is not pessimistic against an actual of %d", projected, actual)
	}
}

func TestCostRejectsNegativeTokens(t *testing.T) {
	if _, err := Cost("claude-opus-5-5", -1, 0); err == nil {
		t.Error("negative prompt tokens were accepted")
	}
}

func TestKnownIsSortedAndNonEmpty(t *testing.T) {
	models := Known()
	if len(models) == 0 {
		t.Fatal("the price table is empty")
	}
	for i := 1; i < len(models); i++ {
		if models[i-1] > models[i] {
			t.Fatalf("Known() is not sorted: %q before %q", models[i-1], models[i])
		}
	}
}

func TestFormatMicros(t *testing.T) {
	cases := map[int64]string{
		0:         "$0.00",
		3004:      "$0.003004",
		1_000_000: "$1.00",
		1_500_000: "$1.50",
		500_000:   "$0.50",
	}
	for micros, want := range cases {
		if got := FormatMicros(micros); got != want {
			t.Errorf("FormatMicros(%d) = %q, want %q", micros, got, want)
		}
	}
}
