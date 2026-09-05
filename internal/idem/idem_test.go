package idem

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

var runA = uuid.MustParse("11111111-1111-4111-8111-111111111111")
var runB = uuid.MustParse("22222222-2222-4222-8222-222222222222")

func key(t *testing.T, run uuid.UUID, step int, input string) string {
	t.Helper()
	k, err := Key(run, step, json.RawMessage(input))
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	return k
}

// The same logical step must always produce the same key. This is what lets a
// worker that took over from a dead one recognise work that already happened.
func TestKeyIsStable(t *testing.T) {
	a := key(t, runA, 3, `{"url":"https://example.com"}`)
	b := key(t, runA, 3, `{"url":"https://example.com"}`)
	if a != b {
		t.Errorf("key is not stable: %s vs %s", a, b)
	}
}

// Key order and whitespace are presentation, not meaning. If they changed the
// key, a retry after a crash would look like a new effect and run twice.
func TestKeyIgnoresKeyOrderAndWhitespace(t *testing.T) {
	a := key(t, runA, 0, `{"b":2,"a":1}`)
	b := key(t, runA, 0, `{"a":1,   "b":2}`)
	c := key(t, runA, 0, "{\n  \"a\": 1,\n  \"b\": 2\n}")
	if a != b || b != c {
		t.Errorf("canonicalisation failed:\n %s\n %s\n %s", a, b, c)
	}
}

func TestKeyIgnoresNestedKeyOrder(t *testing.T) {
	a := key(t, runA, 0, `{"o":{"z":1,"y":{"n":2,"m":3}}}`)
	b := key(t, runA, 0, `{"o":{"y":{"m":3,"n":2},"z":1}}`)
	if a != b {
		t.Errorf("nested key order changed the key:\n %s\n %s", a, b)
	}
}

// Array order IS meaning. Reordering a list is a different call.
func TestKeyRespectsArrayOrder(t *testing.T) {
	a := key(t, runA, 0, `{"xs":[1,2,3]}`)
	b := key(t, runA, 0, `{"xs":[3,2,1]}`)
	if a == b {
		t.Error("array order did not change the key, but order is meaning in a list")
	}
}

func TestKeyVariesByRun(t *testing.T) {
	a := key(t, runA, 0, `{"x":1}`)
	b := key(t, runB, 0, `{"x":1}`)
	if a == b {
		t.Error("two different runs share a key; identical work in two runs is two effects")
	}
}

func TestKeyVariesByStepIndex(t *testing.T) {
	a := key(t, runA, 0, `{"x":1}`)
	b := key(t, runA, 1, `{"x":1}`)
	if a == b {
		t.Error("two steps in one run share a key; a loop calling one tool twice means it twice")
	}
}

func TestKeyVariesByInput(t *testing.T) {
	a := key(t, runA, 0, `{"x":1}`)
	b := key(t, runA, 0, `{"x":2}`)
	if a == b {
		t.Error("different inputs share a key")
	}
}

// Length-prefixing each field is what prevents this: without it, the
// concatenation of (run, 11, input) and (run, 1, "1"+input) could collide, and a
// collision means a real side effect is silently skipped.
func TestKeyIsNotVulnerableToFieldRunOn(t *testing.T) {
	a := key(t, runA, 11, `{}`)
	b := key(t, runA, 1, `{}`)
	if a == b {
		t.Error("step 11 and step 1 collide")
	}
	// A crafted input cannot impersonate a different step index either.
	c := key(t, runA, 1, `{"pad":"1"}`)
	if a == c {
		t.Error("a crafted input impersonated a different step index")
	}
}

// A retried step is the same logical effect, so it keeps its key. That is the
// entire mechanism: attempt 2 finds attempt 1's row and stops.
func TestKeyDoesNotDependOnAttempt(t *testing.T) {
	// There is no attempt parameter at all -- this test documents that as
	// intentional rather than an oversight, so a future change has to argue
	// with it.
	a := key(t, runA, 0, `{"x":1}`)
	b := key(t, runA, 0, `{"x":1}`)
	if a != b {
		t.Fatal("unreachable unless an attempt component is added to Key")
	}
}

func TestKeyIsHex(t *testing.T) {
	k := key(t, runA, 0, `{}`)
	if len(k) != 64 {
		t.Errorf("len(key) = %d, want 64 hex chars for sha256", len(k))
	}
	for _, c := range k {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Fatalf("key contains non-hex %q: %s", c, k)
		}
	}
}

func TestKeyRejectsInvalidJSON(t *testing.T) {
	if _, err := Key(runA, 0, json.RawMessage(`{"x":`)); err == nil {
		t.Error("Key accepted malformed JSON")
	}
	if _, err := Key(runA, 0, json.RawMessage(`{} {}`)); err == nil {
		t.Error("Key accepted trailing data")
	}
}

func TestKeyTreatsEmptyInputAsEmptyObject(t *testing.T) {
	a := key(t, runA, 0, ``)
	b := key(t, runA, 0, `{}`)
	if a != b {
		t.Error("absent input and empty object should be the same effect")
	}
}

// ---------------------------------------------------------------------------

func TestCanonicalJSONSortsKeys(t *testing.T) {
	got, err := CanonicalJSON(json.RawMessage(`{"c":3,"a":1,"b":2}`))
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	if string(got) != `{"a":1,"b":2,"c":3}` {
		t.Errorf("got %s, want sorted keys", got)
	}
}

// Routing numbers through float64 would turn a large integer id into something
// ending in 000, and two different ids would then share a key.
func TestCanonicalJSONPreservesLargeIntegers(t *testing.T) {
	const big = `{"id":12345678901234567890}`
	got, err := CanonicalJSON(json.RawMessage(big))
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	if string(got) != big {
		t.Errorf("got %s, want %s -- precision was lost", got, big)
	}
}

func TestCanonicalJSONDoesNotEscapeHTML(t *testing.T) {
	got, err := CanonicalJSON(json.RawMessage(`{"q":"a<b&c"}`))
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	if string(got) != `{"q":"a<b&c"}` {
		t.Errorf("got %s, want the string unescaped", got)
	}
}

func TestCanonicalJSONAcceptsNonObjects(t *testing.T) {
	for _, in := range []string{`[3,1,2]`, `"hello"`, `42`, `true`, `null`} {
		if _, err := CanonicalJSON(json.RawMessage(in)); err != nil {
			t.Errorf("CanonicalJSON(%s): %v", in, err)
		}
	}
}
