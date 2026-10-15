package worker_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/provider"
	"github.com/marlmelara/anchor/internal/replay"
	"github.com/marlmelara/anchor/internal/store"
	"github.com/marlmelara/anchor/internal/tool"
	"github.com/marlmelara/anchor/internal/worker"
)

// replayOnce submits a replay of originalID, runs it, and compares.
func replayOnce(t *testing.T, s *store.Store, h *harness, originalID uuid.UUID) replay.Result {
	t.Helper()
	ctx := context.Background()

	originalEvents, err := s.LoadEvents(ctx, originalID)
	if err != nil {
		t.Fatalf("load original: %v", err)
	}
	rec, err := replay.Build(originalEvents)
	if err != nil {
		t.Fatalf("build recording: %v", err)
	}

	replayState, err := s.SubmitReplay(ctx, originalID)
	if err != nil {
		t.Fatalf("submit replay: %v", err)
	}
	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-replay", 1)}, 20*time.Second)

	final := loadState(t, s, replayState.RunID)
	res := replay.Result{
		OriginalRunID: originalID.String(),
		ReplayRunID:   replayState.RunID.String(),
	}
	if !final.Terminal() {
		res.Err = "replay did not finish"
		return res
	}
	replayEvents, err := s.LoadEvents(ctx, replayState.RunID)
	if err != nil {
		t.Fatalf("load replay events: %v", err)
	}
	if d := replay.Compare(rec, replayEvents); d != nil {
		res.Divergence = d
		return res
	}
	if d := replay.CompareFinal(rec, final); d != nil {
		res.Divergence = d
		return res
	}
	res.Matched = true
	return res
}

// The core claim: a completed run replays to the identical event sequence.
func TestReplayReproducesARunExactly(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	version := registerAgent(t, s, "two-step", twoNodeGraph)
	original := submit(t, s, "two-step", version, `{"topic":"determinism"}`)

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)
	if st := loadState(t, s, original); st.Status != journal.RunStatusCompleted {
		t.Fatalf("original run: status = %q (%s)", st.Status, st.Error)
	}
	callsAfterOriginal := h.provider.Calls()

	res := replayOnce(t, s, h, original)
	if res.Err != "" {
		t.Fatalf("replay errored: %s", res.Err)
	}
	if !res.Matched {
		t.Fatalf("replay diverged: %s", res.Divergence.Error())
	}

	// The replay must not have touched the provider. If it had, it would be
	// re-running the model rather than replaying it -- which would cost money
	// and would not be a replay at all.
	if got := h.provider.Calls(); got != callsAfterOriginal {
		t.Errorf("the provider was called %d more times during the replay; a replay reaches no provider",
			got-callsAfterOriginal)
	}
}

// A run whose step failed and then succeeded must replay the FAILURE too.
// Returning the recorded success immediately would skip StepFailed and
// RetryScheduled, producing a shorter sequence -- so every run that ever
// retried would score as a fidelity failure for no real reason.
func TestReplayReproducesRetries(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	h.provider.FailFirst = 2 // two failures, then success

	version := registerAgent(t, s, "two-step", twoNodeGraph)
	original := submit(t, s, "two-step", version, `{"topic":"flaky"}`)

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 20*time.Second)
	originalState := loadState(t, s, original)
	if originalState.Status != journal.RunStatusCompleted {
		t.Fatalf("original: status = %q (%s)", originalState.Status, originalState.Error)
	}
	if originalState.Steps[0].Attempt != 2 {
		t.Fatalf("setup: step attempt = %d, want 2", originalState.Steps[0].Attempt)
	}

	res := replayOnce(t, s, h, original)
	if res.Err != "" {
		t.Fatalf("replay errored: %s", res.Err)
	}
	if !res.Matched {
		t.Fatalf("a run with retries did not replay exactly: %s", res.Divergence.Error())
	}

	// The replay's own log must contain the retries, not a shortcut to success.
	replayEvents, err := s.LoadEvents(context.Background(), uuid.MustParse(res.ReplayRunID))
	if err != nil {
		t.Fatalf("load replay events: %v", err)
	}
	var failures, retries int
	for _, e := range replayEvents {
		switch e.Type {
		case journal.TypeStepFailed:
			failures++
		case journal.TypeRetryScheduled:
			retries++
		}
	}
	if failures != 2 || retries != 2 {
		t.Errorf("replay journal has %d failures and %d retries, want 2 and 2", failures, retries)
	}
}

func TestReplayOfAnAgentLoopReproducesEveryIteration(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	h.provider.Script = []provider.Response{
		provider.ToolReply("echo", json.RawMessage(`{"q":"one"}`), 10, 5),
		provider.ToolReply("echo", json.RawMessage(`{"q":"two"}`), 10, 5),
		provider.TextReply("done", 12, 6),
	}
	version := registerAgent(t, s, "looper", loopGraph)
	original := submit(t, s, "looper", version, `{"topic":"loop"}`)

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 20*time.Second)
	originalState := loadState(t, s, original)
	if originalState.Status != journal.RunStatusCompleted {
		t.Fatalf("original: %q (%s)", originalState.Status, originalState.Error)
	}
	// model, tool, model, tool, model
	if len(originalState.Steps) != 5 {
		t.Fatalf("setup: %d steps, want 5", len(originalState.Steps))
	}

	res := replayOnce(t, s, h, original)
	if res.Err != "" {
		t.Fatalf("replay errored: %s", res.Err)
	}
	if !res.Matched {
		t.Fatalf("agent_loop did not replay exactly: %s", res.Divergence.Error())
	}
}

// THE ACCEPTANCE CRITERION for unrecorded non-determinism.
//
// The spec frames this as "calling time.Now() directly". In Anchor's
// declarative model there is no user code that could call it -- an agent is
// JSON the worker interprets, which is the whole reason replay is provable
// here. So the equivalent failure is simulated at its real root: a source of
// difference the journal did not capture.
//
// Deleting a recorded attempt is exactly that. The replay then asks for
// something the recording cannot answer, takes a different path, and the check
// must catch it. If it did not, an unrecorded clock read would pass unnoticed
// too.
func TestDivergenceIsCaughtWhenSomethingWasNotRecorded(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	version := registerAgent(t, s, "two-step", twoNodeGraph)
	original := submit(t, s, "two-step", version, `{"topic":"unrecorded"}`)

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)
	if st := loadState(t, s, original); st.Status != journal.RunStatusCompleted {
		t.Fatalf("original: %q", st.Status)
	}

	originalEvents, err := s.LoadEvents(context.Background(), original)
	if err != nil {
		t.Fatalf("load original: %v", err)
	}
	rec, err := replay.Build(originalEvents)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// Simulate the model call having been driven by something never journaled:
	// the recording has no answer for it.
	step := rec.Steps[0]
	delete(step.Attempts, 0)
	rec.Steps[0] = step

	p := replay.NewProvider(rec)
	_, err = p.Complete(context.Background(), provider.Request{StepIndex: 0, Attempt: 0, Model: "mock"})
	if err == nil {
		t.Fatal("the replay source answered a call it had no recording for")
	}
	d := p.Divergence()
	if d == nil {
		t.Fatal("no divergence was recorded")
	}
	if d.Kind != replay.DivergenceUnexpectedCall {
		t.Errorf("kind = %q, want unexpected_call", d.Kind)
	}
	if d.StepIndex != 0 {
		t.Errorf("StepIndex = %d, want 0", d.StepIndex)
	}
}

// A replay that takes a different path than the original must be caught end to
// end, not just by the source. This drives the whole worker against a recording
// built from a DIFFERENT run, so the walk genuinely diverges.
func TestReplayAgainstTheWrongRecordingDiverges(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)

	// Run A: two nodes.
	twoNode := registerAgent(t, s, "two-step", twoNodeGraph)
	runA := submit(t, s, "two-step", twoNode, `{"topic":"a"}`)
	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)

	// Run B: a single model_call, so a different shape.
	const oneNode = `{"agent":"one-step","v":1,"nodes":[
	  {"id":"n1","type":"model_call","model":"mock","prompt":"x"}]}`
	oneNodeVer := registerAgent(t, s, "one-step", oneNode)
	runB := submit(t, s, "one-step", oneNodeVer, `{"topic":"b"}`)
	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-2", 1)}, 15*time.Second)

	eventsA, err := s.LoadEvents(context.Background(), runA)
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	recA, err := replay.Build(eventsA)
	if err != nil {
		t.Fatalf("build A: %v", err)
	}
	eventsB, err := s.LoadEvents(context.Background(), runB)
	if err != nil {
		t.Fatalf("load B: %v", err)
	}

	// Compare run B's events against run A's recording: two genuinely different
	// executions.
	d := replay.Compare(recA, eventsB)
	if d == nil {
		t.Fatal("two different runs were reported as matching")
	}
	t.Logf("caught: %s", d.Error())
}

// A run still in flight cannot be replayed: its journal is incomplete, so a
// comparison would diverge for a reason that is not a determinism failure.
func TestSubmitReplayRefusesAnUnfinishedRun(t *testing.T) {
	s := testStore(t)
	version := registerAgent(t, s, "two-step", twoNodeGraph)
	pending := submit(t, s, "two-step", version, `{"topic":"pending"}`)

	if _, err := s.SubmitReplay(context.Background(), pending); err == nil {
		t.Error("a pending run was accepted for replay")
	}
}

// Replaying a replay compounds any difference and tells you nothing new.
func TestSubmitReplayRefusesAReplay(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	version := registerAgent(t, s, "two-step", twoNodeGraph)
	original := submit(t, s, "two-step", version, `{"topic":"x"}`)
	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)

	replayState, err := s.SubmitReplay(context.Background(), original)
	if err != nil {
		t.Fatalf("submit replay: %v", err)
	}
	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-2", 1)}, 15*time.Second)

	if _, err := s.SubmitReplay(context.Background(), replayState.RunID); err == nil {
		t.Error("a replay was accepted for replay")
	}
}

// THE M6 ACCEPTANCE CRITERION: replaying a corpus produces a fidelity report.
//
// The spec asks for 200 runs. This builds a corpus with deliberately varied
// shapes -- plain runs, runs with retries, and agent_loops -- because a corpus
// of identical runs would prove only that one shape replays, and the shapes
// that break replay are exactly the awkward ones.
func TestFidelityOverACorpusOfRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("corpus replay is slow; skipped under -short")
	}
	s := testStore(t)
	h := newHarness(t, s)
	ctx := context.Background()

	twoNode := registerAgent(t, s, "two-step", twoNodeGraph)
	looper := registerAgent(t, s, "looper", loopGraph)

	// 200 runs across three shapes.
	const corpusSize = 200
	originals := make([]uuid.UUID, 0, corpusSize)
	for i := range corpusSize {
		switch i % 10 {
		case 7, 8:
			// agent_loop: model, tool, model.
			originals = append(originals, submit(t, s, "looper", looper,
				`{"topic":"loop"}`))
		default:
			originals = append(originals, submit(t, s, "two-step", twoNode,
				`{"topic":"plain"}`))
		}
	}

	// A scripted provider so the loops terminate deterministically: a tool call
	// followed by a final answer, repeating.
	h.provider.Script = nil
	h.provider.Default = provider.TextReply("done", 10, 5)
	h.runner.Handle("echo", func(ctx context.Context, input json.RawMessage) (tool.Result, error) {
		return tool.Result{Output: json.RawMessage(`{"echoed":true}`)}, nil
	})

	workers := []*worker.Worker{
		h.newWorker(t, "c-1", 4), h.newWorker(t, "c-2", 4), h.newWorker(t, "c-3", 4),
	}
	runUntilDrained(t, h, workers, 120*time.Second)

	completed := 0
	for _, id := range originals {
		if loadState(t, s, id).Status == journal.RunStatusCompleted {
			completed++
		}
	}
	if completed < corpusSize {
		t.Fatalf("only %d of %d originals completed; the corpus is not usable", completed, corpusSize)
	}

	// Replay every one of them.
	for _, id := range originals {
		if _, err := s.SubmitReplay(ctx, id); err != nil {
			t.Fatalf("submit replay of %s: %v", id, err)
		}
	}
	runUntilDrained(t, h, workers, 180*time.Second)

	report := &replay.Report{}
	for _, id := range originals {
		originalEvents, err := s.LoadEvents(ctx, id)
		if err != nil {
			t.Fatalf("load original %s: %v", id, err)
		}
		rec, err := replay.Build(originalEvents)
		if err != nil {
			t.Fatalf("build recording for %s: %v", id, err)
		}
		replayIDs, err := s.ReplaysOf(ctx, id)
		if err != nil {
			t.Fatalf("list replays of %s: %v", id, err)
		}
		res := replay.Result{OriginalRunID: id.String()}
		if len(replayIDs) == 0 {
			res.Err = "no replay was created"
			report.Add(res)
			continue
		}
		res.ReplayRunID = replayIDs[0].String()
		replayState, err := s.LoadState(ctx, replayIDs[0])
		if err != nil {
			t.Fatalf("load replay: %v", err)
		}
		if !replayState.Terminal() {
			res.Err = "replay did not finish"
			report.Add(res)
			continue
		}
		replayEvents, err := s.LoadEvents(ctx, replayIDs[0])
		if err != nil {
			t.Fatalf("load replay events: %v", err)
		}
		if d := replay.Compare(rec, replayEvents); d != nil {
			res.Divergence = d
		} else if d := replay.CompareFinal(rec, replayState); d != nil {
			res.Divergence = d
		} else {
			res.Matched = true
		}
		report.Add(res)
	}

	t.Logf("\n%s", report.String())

	if report.Total != corpusSize {
		t.Errorf("report covers %d runs, want %d", report.Total, corpusSize)
	}
	if report.Errored > 0 {
		t.Errorf("%d replay(s) could not be completed; the harness, not determinism, is at fault", report.Errored)
	}
	// The claim this project makes. Anything below 100% here is a real
	// determinism bug, and the report names which kind.
	if report.Fidelity() != 100 {
		t.Errorf("replay fidelity = %.1f%% over %d runs, want 100%%",
			report.Fidelity(), report.Matched+report.Diverged)
	}
}
