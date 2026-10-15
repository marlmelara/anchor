// Command anchorctl is Anchor's operator CLI: apply migrations, rebuild the
// projections from the event log, and register agents.
//
// The rebuild subcommand is the interesting one. It exists to keep the claim
// "run_events is the source of truth" honest: if the projections cannot be
// recomputed from the log, they were never a projection, they were the state.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/marlmelara/anchor/internal/replay"
	"github.com/marlmelara/anchor/internal/store"
)

const defaultDSN = "postgres://anchor:anchor@localhost:5433/anchor?sslmode=disable"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "anchorctl: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: anchorctl <command> [flags]

commands:
  migrate            apply pending schema migrations
  rebuild            recompute the runs and steps projections from run_events
  agent-register     register an agent definition, returning its version
  submit             submit a run
  show               fold a run from its log and print the resulting state
  replay             replay one run and report whether it reproduced exactly
  fidelity           replay a corpus of runs and report the fidelity percentage

Set ANCHOR_DATABASE_URL or pass -dsn.
`)
}

func run() error {
	if len(os.Args) < 2 {
		usage()
		return errors.New("no command given")
	}

	// A single signal-aware context so a long rebuild stops cleanly on Ctrl-C
	// rather than leaving a transaction open until the server times it out.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "migrate":
		return cmdMigrate(ctx, args)
	case "rebuild":
		return cmdRebuild(ctx, args)
	case "agent-register":
		return cmdAgentRegister(ctx, args)
	case "submit":
		return cmdSubmit(ctx, args)
	case "show":
		return cmdShow(ctx, args)
	case "replay":
		return cmdReplay(ctx, args)
	case "fidelity":
		return cmdFidelity(ctx, args)
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// schemaFlag is set by every subcommand so operator commands can be pointed at
// a non-default search_path -- the chaos demo runs in its own schema so it
// never destroys whatever is in the development database.
var schemaFlag string

func addSchemaFlag(fs *flag.FlagSet) {
	fs.StringVar(&schemaFlag, "schema", "", "postgres search_path (default: public)")
}

// openStore resolves the DSN and connects.
func openStore(ctx context.Context, dsn string) (*store.Store, error) {
	if dsn == "" {
		dsn = os.Getenv("ANCHOR_DATABASE_URL")
	}
	if dsn == "" {
		dsn = defaultDSN
	}
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return store.Open(connectCtx, store.Config{DSN: dsn, MaxConns: 8, Schema: schemaFlag})
}

func cmdMigrate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	dsn := fs.String("dsn", "", "postgres DSN")
	addSchemaFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := openStore(ctx, *dsn)
	if err != nil {
		return err
	}
	defer st.Close()

	applied, err := st.Migrate(ctx)
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		fmt.Println("schema is up to date")
		return nil
	}
	for _, name := range applied {
		fmt.Printf("applied %s\n", name)
	}
	return nil
}

func cmdRebuild(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("rebuild", flag.ExitOnError)
	dsn := fs.String("dsn", "", "postgres DSN")
	addSchemaFlag(fs)
	runID := fs.String("run", "", "rebuild only this run id (default: every run)")
	verify := fs.Bool("verify", false, "report differences instead of rewriting the projections")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := openStore(ctx, *dsn)
	if err != nil {
		return err
	}
	defer st.Close()

	var res *store.RebuildResult
	if *runID != "" {
		id, err := uuid.Parse(*runID)
		if err != nil {
			return fmt.Errorf("parse -run: %w", err)
		}
		res, err = st.RebuildRun(ctx, id, *verify)
		if err != nil {
			return err
		}
	} else {
		res, err = st.RebuildAll(ctx, *verify)
		if err != nil {
			return err
		}
	}

	if *verify {
		if len(res.Mismatch) == 0 {
			fmt.Printf("verified %d run(s), %d step(s): projections match the fold\n", res.Runs, res.Steps)
			return nil
		}
		fmt.Printf("verified %d run(s): %d mismatch(es)\n", res.Runs, len(res.Mismatch))
		for _, m := range res.Mismatch {
			fmt.Printf("  %s\n", m)
		}
		// A mismatch is a real defect in the append path, not a warning.
		return errors.New("projections do not match the event log")
	}

	fmt.Printf("rebuilt %d run(s), %d step(s) from run_events\n", res.Runs, res.Steps)
	return nil
}

func cmdAgentRegister(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent-register", flag.ExitOnError)
	dsn := fs.String("dsn", "", "postgres DSN")
	addSchemaFlag(fs)
	name := fs.String("name", "", "agent name")
	file := fs.String("file", "", "path to the agent definition JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *file == "" {
		return errors.New("-name and -file are required")
	}
	def, err := os.ReadFile(*file)
	if err != nil {
		return fmt.Errorf("read definition: %w", err)
	}
	st, err := openStore(ctx, *dsn)
	if err != nil {
		return err
	}
	defer st.Close()

	version, err := st.RegisterAgent(ctx, *name, def)
	if err != nil {
		return err
	}
	fmt.Printf("registered agent %s v%d\n", *name, version)
	return nil
}

func cmdSubmit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	dsn := fs.String("dsn", "", "postgres DSN")
	addSchemaFlag(fs)
	tenant := fs.String("tenant", "default", "tenant id")
	agent := fs.String("agent", "", "agent name")
	version := fs.Int("agent-version", 0, "agent version (default: latest)")
	input := fs.String("input", "{}", "run input as JSON")
	budgetTokens := fs.Int64("budget-tokens", 100000, "token budget")
	budgetMicros := fs.Int64("budget-micros", 500_000, "spending cap in micro-dollars (1e-6 USD); 500000 == $0.50")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *agent == "" {
		return errors.New("-agent is required")
	}
	st, err := openStore(ctx, *dsn)
	if err != nil {
		return err
	}
	defer st.Close()

	v := *version
	if v == 0 {
		v, err = st.LatestAgentVersion(ctx, *agent)
		if err != nil {
			return err
		}
	}

	state, err := st.SubmitRun(ctx, store.SubmitRequest{
		TenantID:     *tenant,
		AgentName:    *agent,
		AgentVersion: v,
		Input:        json.RawMessage(*input),
		BudgetTokens: *budgetTokens,
		BudgetMicros: *budgetMicros,
	})
	if err != nil {
		return err
	}
	fmt.Printf("submitted run %s (agent %s v%d)\n", state.RunID, *agent, v)
	return nil
}

func cmdShow(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("show", flag.ExitOnError)
	dsn := fs.String("dsn", "", "postgres DSN")
	addSchemaFlag(fs)
	runID := fs.String("run", "", "run id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runID == "" {
		return errors.New("-run is required")
	}
	id, err := uuid.Parse(*runID)
	if err != nil {
		return fmt.Errorf("parse -run: %w", err)
	}
	st, err := openStore(ctx, *dsn)
	if err != nil {
		return err
	}
	defer st.Close()

	state, err := st.LoadState(ctx, id)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(state)
}

// cmdReplay replays one run and reports whether it reproduced exactly.
func cmdReplay(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	dsn := fs.String("dsn", "", "postgres DSN")
	addSchemaFlag(fs)
	runID := fs.String("run", "", "run id to replay")
	wait := fs.Duration("wait", 60*time.Second, "how long to wait for the replay to finish")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runID == "" {
		return errors.New("-run is required")
	}
	id, err := uuid.Parse(*runID)
	if err != nil {
		return fmt.Errorf("parse -run: %w", err)
	}
	st, err := openStore(ctx, *dsn)
	if err != nil {
		return err
	}
	defer st.Close()

	res, err := replayAndCompare(ctx, st, id, *wait)
	if err != nil {
		return err
	}
	if res.Err != "" {
		return fmt.Errorf("replay could not complete: %s", res.Err)
	}
	if res.Matched {
		fmt.Printf("run %s replayed exactly (replay %s)\n", res.OriginalRunID, res.ReplayRunID)
		return nil
	}
	fmt.Printf("run %s DIVERGED\n  %s\n", res.OriginalRunID, res.Divergence.Error())
	return errors.New("replay diverged")
}

// cmdFidelity replays a corpus and reports the percentage that matched.
func cmdFidelity(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fidelity", flag.ExitOnError)
	dsn := fs.String("dsn", "", "postgres DSN")
	addSchemaFlag(fs)
	tenant := fs.String("tenant", "", "limit to one tenant (default: all)")
	limit := fs.Int("limit", 200, "how many runs to replay")
	wait := fs.Duration("wait", 120*time.Second, "how long to wait for each replay")
	asJSON := fs.Bool("json", false, "emit the full report as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := openStore(ctx, *dsn)
	if err != nil {
		return err
	}
	defer st.Close()

	ids, err := st.ReplayableRuns(ctx, *tenant, *limit)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return errors.New("no completed runs to replay")
	}
	fmt.Fprintf(os.Stderr, "replaying %d run(s)...\n", len(ids))

	report := &replay.Report{}
	for i, id := range ids {
		res, err := replayAndCompare(ctx, st, id, *wait)
		if err != nil {
			res = replay.Result{OriginalRunID: id.String(), Err: err.Error()}
		}
		report.Add(res)
		if (i+1)%25 == 0 {
			fmt.Fprintf(os.Stderr, "  %d/%d\n", i+1, len(ids))
		}
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Print(report.String())
	if report.Diverged > 0 {
		return fmt.Errorf("%d of %d replays diverged", report.Diverged, report.Matched+report.Diverged)
	}
	return nil
}

// replayAndCompare submits a replay, waits for it, and compares the two logs.
func replayAndCompare(ctx context.Context, st *store.Store, originalID uuid.UUID, wait time.Duration) (replay.Result, error) {
	res := replay.Result{OriginalRunID: originalID.String()}

	originalEvents, err := st.LoadEvents(ctx, originalID)
	if err != nil {
		return res, fmt.Errorf("load original: %w", err)
	}
	rec, err := replay.Build(originalEvents)
	if err != nil {
		return res, fmt.Errorf("build recording: %w", err)
	}

	replayState, err := st.SubmitReplay(ctx, originalID)
	if err != nil {
		return res, fmt.Errorf("submit replay: %w", err)
	}
	res.ReplayRunID = replayState.RunID.String()

	// Poll rather than subscribe: this is an operator command run occasionally,
	// not a hot path, and polling has no connection to leak if it is
	// interrupted.
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		current, err := st.LoadState(ctx, replayState.RunID)
		if err != nil {
			return res, fmt.Errorf("load replay: %w", err)
		}
		if current.Terminal() {
			replayEvents, err := st.LoadEvents(ctx, replayState.RunID)
			if err != nil {
				return res, fmt.Errorf("load replay events: %w", err)
			}
			if d := replay.Compare(rec, replayEvents); d != nil {
				res.Divergence = d
				return res, nil
			}
			if d := replay.CompareFinal(rec, current); d != nil {
				res.Divergence = d
				return res, nil
			}
			res.Matched = true
			return res, nil
		}
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	res.Err = fmt.Sprintf("replay %s did not finish within %s (is a worker running?)", res.ReplayRunID, wait)
	return res, nil
}
