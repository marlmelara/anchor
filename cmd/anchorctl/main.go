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
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
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
	return store.Open(connectCtx, store.Config{DSN: dsn, MaxConns: 8})
}

func cmdMigrate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	dsn := fs.String("dsn", "", "postgres DSN")
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
	tenant := fs.String("tenant", "default", "tenant id")
	agent := fs.String("agent", "", "agent name")
	version := fs.Int("agent-version", 0, "agent version (default: latest)")
	input := fs.String("input", "{}", "run input as JSON")
	budgetTokens := fs.Int64("budget-tokens", 100000, "token budget")
	budgetCents := fs.Int64("budget-cents", 500, "cost budget in cents")
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
		BudgetCents:  *budgetCents,
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
