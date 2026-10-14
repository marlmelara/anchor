// Package sandbox runs tool calls inside short-lived Docker containers.
//
// The threat model, stated plainly: this protects the worker and other runs
// from a buggy tool, or a tool the model misuses. It does NOT protect against a
// determined attacker with a container-escape exploit, because containers share
// the host kernel. Stronger isolation means gVisor or Firecracker microVMs, or
// separate hosts. Knowing the limit of your own defence is the point.
package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"

	"github.com/marlmelara/anchor/internal/tool"
)

// Labels every Anchor-created container carries, so the orphan reaper can find
// them again after a worker dies without cleaning up.
const (
	LabelManaged  = "anchor.managed"
	LabelWorkerID = "anchor.worker_id"
	LabelTool     = "anchor.tool"
)

// Defaults applied when a tool definition leaves a limit unset. Every one of
// them is a cap, never a grant: a tool can ask for less, never more than the
// ceilings in Config.
const (
	DefaultMemoryBytes = 256 << 20 // 256 MB
	DefaultNanoCPUs    = 500_000_000
	DefaultTimeout     = 30 * time.Second
	DefaultPidsLimit   = 64
	maxOutputBytes     = 4 << 20 // 4 MB of stdout
)

// Config bounds what any tool may request.
type Config struct {
	WorkerID string

	MaxMemoryBytes int64
	MaxNanoCPUs    int64
	MaxTimeout     time.Duration
	PidsLimit      int64

	// AllowEgress is a kill switch: with it false, a tool declaring egress
	// still gets no network. Lets a deployment refuse network access fleet-wide
	// regardless of what individual tools ask for.
	AllowEgress bool

	Logf func(format string, args ...any)
}

func (c *Config) setDefaults() {
	if c.MaxMemoryBytes <= 0 {
		c.MaxMemoryBytes = DefaultMemoryBytes
	}
	if c.MaxNanoCPUs <= 0 {
		c.MaxNanoCPUs = DefaultNanoCPUs
	}
	if c.MaxTimeout <= 0 {
		c.MaxTimeout = DefaultTimeout
	}
	if c.PidsLimit <= 0 {
		c.PidsLimit = DefaultPidsLimit
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
}

// Docker is a tool.Runner backed by the Docker API.
type Docker struct {
	cli *client.Client
	cfg Config
}

// NewDocker connects to the local Docker daemon.
func NewDocker(cfg Config) (*Docker, error) {
	cfg.setDefaults()
	// APIVersionNegotiation keeps this working against older daemons instead of
	// failing on a version the client assumed.
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("connect to docker: %w", err)
	}
	return &Docker{cli: cli, cfg: cfg}, nil
}

// Ping verifies the daemon is reachable.
func (d *Docker) Ping(ctx context.Context) error {
	if _, err := d.cli.Ping(ctx); err != nil {
		return fmt.Errorf("ping docker: %w", err)
	}
	return nil
}

// Close releases the Docker client.
func (d *Docker) Close() error { return d.cli.Close() }

// Run executes one tool call in its own container.
//
// The sequence is: create -> attach -> write input to stdin -> start -> wait ->
// read output -> remove. The container is removed on every path out of this
// function, including panics and timeouts.
func (d *Docker) Run(ctx context.Context, def tool.Definition, input json.RawMessage) (tool.Result, error) {
	if def.Image == "" {
		return tool.Result{}, fmt.Errorf("tool %q has no image", def.Name)
	}

	timeout := d.cfg.MaxTimeout
	if def.Timeout > 0 && def.Timeout < timeout {
		timeout = def.Timeout
	}
	// The hard wall-clock limit. A hung tool is killed, not waited on forever.
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	created, err := d.cli.ContainerCreate(runCtx,
		&container.Config{
			Image: def.Image,
			Cmd:   def.Command,
			// "nobody". A tool that finds a writable path still cannot act as
			// root inside its own namespace.
			User:         "65534:65534",
			OpenStdin:    true,
			StdinOnce:    true,
			AttachStdin:  true,
			AttachStdout: true,
			AttachStderr: true,
			Labels: map[string]string{
				LabelManaged:  "true",
				LabelWorkerID: d.cfg.WorkerID,
				LabelTool:     def.Name,
			},
		},
		&container.HostConfig{
			NetworkMode:    container.NetworkMode(d.networkFor(def)),
			ReadonlyRootfs: true,
			// Drop every Linux capability. Even root inside the container keeps
			// none of the special privileges root normally has.
			CapDrop: []string{"ALL"},
			// Stops a process gaining privileges through a setuid binary.
			SecurityOpt: []string{"no-new-privileges"},
			Resources: container.Resources{
				Memory:    d.memoryFor(def),
				NanoCPUs:  d.cpuFor(def),
				PidsLimit: &d.cfg.PidsLimit,
			},
			AutoRemove: false, // removed explicitly below, so failures are visible
		}, nil, nil, "")
	if err != nil {
		return tool.Result{}, fmt.Errorf("create container for %q: %w", def.Name, err)
	}

	// Removal uses a FRESH context on purpose. If the tool timed out, runCtx is
	// already cancelled and a cleanup call on it would fail instantly, leaking
	// the container.
	defer func() {
		removeCtx, removeCancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
		defer removeCancel()
		if err := d.cli.ContainerRemove(removeCtx, created.ID,
			container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
			d.cfg.Logf("remove container %s for tool %q: %v", created.ID[:12], def.Name, err)
		}
	}()

	attached, err := d.cli.ContainerAttach(runCtx, created.ID, container.AttachOptions{
		Stream: true, Stdin: true, Stdout: true, Stderr: true,
	})
	if err != nil {
		return tool.Result{}, fmt.Errorf("attach to container: %w", err)
	}
	defer attached.Close()

	if err := d.cli.ContainerStart(runCtx, created.ID, container.StartOptions{}); err != nil {
		return tool.Result{}, fmt.Errorf("start container: %w", err)
	}

	// Input goes in as JSON on stdin; closing it signals end of input, which is
	// what lets a tool read to EOF rather than guessing when to stop.
	if len(input) == 0 {
		input = []byte(`{}`)
	}
	if _, err := attached.Conn.Write(append(input, '\n')); err != nil {
		return tool.Result{}, fmt.Errorf("write tool input: %w", err)
	}
	if err := attached.CloseWrite(); err != nil {
		d.cfg.Logf("close stdin for %q: %v", def.Name, err)
	}

	// Demultiplex the Docker stream into stdout and stderr, bounded so a
	// runaway tool cannot exhaust the worker's memory through its output.
	var stdout, stderr strings.Builder
	copyErr := make(chan error, 1)
	go func() {
		_, err := stdcopy.StdCopy(
			limitedWriter{w: &stdout, limit: maxOutputBytes},
			limitedWriter{w: &stderr, limit: maxOutputBytes},
			attached.Reader)
		copyErr <- err
	}()

	statusCh, errCh := d.cli.ContainerWait(runCtx, created.ID, container.WaitConditionNotRunning)
	var exitCode int
	select {
	case err := <-errCh:
		if err != nil {
			if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
				return tool.Result{}, &TimeoutError{Tool: def.Name, Timeout: timeout}
			}
			return tool.Result{}, fmt.Errorf("wait for container: %w", err)
		}
	case status := <-statusCh:
		exitCode = int(status.StatusCode)
	case <-runCtx.Done():
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			return tool.Result{}, &TimeoutError{Tool: def.Name, Timeout: timeout}
		}
		return tool.Result{}, runCtx.Err()
	}

	// Give the stream a moment to drain after exit.
	select {
	case err := <-copyErr:
		if err != nil && !errors.Is(err, io.EOF) {
			d.cfg.Logf("read output for %q: %v", def.Name, err)
		}
	case <-time.After(2 * time.Second):
		d.cfg.Logf("timed out draining output for %q", def.Name)
	}

	// 137 is 128 + SIGKILL(9): the kernel OOM killer, or Docker enforcing the
	// memory cap. Naming it beats reporting a bare exit code nobody can read.
	if exitCode == 137 {
		return tool.Result{
			ExitCode: exitCode,
			Stderr:   stderr.String(),
		}, &LimitError{Tool: def.Name, Limit: "memory", Detail: "killed by the OOM killer (exit 137)"}
	}

	return tool.Result{
		Output:   []byte(strings.TrimSpace(stdout.String())),
		ExitCode: exitCode,
		Stderr:   stderr.String(),
	}, nil
}

// networkFor returns the container's network mode.
//
// "none" unless the tool declares egress AND the deployment permits it. Default
// deny: a tool that forgot to declare egress fails loudly rather than silently
// gaining access to everything the worker can reach.
func (d *Docker) networkFor(def tool.Definition) string {
	if def.Egress && d.cfg.AllowEgress {
		return "bridge"
	}
	return "none"
}

func (d *Docker) memoryFor(def tool.Definition) int64 {
	if def.MemoryBytes > 0 && def.MemoryBytes < d.cfg.MaxMemoryBytes {
		return def.MemoryBytes
	}
	return d.cfg.MaxMemoryBytes
}

func (d *Docker) cpuFor(def tool.Definition) int64 {
	if def.NanoCPUs > 0 && def.NanoCPUs < d.cfg.MaxNanoCPUs {
		return def.NanoCPUs
	}
	return d.cfg.MaxNanoCPUs
}

// ReapOrphans removes containers Anchor created that nothing is waiting on.
//
// A worker killed mid-tool-call leaves its container running: the deferred
// removal never ran. This sweeps those on startup. Leftover containers after a
// chaos run are a bug, not an inconvenience -- each one holds memory and a CPU
// share the host has not reclaimed.
func (d *Docker) ReapOrphans(ctx context.Context) (int, error) {
	args := filters.NewArgs()
	args.Add("label", LabelManaged+"=true")

	containers, err := d.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: args})
	if err != nil {
		return 0, fmt.Errorf("list anchor containers: %w", err)
	}

	removed := 0
	for _, c := range containers {
		if err := d.cli.ContainerRemove(ctx, c.ID,
			container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
			d.cfg.Logf("reap container %s: %v", c.ID[:12], err)
			continue
		}
		removed++
	}
	return removed, nil
}

// CountContainers reports how many Anchor containers exist, for tests asserting
// nothing was leaked.
func (d *Docker) CountContainers(ctx context.Context) (int, error) {
	args := filters.NewArgs()
	args.Add("label", LabelManaged+"=true")
	containers, err := d.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: args})
	if err != nil {
		return 0, fmt.Errorf("list anchor containers: %w", err)
	}
	return len(containers), nil
}

// TimeoutError means the tool exceeded its wall-clock limit.
type TimeoutError struct {
	Tool    string
	Timeout time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("tool %q exceeded its %s timeout", e.Tool, e.Timeout)
}

// LimitError means the tool was stopped by a resource cap.
type LimitError struct {
	Tool   string
	Limit  string
	Detail string
}

func (e *LimitError) Error() string {
	return fmt.Sprintf("tool %q hit its %s limit: %s", e.Tool, e.Limit, e.Detail)
}

// limitedWriter discards everything past limit bytes. A tool that writes
// without end must not be able to exhaust the worker's memory.
type limitedWriter struct {
	w     *strings.Builder
	limit int
}

func (l limitedWriter) Write(p []byte) (int, error) {
	remaining := l.limit - l.w.Len()
	if remaining <= 0 {
		// Report the write as accepted: returning short would make StdCopy
		// error out, and the truncation is deliberate, not a failure.
		return len(p), nil
	}
	if len(p) > remaining {
		l.w.Write(p[:remaining])
		return len(p), nil
	}
	l.w.Write(p)
	return len(p), nil
}
