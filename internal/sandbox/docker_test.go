package sandbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/marlmelara/anchor/internal/sandbox"
	"github.com/marlmelara/anchor/internal/tool"
)

// These tests run real containers. There is no useful fake: every property
// being checked -- the OOM killer, a missing network interface, a read-only
// filesystem -- is enforced by the kernel and the daemon, not by Anchor. A mock
// would only test the mock.
//
//	docker pull alpine:3.20
//	ANCHOR_TEST_DOCKER=1 go test ./internal/sandbox/

const testImage = "alpine:3.20"

func testSandbox(t *testing.T) *sandbox.Docker {
	t.Helper()
	if os.Getenv("ANCHOR_TEST_DOCKER") == "" {
		t.Skip("ANCHOR_TEST_DOCKER is not set; skipping container tests")
	}
	d, err := sandbox.NewDocker(sandbox.Config{
		WorkerID:   "test-worker",
		MaxTimeout: 30 * time.Second,
		Logf:       func(format string, args ...any) { t.Logf(format, args...) },
	})
	if err != nil {
		t.Fatalf("connect to docker: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	ctx := context.Background()
	if err := d.Ping(ctx); err != nil {
		t.Skipf("docker daemon unreachable: %v", err)
	}
	// Start from a clean slate so container counts mean something.
	if _, err := d.ReapOrphans(ctx); err != nil {
		t.Fatalf("reap before test: %v", err)
	}
	return d
}

// shellTool builds a tool that runs a shell command in alpine.
func shellTool(name, script string) tool.Definition {
	return tool.Definition{
		Name:       name,
		Image:      testImage,
		Command:    []string{"sh", "-c", script},
		Idempotent: true,
	}
}

func TestSandboxRunsAToolAndReturnsItsOutput(t *testing.T) {
	d := testSandbox(t)
	ctx := context.Background()

	// Reads its JSON input from stdin and echoes it back on stdout.
	def := shellTool("echo", `cat`)
	res, err := d.Run(ctx, def, json.RawMessage(`{"hello":"world"}`))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit code = %d, want 0 (stderr: %s)", res.ExitCode, res.Stderr)
	}
	var out map[string]string
	if err := json.Unmarshal(res.Output, &out); err != nil {
		t.Fatalf("output %q is not JSON: %v", res.Output, err)
	}
	if out["hello"] != "world" {
		t.Errorf("output = %v, want the input echoed back", out)
	}
}

// Acceptance: a tool that tries to use 1 GB of memory is killed.
func TestSandboxKillsAToolThatExceedsItsMemoryCap(t *testing.T) {
	d := testSandbox(t)
	ctx := context.Background()

	// dd with bs=1G allocates a one-gigabyte buffer. The cap is 64 MB, so the
	// kernel's OOM killer stops it.
	//
	// Invoked directly rather than through `sh -c`: a shell wrapper survives
	// the kill and exits 0 itself, which would hide the OOM behind a successful
	// container exit. The command under test has to BE the container's process.
	def := tool.Definition{
		Name:        "memory-hog",
		Image:       testImage,
		Command:     []string{"dd", "if=/dev/zero", "of=/dev/null", "bs=1G", "count=1"},
		MemoryBytes: 64 << 20,
		Idempotent:  true,
	}

	_, err := d.Run(ctx, def, json.RawMessage(`{}`))

	// Either the OOM killer produces exit 137 (reported as a LimitError) or the
	// allocation itself fails inside the container. Both mean the cap held; what
	// must not happen is a clean success.
	if err == nil {
		t.Fatal("a tool allocating 1 GB under a 64 MB cap succeeded; the memory limit is not enforced")
	}
	var limitErr *sandbox.LimitError
	if errors.As(err, &limitErr) {
		if limitErr.Limit != "memory" {
			t.Errorf("limit = %q, want memory", limitErr.Limit)
		}
	}
	t.Logf("memory cap enforced: %v", err)
}

// Acceptance: a tool that tries network access without declaring it fails.
func TestSandboxDeniesNetworkToAToolThatDidNotDeclareEgress(t *testing.T) {
	d := testSandbox(t)
	ctx := context.Background()

	// Egress defaults to false, so the container gets --network none.
	def := shellTool("sneaky", `wget -q -T 3 -O- http://example.com 2>&1; echo "exit=$?"`)

	res, err := d.Run(ctx, def, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	combined := string(res.Output) + res.Stderr
	if strings.Contains(combined, "exit=0") {
		t.Fatalf("the tool reached the network without declaring egress: %s", combined)
	}
	t.Logf("network denied as expected: %s", strings.TrimSpace(combined))
}

// Even with egress declared, a deployment that refuses it fleet-wide wins.
func TestSandboxDeniesEgressWhenTheDeploymentForbidsIt(t *testing.T) {
	if os.Getenv("ANCHOR_TEST_DOCKER") == "" {
		t.Skip("ANCHOR_TEST_DOCKER is not set")
	}
	d, err := sandbox.NewDocker(sandbox.Config{WorkerID: "test-worker", AllowEgress: false})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	def := shellTool("declared-egress", `wget -q -T 3 -O- http://example.com 2>&1; echo "exit=$?"`)
	def.Egress = true // the tool asks for it...

	res, err := d.Run(context.Background(), def, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if strings.Contains(string(res.Output)+res.Stderr, "exit=0") {
		t.Error("the deployment's AllowEgress=false was overridden by the tool's own declaration")
	}
}

func TestSandboxEnforcesAReadOnlyRootFilesystem(t *testing.T) {
	d := testSandbox(t)
	def := shellTool("writer", `touch /tmp-probe-file 2>&1; echo "exit=$?"`)

	res, err := d.Run(context.Background(), def, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if strings.Contains(string(res.Output)+res.Stderr, "exit=0") {
		t.Error("a tool wrote to the root filesystem; ReadonlyRootfs is not in effect")
	}
}

func TestSandboxRunsAsNonRoot(t *testing.T) {
	d := testSandbox(t)
	def := shellTool("whoami", `id -u`)

	res, err := d.Run(context.Background(), def, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	uid := strings.TrimSpace(string(res.Output))
	if uid == "0" {
		t.Error("the tool ran as root")
	}
	if uid != "65534" {
		t.Errorf("uid = %q, want 65534 (nobody)", uid)
	}
}

func TestSandboxKillsAHungTool(t *testing.T) {
	d := testSandbox(t)
	def := shellTool("hang", `sleep 60`)
	def.Timeout = 2 * time.Second

	start := time.Now()
	_, err := d.Run(context.Background(), def, json.RawMessage(`{}`))
	elapsed := time.Since(start)

	var timeoutErr *sandbox.TimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("err = %v, want a TimeoutError", err)
	}
	if elapsed > 20*time.Second {
		t.Errorf("took %s to kill a tool with a 2s timeout", elapsed)
	}
}

// Acceptance: no containers remain. Every path out of Run removes its
// container, including the timeout path -- where the run context is already
// cancelled and a cleanup call made on it would fail instantly.
func TestSandboxLeavesNoContainersBehind(t *testing.T) {
	d := testSandbox(t)
	ctx := context.Background()

	cases := []tool.Definition{
		shellTool("ok", `cat`),
		shellTool("fails", `exit 3`),
		shellTool("writes-nothing", `true`),
	}
	for _, def := range cases {
		_, _ = d.Run(ctx, def, json.RawMessage(`{}`))
	}

	timeoutTool := shellTool("hangs", `sleep 30`)
	timeoutTool.Timeout = time.Second
	_, _ = d.Run(ctx, timeoutTool, json.RawMessage(`{}`))

	memTool := tool.Definition{
		Name:        "oom",
		Image:       testImage,
		Command:     []string{"dd", "if=/dev/zero", "of=/dev/null", "bs=1G", "count=1"},
		MemoryBytes: 32 << 20,
	}
	_, _ = d.Run(ctx, memTool, json.RawMessage(`{}`))

	// Removal is asynchronous on the daemon side; give it a moment.
	deadline := time.Now().Add(15 * time.Second)
	var remaining int
	for time.Now().Before(deadline) {
		var err error
		remaining, err = d.CountContainers(ctx)
		if err != nil {
			t.Fatalf("count containers: %v", err)
		}
		if remaining == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if remaining != 0 {
		t.Errorf("%d Anchor container(s) left behind after 5 tool calls including a timeout and an OOM", remaining)
	}
}

func TestReapOrphansRemovesLeftoverContainers(t *testing.T) {
	d := testSandbox(t)
	ctx := context.Background()

	// A worker killed mid-tool-call leaves its container behind, because the
	// deferred removal never ran. Simulate that with a long-running tool whose
	// own context is cancelled.
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		def := shellTool("abandoned", `sleep 30`)
		def.Timeout = 25 * time.Second
		_, _ = d.Run(runCtx, def, json.RawMessage(`{}`))
	}()

	time.Sleep(2 * time.Second)
	cancel()
	<-done

	// Whether the deferred removal won the race or not, the reaper must leave
	// nothing behind.
	if _, err := d.ReapOrphans(ctx); err != nil {
		t.Fatalf("reap: %v", err)
	}
	remaining, err := d.CountContainers(ctx)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 0 {
		t.Errorf("%d container(s) survived the reaper", remaining)
	}
}
