package docker

import (
	"context"
	"strings"
	"testing"

	"github.com/docker/docker/client"

	"github.com/RuustRun/agent/internal/contract"
)

// TestRunRelease exercises the one-shot release container against a real Docker
// daemon: a zero-exit command succeeds, a non-zero exit is surfaced as an error,
// and in BOTH cases the container's output is captured into the release report.
// Asserting the captured text (not just the exit code) is deliberate: an earlier
// version passed this test whilst silently capturing nothing, which shipped a
// "succeeded, no output" bug to production.
func TestRunRelease(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("no docker client")
	}
	ctx := context.Background()
	if _, err := cli.Ping(ctx); err != nil {
		t.Skip("docker not reachable")
	}
	e := &engineClient{cli: cli, publishHost: "127.0.0.1"}
	if err := e.EnsureImage(ctx, "alpine:3.19"); err != nil {
		t.Fatalf("ensure image: %v", err)
	}

	// Zero-exit: succeeds, and stdout is captured into the report.
	if err := e.runRelease(ctx, contract.WorkloadSpec{ID: "reltest", ImageRef: "alpine:3.19", ReleaseCommand: "echo captured-stdout; exit 0"}); err != nil {
		t.Errorf("a zero-exit release should succeed, got: %v", err)
	}
	if r := e.releaseReports["reltest"]; r == nil || r.Status != "succeeded" || !strings.Contains(r.Log, "captured-stdout") {
		t.Errorf("a successful release must capture its stdout, got: %+v", r)
	}

	// Non-zero exit: returns an error, and stderr is still captured.
	if err := e.runRelease(ctx, contract.WorkloadSpec{ID: "reltest", ImageRef: "alpine:3.19", ReleaseCommand: "echo failing-stderr 1>&2; exit 7"}); err == nil {
		t.Error("a non-zero-exit release should return an error")
	}
	if r := e.releaseReports["reltest"]; r == nil || r.Status != "failed" || !strings.Contains(r.Log, "failing-stderr") {
		t.Errorf("a failed release must capture its stderr, got: %+v", r)
	}
}
