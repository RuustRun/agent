package docker

import (
	"context"
	"testing"

	"github.com/docker/docker/client"

	"github.com/RuustRun/agent/internal/contract"
)

// TestRunRelease exercises the one-shot release container against a real Docker
// daemon: a zero-exit command succeeds, a non-zero exit is surfaced as an error.
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

	if err := e.runRelease(ctx, contract.WorkloadSpec{ID: "reltest", ImageRef: "alpine:3.19", ReleaseCommand: "exit 0"}); err != nil {
		t.Errorf("a zero-exit release should succeed, got: %v", err)
	}
	if err := e.runRelease(ctx, contract.WorkloadSpec{ID: "reltest", ImageRef: "alpine:3.19", ReleaseCommand: "echo failing; exit 7"}); err == nil {
		t.Error("a non-zero-exit release should return an error")
	}
}
