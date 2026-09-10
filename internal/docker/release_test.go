package docker

import (
	"context"
	"os/exec"
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

// TestRunReleaseOverridesImageEntrypoint is the regression test for the real bug:
// a build image (e.g. Nixpacks) sets its own ENTRYPOINT, and if the release
// container sets only Cmd, Docker passes "sh -c <cmd>" as ARGUMENTS to that
// entrypoint, which ignores them and exits 0. The release command then never runs
// (the migration silently does nothing, no output), which is exactly what happened
// in production. runRelease must override the entrypoint so the command runs.
func TestRunReleaseOverridesImageEntrypoint(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("no docker client")
	}
	ctx := context.Background()
	if _, err := cli.Ping(ctx); err != nil {
		t.Skip("docker not reachable")
	}
	e := &engineClient{cli: cli, publishHost: "127.0.0.1"}

	// An image whose ENTRYPOINT swallows any args and exits 0, standing in for a
	// Nixpacks launcher entrypoint.
	tag := "ruust-release-eptest:latest"
	df := "FROM alpine:3.19\nENTRYPOINT [\"/bin/sh\",\"-c\",\"echo ENTRYPOINT-SWALLOWED; exit 0\"]\nCMD [\"true\"]\n"
	build := exec.Command("docker", "build", "-t", tag, "-")
	build.Stdin = strings.NewReader(df)
	if out, err := build.CombinedOutput(); err != nil {
		t.Skipf("cannot build entrypoint test image: %v\n%s", err, out)
	}
	defer func() { _ = exec.Command("docker", "rmi", "-f", tag).Run() }()

	if err := e.runRelease(ctx, contract.WorkloadSpec{ID: "eptest", ImageRef: tag, ReleaseCommand: "echo release-ran-despite-entrypoint"}); err != nil {
		t.Errorf("release should succeed, got: %v", err)
	}
	r := e.releaseReports["eptest"]
	if r == nil || !strings.Contains(r.Log, "release-ran-despite-entrypoint") {
		t.Errorf("release command must run and be captured despite the image ENTRYPOINT, got: %+v", r)
	}
	if r != nil && strings.Contains(r.Log, "ENTRYPOINT-SWALLOWED") {
		t.Errorf("the image ENTRYPOINT should have been overridden, not run, got: %+v", r)
	}
}
