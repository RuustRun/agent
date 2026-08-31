package docker

import (
	"context"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// TestVolumeUsageBytes proves the disk-usage measurement: a Postgres container with a
// data volume reports a non-zero size, and a container with no volume reports zero.
func TestVolumeUsageBytes(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("no docker client")
	}
	ctx := context.Background()
	if _, err := cli.Ping(ctx); err != nil {
		t.Skip("docker not reachable")
	}
	e := &engineClient{cli: cli, publishHost: "127.0.0.1"}
	const image = "postgres:16-alpine"
	env := []string{"POSTGRES_USER=ruust", "POSTGRES_PASSWORD=dupw", "POSTGRES_DB=ruust"}
	if err := e.EnsureImage(ctx, image); err != nil {
		t.Fatalf("ensure image: %v", err)
	}

	// The volume must carry the Ruust persistent-volume prefix, since that is what
	// VolumeUsageBytes measures (anonymous image volumes are deliberately ignored).
	const vol = "ruust-vol-dutest"
	cleanup := func() {
		for _, n := range []string{"du-src", "du-novol"} {
			_ = e.cli.ContainerRemove(ctx, n, container.RemoveOptions{Force: true})
		}
		_ = e.cli.VolumeRemove(ctx, vol, true)
	}
	cleanup()
	defer cleanup()

	// A Postgres on a data volume: its initialised cluster is several MB, so usage
	// must be well above zero.
	src := startPostgres(t, e, ctx, "du-src", vol, image, env)
	used, err := e.VolumeUsageBytes(ctx, src)
	if err != nil {
		t.Fatalf("volume usage: %v", err)
	}
	if used < 1024*1024 {
		t.Errorf("postgres volume usage = %d bytes, want at least 1 MB", used)
	}
	t.Logf("postgres volume usage = %d bytes", used)

	// A container with no volume mount reports zero.
	created, err := e.cli.ContainerCreate(ctx,
		&container.Config{Image: image, Env: env, Entrypoint: []string{"sh", "-c", "sleep 60"}},
		&container.HostConfig{RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled}},
		nil, nil, "du-novol")
	if err != nil {
		t.Fatalf("create novol: %v", err)
	}
	if err := e.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		t.Fatalf("start novol: %v", err)
	}
	// Give it a moment to be up for exec.
	time.Sleep(500 * time.Millisecond)
	none, err := e.VolumeUsageBytes(ctx, created.ID)
	if err != nil {
		t.Fatalf("volume usage novol: %v", err)
	}
	if none != 0 {
		t.Errorf("no-volume container usage = %d, want 0", none)
	}
}
