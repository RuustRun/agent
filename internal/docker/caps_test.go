package docker

import (
	"context"
	"testing"

	"github.com/docker/docker/client"

	"github.com/RuustRun/agent/internal/contract"
)

// TestDroppedCapsHonoursEmpty locks the fix for the database-Egg crash loop: an
// explicit empty DroppedCaps ("drop nothing", what a database Egg sends) must NOT be
// turned into "drop ALL". Only an absent (nil) list defaults to dropping everything.
func TestDroppedCapsHonoursEmpty(t *testing.T) {
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

	cases := []struct {
		name     string
		caps     []string
		wantDrop int // expected len(HostConfig.CapDrop)
		wantAll  bool
	}{
		{"database egg: explicit empty drops nothing", []string{}, 0, false},
		{"web egg: explicit ALL drops all", []string{"ALL"}, 1, true},
		{"unset (nil) still defaults to ALL", nil, 1, true},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id := "capstest-" + string(rune('a'+i))
			spec := contract.WorkloadSpec{
				ID: id, BlobID: "b", EggID: "eg", ImageRef: "alpine:3.19", Port: 0,
				HealthCheckPath: "/",
				Limits: contract.ResourceLimits{
					MemoryMb: 64, CpuFloor: 0.25, CpuBurst: 1.0, PidsLimit: 64,
					ReadOnlyRootfs: false, DroppedCaps: c.caps,
				},
			}
			// clean any leftover
			for _, cc := range mustList(t, e, ctx, id) {
				_ = e.Stop(ctx, cc.ID)
			}
			created, err := e.Create(ctx, spec, "v1", 0)
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			defer func() { _ = e.Stop(ctx, created.ID) }()

			info, err := cli.ContainerInspect(ctx, created.ID)
			if err != nil {
				t.Fatalf("inspect: %v", err)
			}
			var capDrop []string
			if info.HostConfig != nil {
				capDrop = []string(info.HostConfig.CapDrop)
			}
			if len(capDrop) != c.wantDrop {
				t.Fatalf("CapDrop = %v, want %d entries", capDrop, c.wantDrop)
			}
			if c.wantAll && (len(capDrop) != 1 || capDrop[0] != "ALL") {
				t.Errorf("CapDrop = %v, want [ALL]", capDrop)
			}
			if !c.wantAll && len(capDrop) != 0 {
				t.Errorf("CapDrop = %v, want empty (drop nothing)", capDrop)
			}
		})
	}
}

func mustList(t *testing.T, e *engineClient, ctx context.Context, workloadID string) []Container {
	t.Helper()
	list, _ := e.List(ctx)
	out := make([]Container, 0)
	for _, c := range list {
		if c.WorkloadID == workloadID {
			out = append(out, c)
		}
	}
	return out
}
