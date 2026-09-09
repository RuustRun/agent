package docker

import (
	"context"
	"testing"

	"github.com/docker/docker/client"

	"github.com/RuustRun/agent/internal/contract"
)

// TestCreate_ReplacesWhenImageChangesUnderSameVersion proves the fix for the BYO
// redeploy start-loop: a container that already runs the slot's version but a DIFFERENT
// image (a rebuild kept the old image serving under the new version's name whilst it
// built) must be REPLACED once the new image is ready, not treated as an idempotent
// no-op. Without this, converge re-issues ActionStart every tick and the deploy hangs
// on "building" forever.
func TestCreate_ReplacesWhenImageChangesUnderSameVersion(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("no docker client")
	}
	ctx := context.Background()
	if _, err := cli.Ping(ctx); err != nil {
		t.Skip("docker not reachable")
	}
	e := &engineClient{cli: cli, publishHost: "127.0.0.1"}
	if err := e.EnsureImage(ctx, "redis:7-alpine"); err != nil {
		t.Fatalf("ensure image 7: %v", err)
	}
	if err := e.EnsureImage(ctx, "redis:6-alpine"); err != nil {
		t.Fatalf("ensure image 6: %v", err)
	}

	const id = "img-change-test"
	spec := contract.WorkloadSpec{
		ID: id, BlobID: "blob", EggID: "egg", ImageRef: "redis:7-alpine",
		Replicas: 1, Port: 6379, PublishPort: 1, HealthCheckPath: "/",
		Limits: contract.ResourceLimits{MemoryMb: 64, CpuFloor: 0.25, CpuBurst: 1.0, PidsLimit: 128, DroppedCaps: []string{}},
	}

	stopOurs := func() {
		list, _ := e.List(ctx)
		for _, c := range list {
			if c.WorkloadID == id {
				_ = e.Stop(ctx, c.ID)
			}
		}
	}
	stopOurs()
	defer stopOurs()

	// First create: version v-1 running the old image.
	if _, err := e.Create(ctx, spec, "v-1", 0); err != nil {
		t.Fatalf("create old-image container: %v", err)
	}

	// Now the "build finished" moment: SAME version, a DIFFERENT image.
	spec2 := spec
	spec2.ImageRef = "redis:6-alpine"
	if _, err := e.Create(ctx, spec2, "v-1", 0); err != nil {
		t.Fatalf("create new-image container: %v", err)
	}

	// Exactly one container for the slot, and it must now run the NEW image (replaced),
	// not the old one (a same-version, wrong-image no-op).
	list, _ := e.List(ctx)
	var mine []Container
	for _, c := range list {
		if c.WorkloadID == id {
			mine = append(mine, c)
		}
	}
	if len(mine) != 1 {
		t.Fatalf("expected exactly one container for the slot, got %d", len(mine))
	}
	if mine[0].ImageRef != "redis:6-alpine" {
		t.Errorf("container should have been replaced with the new image; got ImageRef=%q, want redis:6-alpine", mine[0].ImageRef)
	}
}
