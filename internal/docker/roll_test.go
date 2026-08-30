package docker

import (
	"context"
	"testing"

	"github.com/docker/docker/client"

	"github.com/RuustRun/agent/internal/contract"
)

// TestRollCoexistThenDrain proves the zero-downtime primitive at the Docker layer:
// two versions of the same replica slot run side by side (distinct names carrying
// the version), and Drain gracefully removes the old one, leaving the new one.
func TestRollCoexistThenDrain(t *testing.T) {
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
		t.Fatalf("ensure image: %v", err)
	}

	const id = "roll-test"
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

	// The old container (start half already done), then the new one beside it.
	oldC, err := e.Create(ctx, spec, "v-old", 0)
	if err != nil {
		t.Fatalf("create old: %v", err)
	}
	newC, err := e.Create(ctx, spec, "v-new", 0)
	if err != nil {
		t.Fatalf("create new (should coexist with old): %v", err)
	}
	if oldC.ID == newC.ID {
		t.Fatal("old and new must be distinct containers")
	}

	count := func() int {
		list, _ := e.List(ctx)
		n := 0
		for _, c := range list {
			if c.WorkloadID == id {
				n++
			}
		}
		return n
	}
	if got := count(); got != 2 {
		t.Fatalf("old and new should run side by side, got %d containers", got)
	}

	// Drain the old one; the new one survives.
	if err := e.Drain(ctx, oldC.ID); err != nil {
		t.Fatalf("drain old: %v", err)
	}
	if got := count(); got != 1 {
		t.Fatalf("after draining the old, exactly one container should remain, got %d", got)
	}
	list, _ := e.List(ctx)
	for _, c := range list {
		if c.WorkloadID == id && c.SpecVersion != "v-new" {
			t.Errorf("the survivor should be the new version, got %s", c.SpecVersion)
		}
	}
}
