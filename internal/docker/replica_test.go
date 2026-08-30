package docker

import (
	"context"
	"testing"

	"github.com/docker/docker/client"

	"github.com/RuustRun/agent/internal/contract"
)

// TestReplicaLifecycle exercises horizontal scaling against a real Docker daemon:
// three replicas of one workload run as three containers, each with its own
// replica index and its own distinct ephemeral host port, and each can be stopped
// independently. redis is used because it binds an unprivileged port and stays up.
func TestReplicaLifecycle(t *testing.T) {
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

	const id = "reptest-hs"
	spec := contract.WorkloadSpec{
		ID: id, BlobID: "blob", EggID: "egg", ImageRef: "redis:7-alpine",
		Replicas: 3, Port: 6379, PublishPort: 1, HealthCheckPath: "/",
		// Empty DroppedCaps ("drop nothing") is the real database-Egg profile: the
		// redis:7-alpine entrypoint drops privileges via su-exec and so needs
		// SETUID/SETGID/CHOWN. This also exercises the fix that an explicit empty list
		// is honoured, not turned into "drop ALL".
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
	stopOurs() // clear any leftovers from a prior run
	defer stopOurs()

	for i := 0; i < 3; i++ {
		if _, err := e.Create(ctx, spec, "v1", i); err != nil {
			t.Fatalf("create replica %d: %v", i, err)
		}
	}

	list, err := e.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	ports := map[int]bool{}
	idxs := map[int]bool{}
	count := 0
	for _, c := range list {
		if c.WorkloadID != id {
			continue
		}
		count++
		if c.PublishedPort <= 0 {
			t.Errorf("replica %d has no published host port", c.ReplicaIndex)
		}
		ports[c.PublishedPort] = true
		idxs[c.ReplicaIndex] = true
	}
	if count != 3 {
		t.Fatalf("want 3 replica containers, got %d", count)
	}
	if len(ports) != 3 {
		t.Errorf("want 3 distinct host ports, got %d (%v)", len(ports), ports)
	}
	if !idxs[0] || !idxs[1] || !idxs[2] {
		t.Errorf("want replica indices 0,1,2, got %v", idxs)
	}
}
