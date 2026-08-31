package reconcile

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/RuustRun/agent/internal/contract"
	"github.com/RuustRun/agent/internal/docker"
)

// fakeClient is an in-memory docker.Client for unit testing the reconcile logic.
// It records calls so tests can assert idempotent, correct convergence without a
// real Docker daemon. Containers are keyed by ID, so a single replica slot can hold
// an old and a new container at once (as it does during a zero-downtime roll).
type fakeClient struct {
	containers map[string]docker.Container // keyed by container ID
	created    []string
	stopped    []string
	drained    []string
	restarted  []string
}

func newFakeClient(initial ...docker.Container) *fakeClient {
	f := &fakeClient{containers: map[string]docker.Container{}}
	for _, c := range initial {
		f.containers[c.ID] = c
	}
	return f
}

func (f *fakeClient) hasWorkload(workloadID string) bool {
	for _, c := range f.containers {
		if c.WorkloadID == workloadID {
			return true
		}
	}
	return false
}

func (f *fakeClient) List(_ context.Context) ([]docker.Container, error) {
	out := make([]docker.Container, 0, len(f.containers))
	for _, c := range f.containers {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].WorkloadID != out[j].WorkloadID {
			return out[i].WorkloadID < out[j].WorkloadID
		}
		if out[i].ReplicaIndex != out[j].ReplicaIndex {
			return out[i].ReplicaIndex < out[j].ReplicaIndex
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (f *fakeClient) EnsureImage(_ context.Context, _ string) error { return nil }

func (f *fakeClient) Create(_ context.Context, spec contract.WorkloadSpec, version string, replica int) (docker.Container, error) {
	c := docker.Container{
		ID:           fmt.Sprintf("c-%s-%d-%s", spec.ID, replica, version),
		WorkloadID:   spec.ID,
		BlobID:       spec.BlobID,
		EggID:        spec.EggID,
		ImageRef:     spec.ImageRef,
		SpecVersion:  version,
		State:        contract.StateRunning,
		Healthy:      true,
		ReplicaIndex: replica,
	}
	f.containers[c.ID] = c
	f.created = append(f.created, c.ID)
	return c, nil
}

func (f *fakeClient) Stop(_ context.Context, id string) error {
	delete(f.containers, id)
	f.stopped = append(f.stopped, id)
	return nil
}

func (f *fakeClient) Drain(_ context.Context, id string) error {
	delete(f.containers, id)
	f.drained = append(f.drained, id)
	return nil
}

func (f *fakeClient) Restart(_ context.Context, id string) error {
	f.restarted = append(f.restarted, id)
	return nil
}

func (f *fakeClient) Stats(_ context.Context, _ string) (contract.CgroupUsage, error) {
	return contract.CgroupUsage{}, nil
}

func (f *fakeClient) Logs(_ context.Context, _ string, _ string) ([]contract.LogLine, error) {
	return nil, nil
}

func (f *fakeClient) Close() error { return nil }

// spec is a small helper to build a WorkloadSpec with sane hard limits.
func spec(id string, replicas int, image string) contract.WorkloadSpec {
	return contract.WorkloadSpec{
		ID:              id,
		EggID:           "egg-" + id,
		BlobID:          "blob-" + id,
		ImageRef:        image,
		Replicas:        replicas,
		Port:            8080,
		HealthCheckPath: "/",
		Limits: contract.ResourceLimits{
			MemoryMb:       256,
			CpuFloor:       0.25,
			CpuBurst:       1.0,
			PidsLimit:      256,
			ReadOnlyRootfs: true,
			DroppedCaps:    []string{"ALL"},
		},
	}
}

// running builds a healthy running container at replica slot 0.
func running(workloadID, image, version string) docker.Container {
	return docker.Container{
		ID:          "c-" + workloadID,
		WorkloadID:  workloadID,
		BlobID:      "blob-" + workloadID,
		EggID:       "egg-" + workloadID,
		ImageRef:    image,
		SpecVersion: version,
		State:       contract.StateRunning,
		Healthy:     true,
	}
}

// runningReplica builds a healthy running container at a specific replica slot.
func runningReplica(workloadID, image, version string, replica int) docker.Container {
	c := running(workloadID, image, version)
	c.ID = fmt.Sprintf("c-%s-%d", workloadID, replica)
	c.ReplicaIndex = replica
	return c
}

// runningVersion builds a healthy running container at a slot with the version in
// its ID, so an old and a new container for the same slot have distinct IDs.
func runningVersion(workloadID, image, version string, replica int) docker.Container {
	c := runningReplica(workloadID, image, version, replica)
	c.ID = fmt.Sprintf("c-%s-%d-%s", workloadID, replica, version)
	return c
}

func countActions(plan Plan, action Action) int {
	n := 0
	for _, s := range plan.Steps {
		if s.Action == action {
			n++
		}
	}
	return n
}

// TestStatefulRollStopsBeforeStart proves a volume-bearing (database) Egg rolls
// stop-old-before-start-new, so two engines never share one data dir, whilst a
// stateless Egg still rolls zero-downtime (start-new-before-stop-old).
func TestStatefulRollStopsBeforeStart(t *testing.T) {
	const oldV, newV = "v-old", "v-new"

	// A stateful workload (has a persistent volume) with a stale container in slot 0.
	dbSpec := spec("db", 1, "postgres:16")
	dbSpec.Volumes = []contract.VolumeMount{{Name: "ruust-vol-blob-db", Path: "/var/lib/postgresql/data"}}
	staleDB := runningVersion("db", "postgres:16", oldV, 0)

	desired := contract.DesiredState{Version: newV, Workloads: []contract.WorkloadSpec{dbSpec}}
	plan := Diff(desired, []docker.Container{staleDB})
	if got := countActions(plan, ActionStart); got != 0 {
		t.Errorf("stateful roll must not start a new container alongside the old one, got %d starts", got)
	}
	if got := countActions(plan, ActionStop); got != 1 {
		t.Errorf("stateful roll must stop the old container first, got %d stops", got)
	}

	// Once the slot is empty, the fresh container starts.
	if got := countActions(Diff(desired, nil), ActionStart); got != 1 {
		t.Errorf("stateful workload with an empty slot should start once, got %d", got)
	}

	// Contrast: a stateless workload rolls zero-downtime (start new, keep old serving).
	webSpec := spec("web", 1, "nginx:2")
	staleWeb := runningVersion("web", "nginx:1", oldV, 0)
	webPlan := Diff(contract.DesiredState{Version: newV, Workloads: []contract.WorkloadSpec{webSpec}}, []docker.Container{staleWeb})
	if got := countActions(webPlan, ActionStart); got != 1 {
		t.Errorf("stateless roll should start the new container first, got %d", got)
	}
	if got := countActions(webPlan, ActionStop) + countActions(webPlan, ActionRoll); got != 0 {
		t.Errorf("stateless roll must not drain the old container before the new is healthy, got %d", got)
	}
}

func TestDiff(t *testing.T) {
	const version = "v-abc"

	crashed := running("crash", "nginx:latest", version)
	crashed.State = contract.StateCrashed
	crashed.Healthy = false

	unhealthy := running("sick", "nginx:latest", version)
	unhealthy.Healthy = false

	cases := []struct {
		name        string
		desired     contract.DesiredState
		actual      []docker.Container
		wantStart   int
		wantStop    int
		wantRestart int
		wantRoll    int
	}{
		{
			name: "start missing",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("a", 1, "nginx:latest"),
			}},
			actual:    nil,
			wantStart: 1,
		},
		{
			name: "steady state is a no-op",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("a", 1, "nginx:latest"),
			}},
			actual: []docker.Container{running("a", "nginx:latest", version)},
		},
		{
			name:     "stop extra",
			desired:  contract.DesiredState{HostID: "h1", Version: version},
			actual:   []docker.Container{running("orphan", "nginx:latest", version)},
			wantStop: 1,
		},
		{
			name: "restart crashed (cracked Egg comes back)",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("crash", 1, "nginx:latest"),
			}},
			actual:      []docker.Container{crashed},
			wantRestart: 1,
		},
		{
			name: "restart unhealthy",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("sick", 1, "nginx:latest"),
			}},
			actual:      []docker.Container{unhealthy},
			wantRestart: 1,
		},
		{
			// Zero-downtime: an image change starts the NEW container and leaves the
			// old one running (it is drained later, once the new one is healthy), so
			// this single plan is a start with no stop.
			name: "image change starts the replacement, old kept serving",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("a", 1, "nginx:1.27"),
			}},
			actual:    []docker.Container{running("a", "nginx:1.25", version)},
			wantStart: 1,
			wantRoll:  0,
		},
		{
			name: "version change starts the replacement, old kept serving",
			desired: contract.DesiredState{HostID: "h1", Version: "v-new", Workloads: []contract.WorkloadSpec{
				spec("a", 1, "nginx:latest"),
			}},
			actual:    []docker.Container{running("a", "nginx:latest", "v-old")},
			wantStart: 1,
			wantRoll:  0,
		},
		{
			name: "replicas zero stops the container (Egg goes cold)",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("a", 0, "nginx:latest"),
			}},
			actual:   []docker.Container{running("a", "nginx:latest", version)},
			wantStop: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := Diff(tc.desired, tc.actual)
			if got := countActions(plan, ActionStart); got != tc.wantStart {
				t.Errorf("start count = %d, want %d (plan: %+v)", got, tc.wantStart, plan.Steps)
			}
			if got := countActions(plan, ActionStop); got != tc.wantStop {
				t.Errorf("stop count = %d, want %d (plan: %+v)", got, tc.wantStop, plan.Steps)
			}
			if got := countActions(plan, ActionRestart); got != tc.wantRestart {
				t.Errorf("restart count = %d, want %d (plan: %+v)", got, tc.wantRestart, plan.Steps)
			}
			if got := countActions(plan, ActionRoll); got != tc.wantRoll {
				t.Errorf("roll count = %d, want %d (plan: %+v)", got, tc.wantRoll, plan.Steps)
			}
		})
	}
}

// TestReplicaScaling covers the horizontal-scaling paths of Diff: scale up starts
// the missing replica slots, scale down stops the surplus, a steady multi-replica
// set is a no-op, and a version bump starts a replacement in every slot (leaving
// the old ones serving until the new ones are healthy).
func TestReplicaScaling(t *testing.T) {
	const version = "v-1"

	legacy := running("a", "nginx:latest", version)
	legacy.Legacy = true // pre-replica-index container

	cases := []struct {
		name        string
		desired     contract.DesiredState
		actual      []docker.Container
		wantStart   int
		wantStop    int
		wantRestart int
		wantRoll    int
	}{
		{
			name: "scale up from one to three starts two",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("a", 3, "nginx:latest"),
			}},
			actual:    []docker.Container{runningReplica("a", "nginx:latest", version, 0)},
			wantStart: 2,
		},
		{
			name: "steady three-replica set is a no-op",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("a", 3, "nginx:latest"),
			}},
			actual: []docker.Container{
				runningReplica("a", "nginx:latest", version, 0),
				runningReplica("a", "nginx:latest", version, 1),
				runningReplica("a", "nginx:latest", version, 2),
			},
		},
		{
			name: "scale down from three to one stops two",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("a", 1, "nginx:latest"),
			}},
			actual: []docker.Container{
				runningReplica("a", "nginx:latest", version, 0),
				runningReplica("a", "nginx:latest", version, 1),
				runningReplica("a", "nginx:latest", version, 2),
			},
			wantStop: 2,
		},
		{
			// A legacy (unindexed) container is treated as old: the indexed
			// replacement is started beside it and it keeps serving until then.
			name: "legacy container gets an indexed replacement started beside it",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("a", 1, "nginx:latest"),
			}},
			actual:    []docker.Container{legacy},
			wantStart: 1,
			wantStop:  0,
			wantRoll:  0,
		},
		{
			name: "version bump starts a replacement in every slot",
			desired: contract.DesiredState{HostID: "h1", Version: "v-2", Workloads: []contract.WorkloadSpec{
				spec("a", 2, "nginx:latest"),
			}},
			actual: []docker.Container{
				runningReplica("a", "nginx:latest", "v-1", 0),
				runningReplica("a", "nginx:latest", "v-1", 1),
			},
			wantStart: 2,
			wantRoll:  0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := Diff(tc.desired, tc.actual)
			if got := countActions(plan, ActionStart); got != tc.wantStart {
				t.Errorf("start count = %d, want %d (plan: %+v)", got, tc.wantStart, plan.Steps)
			}
			if got := countActions(plan, ActionStop); got != tc.wantStop {
				t.Errorf("stop count = %d, want %d (plan: %+v)", got, tc.wantStop, plan.Steps)
			}
			if got := countActions(plan, ActionRestart); got != tc.wantRestart {
				t.Errorf("restart count = %d, want %d (plan: %+v)", got, tc.wantRestart, plan.Steps)
			}
			if got := countActions(plan, ActionRoll); got != tc.wantRoll {
				t.Errorf("roll count = %d, want %d (plan: %+v)", got, tc.wantRoll, plan.Steps)
			}
		})
	}
}

// TestZeroDowntimeRoll covers the two phases of a rolling deploy explicitly: while
// the new container is booting the old one is untouched, and only once the new one
// is healthy is the old one drained. Never both at once.
func TestZeroDowntimeRoll(t *testing.T) {
	const oldV, newV = "v-old", "v-new"
	w := spec("a", 1, "nginx:latest")

	oldHealthy := runningVersion("a", "nginx:latest", oldV, 0)

	newBooting := runningVersion("a", "nginx:latest", newV, 0)
	newBooting.State = contract.StateStarting
	newBooting.Healthy = false

	newHealthy := runningVersion("a", "nginx:latest", newV, 0)

	desired := contract.DesiredState{HostID: "h1", Version: newV, Workloads: []contract.WorkloadSpec{w}}

	t.Run("phase 1: new is booting, old is left serving", func(t *testing.T) {
		plan := Diff(desired, []docker.Container{oldHealthy, newBooting})
		if got := countActions(plan, ActionRoll); got != 0 {
			t.Errorf("must not drain the old container whilst the new one boots (plan: %+v)", plan.Steps)
		}
		if got := countActions(plan, ActionStop); got != 0 {
			t.Errorf("must not stop anything in phase 1 (plan: %+v)", plan.Steps)
		}
		if got := countActions(plan, ActionStart); got != 0 {
			t.Errorf("the new container already exists, so no start (plan: %+v)", plan.Steps)
		}
	})

	t.Run("phase 2: new is healthy, old is drained", func(t *testing.T) {
		plan := Diff(desired, []docker.Container{oldHealthy, newHealthy})
		if got := countActions(plan, ActionRoll); got != 1 {
			t.Errorf("the old container should be drained once the new one is healthy (plan: %+v)", plan.Steps)
		}
		// The drain must target the OLD container, never the new one.
		for _, s := range plan.Steps {
			if s.Action == ActionRoll && s.ContainerID != oldHealthy.ID {
				t.Errorf("drained the wrong container: %s (want %s)", s.ContainerID, oldHealthy.ID)
			}
		}
	})
}

// TestConvergeIsIdempotent proves that converging twice against an unchanged
// desired state does no work the second time.
func TestConvergeIsIdempotent(t *testing.T) {
	const version = "v-1"
	desired := contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
		spec("a", 1, "nginx:latest"),
		spec("b", 1, "redis:7"),
	}}
	fake := newFakeClient()

	if _, err := Converge(context.Background(), fake, desired); err != nil {
		t.Fatalf("first converge: %v", err)
	}
	if len(fake.created) != 2 {
		t.Fatalf("first converge created %d, want 2", len(fake.created))
	}

	before := len(fake.created)
	if _, err := Converge(context.Background(), fake, desired); err != nil {
		t.Fatalf("second converge: %v", err)
	}
	if len(fake.created) != before {
		t.Errorf("second converge created %d more containers, want 0", len(fake.created)-before)
	}
}

// TestConvergeStartsAndStops proves an end-to-end converge starts what is
// missing and stops what is extra.
func TestConvergeStartsAndStops(t *testing.T) {
	const version = "v-2"
	desired := contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
		spec("keep", 1, "nginx:latest"),
	}}
	fake := newFakeClient(
		running("keep", "nginx:latest", version),
		running("gone", "nginx:latest", version),
	)

	plan, err := Converge(context.Background(), fake, desired)
	if err != nil {
		t.Fatalf("converge: %v", err)
	}
	if len(fake.stopped) != 1 {
		t.Errorf("stopped %d, want 1 (plan: %+v)", len(fake.stopped), plan.Steps)
	}
	if fake.hasWorkload("gone") {
		t.Errorf("extra container was not stopped")
	}
	if !fake.hasWorkload("keep") {
		t.Errorf("kept container was removed")
	}
}

// TestConvergeScalesUp proves an end-to-end converge starts all replica slots and
// is then idempotent at the new replica count.
func TestConvergeScalesUp(t *testing.T) {
	const version = "v-1"
	desired := contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
		spec("web", 3, "nginx:latest"),
	}}
	fake := newFakeClient()

	if _, err := Converge(context.Background(), fake, desired); err != nil {
		t.Fatalf("first converge: %v", err)
	}
	if len(fake.created) != 3 {
		t.Fatalf("first converge created %d replicas, want 3", len(fake.created))
	}
	before := len(fake.created)
	if _, err := Converge(context.Background(), fake, desired); err != nil {
		t.Fatalf("second converge: %v", err)
	}
	if len(fake.created) != before {
		t.Errorf("second converge created %d more, want 0 (idempotent)", len(fake.created)-before)
	}
}

// TestConvergeRollIsZeroDowntime drives a full rolling deploy across two ticks and
// asserts the old container is only removed after the new one exists and is healthy.
func TestConvergeRollIsZeroDowntime(t *testing.T) {
	const oldV, newV = "v-old", "v-new"
	desired := contract.DesiredState{HostID: "h1", Version: newV, Workloads: []contract.WorkloadSpec{
		spec("web", 1, "nginx:latest"),
	}}
	fake := newFakeClient(runningVersion("web", "nginx:latest", oldV, 0))

	// Tick 1: start the new container, leave the old one running.
	if _, err := Converge(context.Background(), fake, desired); err != nil {
		t.Fatalf("tick 1 converge: %v", err)
	}
	if len(fake.created) != 1 {
		t.Errorf("tick 1 should start the new container, created %d", len(fake.created))
	}
	if len(fake.drained) != 0 || len(fake.stopped) != 0 {
		t.Errorf("tick 1 must not remove the old container yet (drained %d, stopped %d)", len(fake.drained), len(fake.stopped))
	}
	if len(fake.containers) != 2 {
		t.Errorf("tick 1 should leave old and new running side by side, have %d", len(fake.containers))
	}

	// Tick 2: the new container (created healthy by the fake) is serving, so the
	// old one is drained.
	if _, err := Converge(context.Background(), fake, desired); err != nil {
		t.Fatalf("tick 2 converge: %v", err)
	}
	if len(fake.drained) != 1 {
		t.Errorf("tick 2 should drain the old container, drained %d", len(fake.drained))
	}
	if len(fake.containers) != 1 {
		t.Errorf("after the roll exactly one container should remain, have %d", len(fake.containers))
	}
	for _, c := range fake.containers {
		if c.SpecVersion != newV {
			t.Errorf("the surviving container should be the new version, got %s", c.SpecVersion)
		}
	}
}
