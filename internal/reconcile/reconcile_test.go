package reconcile

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/RuustRun/agent/internal/contract"
	"github.com/RuustRun/agent/internal/docker"
)

// fakeClient is an in-memory docker.Client for unit testing the reconcile
// logic. It records calls so tests can assert idempotent, correct convergence
// without a real Docker daemon. Containers are keyed by (workload id, replica
// index), so a workload can hold several replicas.
type fakeClient struct {
	containers map[string]docker.Container // keyed by "<workloadId>#<replica>"
	created    []string
	stopped    []string
	restarted  []string
}

func replicaKey(workloadID string, replica int) string {
	return workloadID + "#" + strconv.Itoa(replica)
}

func newFakeClient(initial ...docker.Container) *fakeClient {
	f := &fakeClient{containers: map[string]docker.Container{}}
	for _, c := range initial {
		f.containers[replicaKey(c.WorkloadID, c.ReplicaIndex)] = c
	}
	return f
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
		return out[i].ReplicaIndex < out[j].ReplicaIndex
	})
	return out, nil
}

func (f *fakeClient) EnsureImage(_ context.Context, _ string) error { return nil }

func (f *fakeClient) Create(_ context.Context, spec contract.WorkloadSpec, version string, replica int) (docker.Container, error) {
	c := docker.Container{
		ID:           fmt.Sprintf("c-%s-%d", spec.ID, replica),
		WorkloadID:   spec.ID,
		BlobID:       spec.BlobID,
		EggID:        spec.EggID,
		ImageRef:     spec.ImageRef,
		SpecVersion:  version,
		State:        contract.StateRunning,
		Healthy:      true,
		ReplicaIndex: replica,
	}
	f.containers[replicaKey(spec.ID, replica)] = c
	f.created = append(f.created, c.ID)
	return c, nil
}

func (f *fakeClient) Stop(_ context.Context, id string) error {
	for key, c := range f.containers {
		if c.ID == id {
			delete(f.containers, key)
			break
		}
	}
	f.stopped = append(f.stopped, id)
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

func countActions(plan Plan, action Action) int {
	n := 0
	for _, s := range plan.Steps {
		if s.Action == action {
			n++
		}
	}
	return n
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
			name: "roll on image change",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("a", 1, "nginx:1.27"),
			}},
			actual:    []docker.Container{running("a", "nginx:1.25", version)},
			wantRoll:  1,
			wantStart: 1,
		},
		{
			name: "roll on version change",
			desired: contract.DesiredState{HostID: "h1", Version: "v-new", Workloads: []contract.WorkloadSpec{
				spec("a", 1, "nginx:latest"),
			}},
			actual:    []docker.Container{running("a", "nginx:latest", "v-old")},
			wantRoll:  1,
			wantStart: 1,
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
	if _, ok := fake.containers[replicaKey("gone", 0)]; ok {
		t.Errorf("extra container was not stopped")
	}
	if _, ok := fake.containers[replicaKey("keep", 0)]; !ok {
		t.Errorf("kept container was removed")
	}
}

// running1 builds a running container at a specific replica index, for the
// horizontal-scaling tests.
func runningReplica(workloadID, image, version string, replica int) docker.Container {
	c := running(workloadID, image, version)
	c.ID = fmt.Sprintf("c-%s-%d", workloadID, replica)
	c.ReplicaIndex = replica
	return c
}

// TestReplicaScaling covers the horizontal-scaling paths of Diff: scale up starts
// the missing replica slots, scale down stops the surplus, a steady multi-replica
// set is a no-op, a legacy (unindexed) container is always rolled, and a version
// bump rolls every replica.
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
			name: "legacy container is rolled onto the indexed name",
			desired: contract.DesiredState{HostID: "h1", Version: version, Workloads: []contract.WorkloadSpec{
				spec("a", 1, "nginx:latest"),
			}},
			actual:    []docker.Container{legacy},
			wantStop:  1, // legacy stopped
			wantStart: 1, // replica 0 started
		},
		{
			name: "version bump rolls every replica",
			desired: contract.DesiredState{HostID: "h1", Version: "v-2", Workloads: []contract.WorkloadSpec{
				spec("a", 2, "nginx:latest"),
			}},
			actual: []docker.Container{
				runningReplica("a", "nginx:latest", "v-1", 0),
				runningReplica("a", "nginx:latest", "v-1", 1),
			},
			wantRoll:  2,
			wantStart: 2,
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
