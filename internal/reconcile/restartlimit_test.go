package reconcile

import (
	"testing"
	"time"

	"github.com/RuustRun/agent/internal/contract"
	"github.com/RuustRun/agent/internal/docker"
)

func crashed(id string) docker.Container {
	return docker.Container{ID: id, WorkloadID: "w-" + id, BlobID: "b-" + id, State: contract.StateCrashed}
}

func TestCrashLoopLimiterCapsThenLeavesStopped(t *testing.T) {
	lim := NewCrashLoopLimiter(3, 0)
	c := crashed("abc")
	for i := 1; i <= 3; i++ {
		lim.Observe([]docker.Container{c})
		if !lim.AllowRestart(c) {
			t.Fatalf("restart attempt %d should be allowed under a cap of 3", i)
		}
	}
	// The next attempt trips the cap and every attempt after it is refused.
	lim.Observe([]docker.Container{c})
	if lim.AllowRestart(c) {
		t.Fatal("attempt beyond the cap must be refused")
	}
	if lim.AllowRestart(c) {
		t.Fatal("a container past the cap stays refused")
	}
	if !lim.Limited(c.ID) {
		t.Fatal("container should be marked restart-limited")
	}
	nl := lim.TakeNewlyLimited()
	if len(nl) != 1 || nl[0].ID != "abc" {
		t.Fatalf("newly-limited = %v, want one entry for abc", nl)
	}
	if got := lim.TakeNewlyLimited(); len(got) != 0 {
		t.Fatal("TakeNewlyLimited must drain, so a give-up is logged exactly once")
	}
}

func TestCrashLoopLimiterResetsWhenRecovered(t *testing.T) {
	lim := NewCrashLoopLimiter(2, 0)
	c := crashed("abc")
	lim.Observe([]docker.Container{c})
	lim.AllowRestart(c)
	lim.Observe([]docker.Container{c})
	lim.AllowRestart(c)

	healthy := docker.Container{ID: "abc", WorkloadID: "w-abc", State: contract.StateRunning, Healthy: true}
	lim.Observe([]docker.Container{healthy})
	if lim.Attempts("abc") != 0 || lim.Limited("abc") {
		t.Fatal("a recovered container must have its restart budget cleared")
	}
	// Fresh budget after recovery.
	lim.Observe([]docker.Container{c})
	if !lim.AllowRestart(c) {
		t.Fatal("after recovery a later crash should be restarted again")
	}
}

func TestCrashLoopLimiterForgetsGoneContainers(t *testing.T) {
	lim := NewCrashLoopLimiter(2, 0)
	c := crashed("abc")
	lim.Observe([]docker.Container{c})
	lim.AllowRestart(c)
	lim.Observe(nil) // container removed (rolled/redeployed)
	if lim.Attempts("abc") != 0 {
		t.Fatal("a container that no longer exists must be forgotten")
	}
}

func TestCrashLoopLimiterDisabled(t *testing.T) {
	lim := NewCrashLoopLimiter(0, 0) // 0 disables the cap
	c := crashed("abc")
	for i := 0; i < 50; i++ {
		if !lim.AllowRestart(c) {
			t.Fatal("a disabled cap must always allow a restart")
		}
	}
}

func TestLimitRestartsDropsCappedSteps(t *testing.T) {
	lim := NewCrashLoopLimiter(1, 0)
	c := crashed("abc")
	actual := []docker.Container{c}
	plan := Plan{Steps: []Step{{Action: ActionRestart, WorkloadID: "w-abc", ContainerID: "abc"}}}

	lim.Observe(actual)
	if got := limitRestarts(plan, actual, lim); len(got.Steps) != 1 {
		t.Fatalf("first restart should pass the cap, got %d steps", len(got.Steps))
	}
	lim.Observe(actual)
	if got := limitRestarts(plan, actual, lim); len(got.Steps) != 0 {
		t.Fatalf("restart beyond the cap should be dropped, got %d steps", len(got.Steps))
	}
}

func TestLimitRestartsLeavesOtherActions(t *testing.T) {
	lim := NewCrashLoopLimiter(0, 0) // even disabled, non-restart steps pass through untouched
	actual := []docker.Container{crashed("abc")}
	plan := Plan{Steps: []Step{
		{Action: ActionStop, WorkloadID: "w1", ContainerID: "x"},
		{Action: ActionRoll, WorkloadID: "w2", ContainerID: "y"},
		{Action: ActionStart, WorkloadID: "w3"},
	}}
	got := limitRestarts(plan, actual, lim)
	if len(got.Steps) != 3 {
		t.Fatalf("non-restart steps must be preserved, got %d", len(got.Steps))
	}
}

func TestCrashLoopLimiterRetriesOncePerCooldown(t *testing.T) {
	lim := NewCrashLoopLimiter(2, 5*time.Minute)
	clock := time.Date(2026, 11, 23, 13, 50, 0, 0, time.UTC)
	lim.now = func() time.Time { return clock }
	c := crashed("abc")
	lim.AllowRestart(c)
	lim.AllowRestart(c)
	if lim.AllowRestart(c) {
		t.Fatal("attempt beyond the cap must be refused")
	}
	clock = clock.Add(4 * time.Minute)
	if lim.AllowRestart(c) {
		t.Fatal("a limited container must not be retried before its cooldown")
	}
	clock = clock.Add(time.Minute)
	if !lim.AllowRestart(c) {
		t.Fatal("a limited container must be retried once its cooldown has passed")
	}
	if lim.AllowRestart(c) {
		t.Fatal("only one retry per cooldown")
	}
	if !lim.Limited(c.ID) {
		t.Fatal("a slow-cadence container is still reported as restart-limited")
	}
	// It comes up (its database is back): the budget resets in full.
	lim.Observe([]docker.Container{{ID: "abc", State: contract.StateRunning, Healthy: true}})
	if lim.Limited(c.ID) || lim.Attempts(c.ID) != 0 {
		t.Fatal("a recovered container must leave the slow cadence")
	}
}
