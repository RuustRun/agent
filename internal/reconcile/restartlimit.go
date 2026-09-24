// Crash-loop restart limiting. A cracked Egg is restarted in place on every converge
// (see needsRestart), which is right for a transient fault but wrong for an Egg that can
// never come up: it bounces forever, and each bounce takes its neighbours' shared resources
// (and, for a database Egg, risks an unclean shutdown every cycle). So we cap the number of
// crash-restarts and then leave the Egg stopped (cracked) until an operator or a fresh
// deploy intervenes. A redeploy rolls the workload to a new container id, which the limiter
// has never seen, so the new build gets a full budget: the cap never wedges a good deploy.
//
// The limiter is stateful and lives on the agent (one per process). It is deliberately kept
// out of the pure Diff so that stays trivially testable; ConvergeGoverned threads it in.
// British English. No em dashes.
package reconcile

import (
	"context"
	"fmt"
	"sync"

	"github.com/RuustRun/agent/internal/contract"
	"github.com/RuustRun/agent/internal/docker"
)

// DefaultMaxCrashRestarts is how many times a crashed container is restarted before the
// limiter gives up and leaves it stopped. Ten covers a genuinely transient fault (a
// dependency that was briefly unreachable) whilst bounding a hard crash-loop to a couple of
// minutes at the ~10-15s poll cadence.
const DefaultMaxCrashRestarts = 10

// RestartLimiter decides whether a crashed container may be restarted again. Observe is
// called once per converge with the full container list so the limiter can reset a
// recovered container's budget and forget ones that no longer exist; AllowRestart is called
// for each in-place restart the plan wants to perform and counts the attempt.
type RestartLimiter interface {
	Observe(actual []docker.Container)
	AllowRestart(c docker.Container) bool
}

// CrashLoopLimiter is the default RestartLimiter: an in-memory per-container attempt count
// with a hard ceiling. Safe for concurrent use (the shell and egress goroutines never touch
// it, but the interface is small enough to keep honest). A container that reaches the ceiling
// is "given up on" and stays so until Observe sees it healthy (recovered) or gone (rolled).
type CrashLoopLimiter struct {
	max int

	mu       sync.Mutex
	attempts map[string]int  // containerID -> crash-restarts issued this episode
	limited  map[string]bool // containerID -> ceiling reached, now left stopped
	// newlyLimited accumulates containers that JUST hit the ceiling, so the caller can log
	// the give-up exactly once. Drained by TakeNewlyLimited.
	newlyLimited []docker.Container
}

// NewCrashLoopLimiter returns a limiter capping crash-restarts at max. A max <= 0 disables
// the cap (AllowRestart always allows), which keeps the old unbounded behaviour available.
func NewCrashLoopLimiter(max int) *CrashLoopLimiter {
	return &CrashLoopLimiter{
		max:      max,
		attempts: map[string]int{},
		limited:  map[string]bool{},
	}
}

// Observe resets the budget of any container that has recovered (running and healthy) and
// forgets any container that no longer exists, so the map cannot grow without bound and a
// redeployed or hand-restarted Egg starts fresh.
func (l *CrashLoopLimiter) Observe(actual []docker.Container) {
	l.mu.Lock()
	defer l.mu.Unlock()
	present := make(map[string]bool, len(actual))
	for _, c := range actual {
		present[c.ID] = true
		if c.State == contract.StateRunning && c.Healthy {
			delete(l.attempts, c.ID)
			delete(l.limited, c.ID)
		}
	}
	for id := range l.attempts {
		if !present[id] {
			delete(l.attempts, id)
		}
	}
	for id := range l.limited {
		if !present[id] {
			delete(l.limited, id)
		}
	}
}

// AllowRestart reports whether this crashed container may be restarted again, counting the
// attempt. Once the ceiling is passed it returns false for good (until Observe clears the
// container), and records the container the first time so the give-up can be logged once.
func (l *CrashLoopLimiter) AllowRestart(c docker.Container) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.max <= 0 {
		return true // cap disabled
	}
	if l.limited[c.ID] {
		return false
	}
	l.attempts[c.ID]++
	if l.attempts[c.ID] > l.max {
		l.limited[c.ID] = true
		l.newlyLimited = append(l.newlyLimited, c)
		return false
	}
	return true
}

// Attempts is the number of crash-restarts counted for a container this episode, for the
// status report. Zero for a container the limiter has never restarted.
func (l *CrashLoopLimiter) Attempts(containerID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.attempts[containerID]
}

// Limited reports whether the limiter has given up on a container (ceiling reached), for the
// status report so the control plane can tell the customer the Egg is stopped, not looping.
func (l *CrashLoopLimiter) Limited(containerID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limited[containerID]
}

// TakeNewlyLimited returns and clears the containers that have hit the ceiling since the last
// call, so the caller can log each give-up exactly once.
func (l *CrashLoopLimiter) TakeNewlyLimited() []docker.Container {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.newlyLimited
	l.newlyLimited = nil
	return out
}

// ConvergeGoverned is Converge with a crash-loop limiter. It lists actual state, computes the
// plan, drops any in-place restart the limiter refuses (leaving that container stopped), and
// applies the rest. A nil limiter is exactly Converge (no cap).
func ConvergeGoverned(ctx context.Context, cli docker.Client, desired contract.DesiredState, lim RestartLimiter) (Plan, error) {
	actual, err := cli.List(ctx)
	if err != nil {
		return Plan{}, fmt.Errorf("listing actual state: %w", err)
	}
	plan := Diff(desired, actual)
	if lim != nil {
		lim.Observe(actual)
		plan = limitRestarts(plan, actual, lim)
	}
	return plan, Apply(ctx, cli, desired, plan)
}

// limitRestarts drops ActionRestart steps the limiter refuses. The refused container keeps
// whatever state it is in (crashed/stopped), so nothing is done to it: it stays down.
func limitRestarts(plan Plan, actual []docker.Container, lim RestartLimiter) Plan {
	byID := make(map[string]docker.Container, len(actual))
	for _, c := range actual {
		byID[c.ID] = c
	}
	out := Plan{Steps: make([]Step, 0, len(plan.Steps))}
	for _, step := range plan.Steps {
		if step.Action == ActionRestart {
			if c, ok := byID[step.ContainerID]; ok && !lim.AllowRestart(c) {
				continue // ceiling reached: leave the Egg stopped
			}
		}
		out.Steps = append(out.Steps, step)
	}
	return out
}
