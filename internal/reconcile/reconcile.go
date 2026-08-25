// Package reconcile holds the diff and converge logic at the heart of the
// agent. It compares the desired state (from the control plane) against the
// actual state (containers on the host carrying our label prefix) and converges:
// start missing, stop extra, restart unhealthy or stale. Every operation is
// idempotent and safely retryable, so a converge that is interrupted and re-run
// reaches the same end state.
//
// The logic here depends only on the docker.Client interface, so it is fully
// unit testable with a fake client (see reconcile_test.go).
package reconcile

import (
	"context"
	"fmt"

	"github.com/RuustRun/agent/internal/contract"
	"github.com/RuustRun/agent/internal/docker"
)

// Action names a single convergence step, for structured logging.
type Action string

const (
	// ActionStart starts a missing container for a desired workload.
	ActionStart Action = "start"
	// ActionStop stops and removes a container with no matching desired workload.
	ActionStop Action = "stop"
	// ActionRestart restarts an unhealthy or crashed container.
	ActionRestart Action = "restart"
	// ActionRoll stops a stale container (wrong image or spec version) and starts
	// a fresh one.
	ActionRoll Action = "roll"
)

// Step is one planned or performed convergence action, kept for logging and for
// assertion in tests.
type Step struct {
	Action     Action
	WorkloadID string
	// BlobID is the internal Blob identity of the Egg the step concerns, if
	// known. Never rendered to customers.
	BlobID string
	// ContainerID is the target container, where one exists.
	ContainerID string
}

// Plan is the ordered set of steps computed by Diff. Stops run before starts so
// a roll frees resources before claiming them again.
type Plan struct {
	Steps []Step
}

// Diff computes the convergence plan for a desired state against the observed
// containers. It is pure: it performs no I/O and mutates nothing, which is what
// makes it unit testable.
//
// Rules:
//   - A desired workload with replicas >= 1 and no healthy up-to-date container
//     is started (or rolled if a stale one exists).
//   - A desired workload with replicas == 0 has any container stopped (the Egg
//     goes cold on purpose).
//   - A container with no matching desired workload is stopped (extra).
//   - A container whose image or spec version differs from desired is rolled.
//   - A crashed or unhealthy container for a still-desired workload is restarted.
func Diff(desired contract.DesiredState, actual []docker.Container) Plan {
	var plan Plan

	// Index actual containers by workload id. In phase 1 replicas is effectively
	// one container per workload; we take the first match and treat any surplus as
	// extra so the invariant stays simple and idempotent.
	byWorkload := make(map[string]docker.Container, len(actual))
	seen := make(map[string]bool, len(actual))
	for _, c := range actual {
		if _, ok := byWorkload[c.WorkloadID]; ok {
			// Surplus container for an already-matched workload: stop it.
			plan.Steps = append(plan.Steps, Step{
				Action:      ActionStop,
				WorkloadID:  c.WorkloadID,
				BlobID:      c.BlobID,
				ContainerID: c.ID,
			})
			continue
		}
		byWorkload[c.WorkloadID] = c
	}

	desiredByID := make(map[string]contract.WorkloadSpec, len(desired.Workloads))

	// First pass: stops and rolls (free resources before claiming them).
	for _, w := range desired.Workloads {
		desiredByID[w.ID] = w
		c, running := byWorkload[w.ID]
		if !running {
			continue
		}
		seen[w.ID] = true

		if w.Replicas == 0 {
			// Desired cold: stop any running container.
			plan.Steps = append(plan.Steps, Step{
				Action:      ActionStop,
				WorkloadID:  w.ID,
				BlobID:      w.BlobID,
				ContainerID: c.ID,
			})
			continue
		}

		if isStale(w, c, desired.Version) {
			plan.Steps = append(plan.Steps, Step{
				Action:      ActionRoll,
				WorkloadID:  w.ID,
				BlobID:      w.BlobID,
				ContainerID: c.ID,
			})
		}
	}

	// Extra containers: any observed workload not in desired state at all.
	for _, c := range actual {
		if _, wanted := desiredByID[c.WorkloadID]; !wanted {
			plan.Steps = append(plan.Steps, Step{
				Action:      ActionStop,
				WorkloadID:  c.WorkloadID,
				BlobID:      c.BlobID,
				ContainerID: c.ID,
			})
		}
	}

	// Second pass: starts and restarts.
	for _, w := range desired.Workloads {
		if w.Replicas == 0 {
			continue
		}
		c, running := byWorkload[w.ID]
		switch {
		case !running:
			// Missing: start it.
			plan.Steps = append(plan.Steps, Step{
				Action:     ActionStart,
				WorkloadID: w.ID,
				BlobID:     w.BlobID,
			})
		case isStale(w, c, desired.Version):
			// Already scheduled a roll (stop) above; start the replacement.
			plan.Steps = append(plan.Steps, Step{
				Action:     ActionStart,
				WorkloadID: w.ID,
				BlobID:     w.BlobID,
			})
		case needsRestart(c):
			// Present but unhealthy or crashed: restart in place.
			plan.Steps = append(plan.Steps, Step{
				Action:      ActionRestart,
				WorkloadID:  w.ID,
				BlobID:      w.BlobID,
				ContainerID: c.ID,
			})
		}
	}

	return plan
}

// isStale reports whether an existing container no longer matches the desired
// spec and must be rolled: a changed image reference or a changed desired-state
// version both force a fresh container.
func isStale(w contract.WorkloadSpec, c docker.Container, version string) bool {
	if c.ImageRef != "" && c.ImageRef != w.ImageRef {
		return true
	}
	if c.SpecVersion != "" && c.SpecVersion != version {
		return true
	}
	return false
}

// needsRestart reports whether a still-desired container should be restarted in
// place because it has crashed (a cracked Egg) or is failing its health check.
func needsRestart(c docker.Container) bool {
	if c.State == contract.StateCrashed || c.State == contract.StateStopped {
		return true
	}
	if c.State == contract.StateRunning && !c.Healthy {
		return true
	}
	return false
}

// Apply executes a plan against the docker client. It is idempotent: a step
// whose effect is already in place is a cheap no-op, and a failure on one step
// does not abort the rest, so the next poll retries what did not land. The set
// of errors encountered is returned joined, so the caller can log and carry on
// rather than crash.
func Apply(ctx context.Context, cli docker.Client, desired contract.DesiredState, plan Plan) error {
	specByID := make(map[string]contract.WorkloadSpec, len(desired.Workloads))
	for _, w := range desired.Workloads {
		specByID[w.ID] = w
	}

	var errs []error
	for _, step := range plan.Steps {
		switch step.Action {
		case ActionStop, ActionRoll:
			// Roll's stop half and a plain stop are the same idempotent removal.
			// Roll's start half is emitted as a separate ActionStart step.
			if step.ContainerID != "" {
				if err := cli.Stop(ctx, step.ContainerID); err != nil {
					errs = append(errs, fmt.Errorf("stop %s: %w", step.WorkloadID, err))
				}
			}
		case ActionStart:
			spec, ok := specByID[step.WorkloadID]
			if !ok {
				continue
			}
			if _, err := cli.Create(ctx, spec, desired.Version); err != nil {
				errs = append(errs, fmt.Errorf("start %s: %w", step.WorkloadID, err))
			}
		case ActionRestart:
			if step.ContainerID != "" {
				if err := cli.Restart(ctx, step.ContainerID); err != nil {
					errs = append(errs, fmt.Errorf("restart %s: %w", step.WorkloadID, err))
				}
			}
		}
	}

	return joinErrors(errs)
}

// Converge is the full diff-then-apply cycle: list actual containers, compute
// the plan and apply it. It returns the plan (for logging) and any converge
// errors.
func Converge(ctx context.Context, cli docker.Client, desired contract.DesiredState) (Plan, error) {
	actual, err := cli.List(ctx)
	if err != nil {
		return Plan{}, fmt.Errorf("listing actual state: %w", err)
	}
	plan := Diff(desired, actual)
	return plan, Apply(ctx, cli, desired, plan)
}

// joinErrors combines a slice of errors into one, or nil if empty. Kept local
// so the package builds on Go 1.19 as well as newer toolchains with
// errors.Join.
func joinErrors(errs []error) error {
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		msg := "reconcile encountered errors:"
		for _, e := range errs {
			msg += "\n  - " + e.Error()
		}
		return fmt.Errorf("%s", msg)
	}
}
