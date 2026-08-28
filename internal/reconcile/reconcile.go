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
	// ReplicaIndex is the 0-based replica slot this step concerns. ActionStart uses
	// it to name and label the new container; stop, roll and restart carry it for
	// logging and idempotency.
	ReplicaIndex int
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
// Rules, per desired workload with Replicas = N:
//   - Exactly N replica slots (indices 0..N-1) should each have one healthy,
//     up-to-date container. A missing slot is started; a stale one is rolled; an
//     unhealthy or crashed one is restarted in place.
//   - A container at an index >= N (a scale-down), a duplicate for a slot already
//     filled, or a legacy container from an agent that predates replica indexing,
//     is surplus and stopped.
//   - Replicas == 0 stops every container for the workload (the Egg goes cold on
//     purpose).
//   - A container with no matching desired workload is stopped (extra).
func Diff(desired contract.DesiredState, actual []docker.Container) Plan {
	var plan Plan

	desiredByID := make(map[string]contract.WorkloadSpec, len(desired.Workloads))
	for _, w := range desired.Workloads {
		desiredByID[w.ID] = w
	}

	// Group observed containers by workload id, preserving listing order so the
	// first container seen for a given replica slot is the one retained.
	byWorkload := make(map[string][]docker.Container, len(actual))
	for _, c := range actual {
		byWorkload[c.WorkloadID] = append(byWorkload[c.WorkloadID], c)
	}

	stop := func(c docker.Container, action Action) Step {
		return Step{Action: action, WorkloadID: c.WorkloadID, BlobID: c.BlobID, ContainerID: c.ID, ReplicaIndex: c.ReplicaIndex}
	}

	// First pass: stops and rolls (free resources before claiming them again).
	// keepers[w.ID][idx] is the single container retained for that replica slot.
	keepers := make(map[string]map[int]docker.Container, len(desired.Workloads))
	for _, w := range desired.Workloads {
		cs := byWorkload[w.ID]

		// Desired cold: stop every container for this workload.
		if w.Replicas <= 0 {
			for _, c := range cs {
				plan.Steps = append(plan.Steps, stop(c, ActionStop))
			}
			continue
		}

		keep := make(map[int]docker.Container, w.Replicas)
		for _, c := range cs {
			// Legacy (no replica label), an index beyond the desired count (scale
			// down), or a duplicate for an already-filled slot: all surplus, stop.
			if c.Legacy || c.ReplicaIndex < 0 || c.ReplicaIndex >= w.Replicas {
				plan.Steps = append(plan.Steps, stop(c, ActionStop))
				continue
			}
			if _, taken := keep[c.ReplicaIndex]; taken {
				plan.Steps = append(plan.Steps, stop(c, ActionStop))
				continue
			}
			keep[c.ReplicaIndex] = c
		}
		keepers[w.ID] = keep

		// Roll any kept-but-stale replica: stop it here, start the replacement below.
		for idx := 0; idx < w.Replicas; idx++ {
			if c, ok := keep[idx]; ok && isStale(w, c, desired.Version) {
				plan.Steps = append(plan.Steps, stop(c, ActionRoll))
			}
		}
	}

	// Extra containers: any observed workload not in desired state at all.
	for _, c := range actual {
		if _, wanted := desiredByID[c.WorkloadID]; !wanted {
			plan.Steps = append(plan.Steps, stop(c, ActionStop))
		}
	}

	// Second pass: starts and restarts, per replica slot.
	for _, w := range desired.Workloads {
		if w.Replicas <= 0 {
			continue
		}
		keep := keepers[w.ID]
		for idx := 0; idx < w.Replicas; idx++ {
			c, present := keep[idx]
			switch {
			case !present:
				// Missing slot: start it.
				plan.Steps = append(plan.Steps, Step{Action: ActionStart, WorkloadID: w.ID, BlobID: w.BlobID, ReplicaIndex: idx})
			case isStale(w, c, desired.Version):
				// Already scheduled a roll (stop) above; start the replacement.
				plan.Steps = append(plan.Steps, Step{Action: ActionStart, WorkloadID: w.ID, BlobID: w.BlobID, ReplicaIndex: idx})
			case needsRestart(c):
				// Present but unhealthy or crashed: restart in place.
				plan.Steps = append(plan.Steps, Step{Action: ActionRestart, WorkloadID: w.ID, BlobID: w.BlobID, ContainerID: c.ID, ReplicaIndex: idx})
			}
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
			if _, err := cli.Create(ctx, spec, desired.Version, step.ReplicaIndex); err != nil {
				errs = append(errs, fmt.Errorf("start %s replica %d: %w", step.WorkloadID, step.ReplicaIndex, err))
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
