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
	// ActionStart starts a fresh container for a replica slot: a plain first start,
	// or the start half of a zero-downtime roll (the old container keeps serving).
	ActionStart Action = "start"
	// ActionStop stops and removes a container immediately: a scale-down, an extra,
	// a cold Egg or a duplicate.
	ActionStop Action = "stop"
	// ActionRestart restarts an up-to-date but crashed container in place.
	ActionRestart Action = "restart"
	// ActionRoll drains a superseded (stale or legacy) container gracefully, once a
	// healthy replacement is already serving: the stop half of a zero-downtime roll.
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

// Plan is the set of steps computed by Diff. A rolling deploy spans more than one
// plan: an early plan starts the new container (leaving the old serving), and a
// later plan drains the old once the new is healthy. No single plan ever stops a
// slot's only healthy backend.
type Plan struct {
	Steps []Step
}

// Diff computes the convergence plan for a desired state against the observed
// containers. It is pure: it performs no I/O and mutates nothing, which is what
// makes it unit testable.
//
// Rules, per desired workload with Replicas = N, one replica slot at a time
// (indices 0..N-1), converging towards one healthy, up-to-date container per slot
// WITHOUT ever taking the slot's only healthy backend away (zero-downtime rolls):
//
//   - No up-to-date container in the slot: START a new one. Any stale (old image or
//     version) or legacy container in the slot is LEFT RUNNING so it keeps serving
//     whilst the new one boots. This is the start half of a start-new-before-stop-old
//     roll, and also the plain first start when the slot is empty.
//   - An up-to-date container exists and is HEALTHY: the slot is served by new code,
//     so DRAIN the old and legacy containers now (the stop half of the roll), and
//     stop any duplicate up-to-date containers.
//   - An up-to-date container exists but is still booting: WAIT. The old container
//     (if any) keeps serving; nothing is stopped.
//   - An up-to-date container has crashed: RESTART it in place.
//   - A container at an index >= N (a scale-down) is surplus and stopped.
//   - Replicas == 0 stops every container for the workload (the Egg goes cold).
//   - A container with no matching desired workload is stopped (extra).
func Diff(desired contract.DesiredState, actual []docker.Container) Plan {
	var plan Plan

	desiredByID := make(map[string]contract.WorkloadSpec, len(desired.Workloads))
	for _, w := range desired.Workloads {
		desiredByID[w.ID] = w
	}

	// Group observed containers by workload id.
	byWorkload := make(map[string][]docker.Container, len(actual))
	for _, c := range actual {
		byWorkload[c.WorkloadID] = append(byWorkload[c.WorkloadID], c)
	}

	step := func(action Action, c docker.Container) Step {
		return Step{Action: action, WorkloadID: c.WorkloadID, BlobID: c.BlobID, ContainerID: c.ID, ReplicaIndex: c.ReplicaIndex}
	}

	for _, w := range desired.Workloads {
		cs := byWorkload[w.ID]

		// Desired cold: stop every container for this workload.
		if w.Replicas <= 0 {
			for _, c := range cs {
				plan.Steps = append(plan.Steps, step(ActionStop, c))
			}
			continue
		}

		// A stateful workload keeps its data on ONE shared persistent volume, so a
		// slot can never hold two containers at once: a second database engine locks
		// on the data dir (Postgres postmaster.pid) and crash-loops. Such a workload
		// therefore rolls stop-old-before-start-new (a brief blip) instead of the
		// stateless zero-downtime start-new-before-stop-old.
		stateful := len(w.Volumes) > 0

		// Bucket the workload's containers by replica slot; anything at an index
		// beyond the desired count is a scale-down and stopped immediately.
		bySlot := make(map[int][]docker.Container, w.Replicas)
		for _, c := range cs {
			if c.ReplicaIndex < 0 || c.ReplicaIndex >= w.Replicas {
				plan.Steps = append(plan.Steps, step(ActionStop, c))
				continue
			}
			bySlot[c.ReplicaIndex] = append(bySlot[c.ReplicaIndex], c)
		}

		for idx := 0; idx < w.Replicas; idx++ {
			var upToDate, old []docker.Container
			for _, c := range bySlot[idx] {
				// Legacy (pre-replica-index) and stale (wrong image or version)
				// containers are "old": kept running until a fresh one is healthy.
				if c.Legacy || isStale(w, c, desired.Version) {
					old = append(old, c)
				} else {
					upToDate = append(upToDate, c)
				}
			}

			if len(upToDate) == 0 {
				// Stateful: never start a second container on the shared volume. Stop the
				// old one now; the fresh one starts on the next tick once the slot is
				// empty (stop-old-before-start-new). This is what makes a database Egg
				// redeploy or restart work instead of deadlocking two engines on one dir.
				if stateful && len(old) > 0 {
					for _, c := range old {
						plan.Steps = append(plan.Steps, step(ActionStop, c))
					}
					continue
				}
				// Slot empty (or stateless): start the fresh container. For a stateless
				// workload any old container is left serving until the new one is healthy.
				plan.Steps = append(plan.Steps, Step{Action: ActionStart, WorkloadID: w.ID, BlobID: w.BlobID, ReplicaIndex: idx})
				continue
			}

			// Prefer a healthy up-to-date container as the keeper, so a flapping
			// duplicate does not win and trigger a premature drain of the old one.
			keeper := upToDate[0]
			for _, c := range upToDate {
				if healthy(c) {
					keeper = c
					break
				}
			}
			// Stop any duplicate up-to-date containers for this slot.
			for _, c := range upToDate {
				if c.ID != keeper.ID {
					plan.Steps = append(plan.Steps, step(ActionStop, c))
				}
			}

			switch {
			case healthy(keeper):
				// New code is serving: drain the superseded old and legacy containers
				// now. This is the stop half of the roll, and it only ever runs once a
				// healthy replacement exists, so the slot is never left without a backend.
				for _, c := range old {
					plan.Steps = append(plan.Steps, step(ActionRoll, c))
				}
			case needsRestart(keeper):
				// Up-to-date but crashed: restart in place. Any old container keeps
				// serving until the restarted one is healthy.
				plan.Steps = append(plan.Steps, step(ActionRestart, keeper))
			default:
				// Keeper is still booting: wait. The old container (if any) serves on.
			}
		}
	}

	// Extra containers: any observed workload not in desired state at all.
	for _, c := range actual {
		if _, wanted := desiredByID[c.WorkloadID]; !wanted {
			plan.Steps = append(plan.Steps, step(ActionStop, c))
		}
	}

	return plan
}

// healthy reports whether a container is up and passing its health check, i.e. it
// is a live backend that ingress will route to. List reports a running-but-unhealthy
// container as Starting, so a Running state already implies the last probe passed;
// the explicit Healthy check keeps unit tests that set the fields directly honest.
func healthy(c docker.Container) bool {
	return c.State == contract.StateRunning && c.Healthy
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
		case ActionStop:
			// Immediate removal: a scale-down, an extra, a cold Egg or a duplicate.
			if step.ContainerID != "" {
				if err := cli.Stop(ctx, step.ContainerID); err != nil {
					errs = append(errs, fmt.Errorf("stop %s: %w", step.WorkloadID, err))
				}
			}
		case ActionRoll:
			// The stop half of a zero-downtime roll: gracefully drain the old container
			// now that a healthy replacement is serving. Its replacement was started on
			// an earlier tick as a separate ActionStart.
			if step.ContainerID != "" {
				if err := cli.Drain(ctx, step.ContainerID); err != nil {
					errs = append(errs, fmt.Errorf("drain %s: %w", step.WorkloadID, err))
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
