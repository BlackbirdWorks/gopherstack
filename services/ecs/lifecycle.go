package ecs

import (
	"time"
)

// lifecycleKindStop and lifecycleKindStart identify which observable transition
// pipeline a tracked task is progressing through.
const (
	lifecycleKindStop  = "stop"
	lifecycleKindStart = "start"
)

// taskLifecycle records an in-flight task transition through the observable
// intermediate ECS states. AWS surfaces DEACTIVATING → STOPPING →
// DEPROVISIONING → STOPPED on a stop, and PROVISIONING → PENDING → RUNNING on a
// start; SDK waiters (for example ecs.TasksStopped) poll DescribeTasks and rely
// on being able to observe those states. The default fast path finalizes inline
// and never creates a taskLifecycle; entries exist only when stopDelay/startDelay
// are positive.
type taskLifecycle struct {
	nextAt      time.Time
	clusterName string
	kind        string
	phase       string
	reason      string
}

// SetStopDelay configures the per-phase delay applied to the task stop
// lifecycle. A positive delay makes StopTask leave the task in DEACTIVATING and
// advance it through STOPPING/DEPROVISIONING/STOPPED on subsequent lifecycle
// steps, so waiters can observe the intermediate states. Zero (the default)
// restores the immediate RUNNING→STOPPED transition.
func (b *InMemoryBackend) SetStopDelay(d time.Duration) {
	b.mu.Lock("SetStopDelay")
	defer b.mu.Unlock()

	b.stopDelay = d
}

// SetStartDelay configures the per-phase delay applied to the no-runner task
// start lifecycle (PROVISIONING→PENDING→RUNNING). Zero (the default) keeps the
// immediate transition.
func (b *InMemoryBackend) SetStartDelay(d time.Duration) {
	b.mu.Lock("SetStartDelay")
	defer b.mu.Unlock()

	b.startDelay = d
}

// registerStartLifecycleLocked enrolls a freshly created (PROVISIONING) task in
// the observable start pipeline. Must be called with the write lock held.
func (b *InMemoryBackend) registerStartLifecycleLocked(task *Task, clusterName string) {
	b.lifecycle[task.TaskArn] = &taskLifecycle{
		clusterName: clusterName,
		kind:        lifecycleKindStart,
		phase:       statusProvisioning,
		nextAt:      time.Now().Add(b.startDelay),
	}
}

// stepTaskLifecycle advances every tracked task whose next transition is due at
// or before now. Tasks that reach a terminal state (STOPPED for a stop, RUNNING
// for a start) are removed from tracking. For stopped tasks that had a real
// container runner, the runner's StopTask is invoked after the lock is released.
//
// This is invoked from the reconciler's background loop and is also called
// directly by tests with a controlled clock for deterministic assertions.
func (b *InMemoryBackend) stepTaskLifecycle(now time.Time) {
	b.mu.Lock("stepTaskLifecycle")

	// Collect tasks that finished stopping so their containers can be torn down
	// outside the lock (Docker calls must not serialize the backend).
	var toStopRunner []*Task

	for arn, lc := range b.lifecycle {
		if now.Before(lc.nextAt) {
			continue
		}

		task := b.lookupTaskLocked(arn)
		if task == nil {
			// Task was deleted (e.g. janitor swept it or cluster removed).
			delete(b.lifecycle, arn)

			continue
		}

		if b.advanceLifecycleLocked(task, lc, now, &toStopRunner) {
			delete(b.lifecycle, arn)
		}
	}

	b.mu.Unlock()

	if b.runner != nil {
		for _, t := range toStopRunner {
			_ = b.runner.StopTask(t)
		}
	}
}

// lookupTaskLocked returns the task pointer for taskArn, or nil. Must be
// called with at least the read lock held.
func (b *InMemoryBackend) lookupTaskLocked(taskArn string) *Task {
	t, _ := b.tasks.Get(taskArn)

	return t
}

// advanceLifecycleLocked moves a single task to its next observable state and
// reports whether the task has reached a terminal state (and should be removed
// from tracking). Must be called with the write lock held.
func (b *InMemoryBackend) advanceLifecycleLocked(
	task *Task,
	lc *taskLifecycle,
	now time.Time,
	toStopRunner *[]*Task,
) bool {
	switch lc.kind {
	case lifecycleKindStop:
		return b.advanceStopLocked(task, lc, now, toStopRunner)
	case lifecycleKindStart:
		return b.advanceStartLocked(task, lc, now)
	default:
		return true
	}
}

// nextStopPhase returns the state that follows the given stop-pipeline phase.
func nextStopPhase(phase string) string {
	switch phase {
	case statusDeactivating:
		return statusStopping
	case statusStopping:
		return statusDeprovisioning
	default:
		return statusStopped
	}
}

// advanceStopLocked steps a stopping task one phase forward. Returns true when
// the task reaches STOPPED.
func (b *InMemoryBackend) advanceStopLocked(
	task *Task,
	lc *taskLifecycle,
	now time.Time,
	toStopRunner *[]*Task,
) bool {
	next := nextStopPhase(lc.phase)
	task.LastStatus = next
	lc.phase = next
	lc.nextAt = now.Add(b.stopDelay)

	if next != statusStopped {
		return false
	}

	stoppedAt := now
	task.StoppedAt = &stoppedAt
	task.DesiredStatus = statusStopped

	if task.StoppedReason == "" {
		task.StoppedReason = lc.reason
	}

	syncContainerStatuses(task, nil)

	if b.runner != nil {
		*toStopRunner = append(*toStopRunner, task)
	}

	b.unindexTaskFromInstance(lc.clusterName, task.ContainerInstanceArn, task.TaskArn)
	b.taskProtections.Delete(task.TaskArn)
	b.deregisterTaskFromELBv2Locked(task, lc.clusterName)

	return true
}

// advanceStartLocked steps a starting task one phase forward. Returns true when
// the task reaches RUNNING.
func (b *InMemoryBackend) advanceStartLocked(task *Task, lc *taskLifecycle, now time.Time) bool {
	switch lc.phase {
	case statusProvisioning:
		task.LastStatus = statusPending
		lc.phase = statusPending
		lc.nextAt = now.Add(b.startDelay)

		return false
	default:
		task.LastStatus = statusRunning
		syncContainerStatuses(task, nil)

		if c, _ := b.clusters.Get(lc.clusterName); c != nil {
			c.PendingTasksCount--
			c.RunningTasksCount++
		}

		b.registerTaskWithELBv2Locked(task, lc.clusterName)

		return true
	}
}
