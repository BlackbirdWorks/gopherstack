package ecs

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	lostTaskReason        = "Task containers were lost when the emulator restarted"
	containerLostExitCode = 137
)

// adoptRestoredTasks resumes a restored snapshot: RUNNING tasks are re-attached to their containers and tasks
// that were mid-launch are launched again.
func (b *InMemoryBackend) adoptRestoredTasks(ctx context.Context) {
	b.adoptRunningTasks(ctx)
	b.resumeRestoredLaunches()
}

// adoptRunningTasks reattaches the runner to the containers of RUNNING tasks restored from a snapshot. A task
// whose containers did not survive is stopped, as the real service stops a task whose containers are gone.
func (b *InMemoryBackend) adoptRunningTasks(ctx context.Context) {
	ad, ok := b.runner.(taskAdopter)
	if !ok {
		return
	}

	type pending struct {
		task *Task
		td   *TaskDefinition
	}

	var work []pending

	b.mu.RLock("adoptRestoredTasks")

	for _, t := range b.tasks.All() {
		if t.LastStatus != statusRunning {
			continue
		}

		if td, found := b.taskDefByArn.Get(t.TaskDefinitionArn); found {
			work = append(work, pending{task: t, td: td})
		}
	}

	b.mu.RUnlock()

	for _, w := range work {
		if ad.AdoptTask(ctx, w.task, w.td) {
			continue
		}

		logger.Load(ctx).WarnContext(ctx, "ecs: restored task lost its containers", "task", w.task.TaskArn)
		b.stopLostTask(w.task, w.td)
	}
}

func (b *InMemoryBackend) stopLostTask(task *Task, td *TaskDefinition) {
	name := ""
	if len(td.ContainerDefinitions) > 0 {
		name = td.ContainerDefinitions[0].Name
	}

	for _, cd := range td.ContainerDefinitions {
		if cd.Essential {
			name = cd.Name

			break
		}
	}

	b.mu.Lock("stopLostTask")

	if task.StoppedReason == "" {
		task.StoppedReason = lostTaskReason
	}

	b.mu.Unlock()

	b.markTaskStoppedByContainerExit(task.TaskArn, name, containerLostExitCode)
}

var errRestoredTaskDefinition = errors.New("task definition is gone")

// resumeRestoredLaunches launches again every restored task that was PROVISIONING, PENDING or ACTIVATING when
// the snapshot was taken. Tasks waiting for managed-scaling capacity stay with the scaler.
func (b *InMemoryBackend) resumeRestoredLaunches() {
	if _, noop := b.runner.(noopRunner); b.runner == nil || noop {
		return
	}

	var work []taskWork

	b.mu.RLock("resumeRestoredLaunches")

	for _, t := range b.tasks.All() {
		if !isLaunchingStatus(t.LastStatus) || t.AwaitingCapacity || t.DesiredStatus == statusStopped {
			continue
		}

		td, _ := b.taskDefByArn.Get(t.TaskDefinitionArn)
		work = append(work, taskWork{task: t, td: td, restored: true})
	}

	b.mu.RUnlock()

	for _, w := range work {
		b.ensureAwslogsStreams(w.task, w.td)
		b.launchAsync(w, clusterKey(clusterFromTaskARN(w.task.TaskArn)))
	}
}

// prepareRestoredLaunch readies a restored task for driveLaunch and reports whether driveLaunch should go on.
// RunTask only starts after the ACTIVATING step, so PROVISIONING and PENDING tasks own no containers and are
// launched afresh. An ACTIVATING task may own containers: they are adopted when all survive, else it starts over.
func (b *InMemoryBackend) prepareRestoredLaunch(ctx context.Context, w taskWork, clusterName string) bool {
	task := w.task

	if w.td == nil {
		b.mu.Lock("prepareRestoredLaunch-missingDef")
		defer b.mu.Unlock()

		if b.isLiveTaskLocked(task) && isLaunchingStatus(task.LastStatus) {
			b.failLaunchLocked(task, clusterName, errRestoredTaskDefinition, b.clock())
		}

		return false
	}

	b.mu.RLock("prepareRestoredLaunch")
	status := task.LastStatus
	b.mu.RUnlock()

	if ad, ok := b.runner.(taskAdopter); ok && status == statusActivating && ad.AdoptTask(ctx, task, w.td) {
		b.completeLaunch(ctx, w, clusterName, nil)

		return false
	}

	b.mu.Lock("prepareRestoredLaunch-rewind")
	defer b.mu.Unlock()

	if ctx.Err() == nil && b.isLiveTaskLocked(task) && isLaunchingStatus(task.LastStatus) {
		task.LastStatus = statusProvisioning
		task.PullStartedAt = nil
	}

	return true
}
