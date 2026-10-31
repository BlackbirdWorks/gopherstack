package ecs

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	lostTaskReason        = "Task containers were lost when the emulator restarted"
	containerLostExitCode = 137
)

// adoptRestoredTasks reattaches the runner to the containers of RUNNING tasks restored from a snapshot. A task
// whose containers did not survive is stopped, as the real service stops a task whose containers are gone.
func (b *InMemoryBackend) adoptRestoredTasks(ctx context.Context) {
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
