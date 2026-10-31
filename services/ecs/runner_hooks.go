package ecs

import "context"

// taskCompletionRunner is implemented by TaskRunners that can report when a
// container they started exits on its own, not via an explicit StopTask call
// (currently only realDockerRunner). Runners that don't implement it
// (noopRunner, test fakes) stay unwired: their tasks only reach STOPPED via
// StopTask.
type taskCompletionRunner interface {
	SetTaskCompletionHandler(fn func(taskArn, containerName string, exitCode int))
}

// containerHealthRunner is implemented by runners that report real container health.
type containerHealthRunner interface {
	SetContainerHealthHandler(fn func(taskArn, containerName, status string))
	SetContainerReasonHandler(fn func(taskArn, containerName, reason string))
}

// secretRunner is implemented by runners that inject container secrets.
type secretRunner interface {
	SetSecretResolver(fn func(ctx context.Context, valueFrom string) (string, error))
}

// wireRunnerHooks connects every optional runner capability to this backend.
func (b *InMemoryBackend) wireRunnerHooks(runner TaskRunner) {
	if r, ok := runner.(taskCompletionRunner); ok {
		r.SetTaskCompletionHandler(b.markTaskStoppedByContainerExit)
	}

	if r, ok := runner.(taskEnvRunner); ok {
		r.SetTaskEnvProvider(b.containerRuntimeEnv)
	}

	if r, ok := runner.(containerAttachRunner); ok {
		r.SetContainerAttacher(b.attachLinkLocal)
	}

	if r, ok := runner.(admitHookRunner); ok {
		r.SetAdmitHook(b.taskAdmit)
	}

	if r, ok := runner.(containerHealthRunner); ok {
		r.SetContainerHealthHandler(b.updateContainerHealth)
		r.SetContainerReasonHandler(b.setContainerReason)
	}

	if r, ok := runner.(secretRunner); ok {
		r.SetSecretResolver(b.resolveSecret)
	}

	if r, ok := runner.(efsVolumeRunner); ok {
		r.SetEFSVolumeResolver(b.resolveEFSVolume)
	}

	if r, ok := runner.(launchInputRunner); ok {
		r.SetRegistryAuthResolver(b.resolveRegistryAuth)
		r.SetEnvFileResolver(b.resolveEnvFiles)
	}
}

// updateContainerHealth records a container's runtime health and recomputes the task's.
func (b *InMemoryBackend) updateContainerHealth(taskArn, containerName, status string) {
	b.mu.Lock("updateContainerHealth")
	defer b.mu.Unlock()

	task, ok := b.tasks.Get(taskArn)
	if !ok {
		return
	}

	for i := range task.Containers {
		if task.Containers[i].Name == containerName {
			task.Containers[i].HealthStatus = status
		}
	}

	task.HealthStatus = b.taskHealthLocked(task)
	b.emitTaskStateLocked(task)
}

// setContainerReason records why a container stopped (for example OutOfMemoryError).
func (b *InMemoryBackend) setContainerReason(taskArn, containerName, reason string) {
	b.mu.Lock("setContainerReason")
	defer b.mu.Unlock()

	task, ok := b.tasks.Get(taskArn)
	if !ok {
		return
	}

	for i := range task.Containers {
		if task.Containers[i].Name == containerName {
			task.Containers[i].Reason = reason
		}
	}
}

// taskHealthLocked is HEALTHY when every essential container with a health check is HEALTHY, UNHEALTHY when
// any is UNHEALTHY, and UNKNOWN otherwise.
func (b *InMemoryBackend) taskHealthLocked(task *Task) string {
	essential := map[string]bool{}

	if td, err := b.findTaskDefinitionLocked(task.TaskDefinitionArn); err == nil {
		for _, cd := range td.ContainerDefinitions {
			essential[cd.Name] = cd.Essential
		}
	}

	checked, healthy := 0, 0

	for _, c := range task.Containers {
		if !essential[c.Name] || c.HealthStatus == "" {
			continue
		}

		checked++

		switch c.HealthStatus {
		case healthECSUnhealthy:
			return healthECSUnhealthy
		case healthECSHealthy:
			healthy++
		}
	}

	if checked > 0 && healthy == checked {
		return healthECSHealthy
	}

	return healthECSUnknown
}
