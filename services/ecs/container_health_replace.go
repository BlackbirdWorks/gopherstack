package ecs

import "time"

const containerHealthStopReason = "Task failed container health checks"

// stopUnhealthyServiceTasks stops RUNNING service tasks whose essential-container health is UNHEALTHY once the
// service's healthCheckGracePeriodSeconds has elapsed since the task started; the reconciler then launches the
// replacement within the deployment's maximumPercent. Standalone tasks are never replaced. Unhealthy tasks do not
// count as healthy, so minimumHealthyPercent does not hold the stop back.
func (b *InMemoryBackend) stopUnhealthyServiceTasks() {
	type target struct{ cluster, arn string }

	var targets []target

	b.mu.RLock("stopUnhealthyServiceTasks")

	now := b.clock()

	for _, t := range b.tasks.All() {
		if t.LastStatus != statusRunning || t.DesiredStatus != statusRunning || t.HealthStatus != healthECSUnhealthy {
			continue
		}

		svc := b.serviceForTaskLocked(t, clusterKey(t.ClusterArn))
		if svc == nil || svc.Status != statusActive || inHealthGracePeriod(svc, t, now) {
			continue
		}

		targets = append(targets, target{cluster: t.ClusterArn, arn: t.TaskArn})
	}

	b.mu.RUnlock()

	for _, tg := range targets {
		if _, err := b.StopTask(tg.cluster, tg.arn, containerHealthStopReason); err == nil {
			b.markServiceSchedulerStop(tg.arn)
		}
	}
}

func inHealthGracePeriod(svc *Service, t *Task, now time.Time) bool {
	if svc.HealthCheckGracePeriodSeconds == nil || *svc.HealthCheckGracePeriodSeconds <= 0 || t.StartedAt == nil {
		return false
	}

	return now.Before(t.StartedAt.Add(time.Duration(*svc.HealthCheckGracePeriodSeconds) * time.Second))
}

func (b *InMemoryBackend) markServiceSchedulerStop(taskArn string) {
	b.mu.Lock("markServiceSchedulerStop")
	defer b.mu.Unlock()

	if t, ok := b.tasks.Get(taskArn); ok {
		t.StopCode = stopCodeServiceSchedulerInitiated
	}
}
