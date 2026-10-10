package ecs

import (
	"context"
	"strings"
)

const elbTargetUnhealthy = "unhealthy"

// ELBv2TargetHealthReader is an optional extension of ELBv2TargetRegistrar: when the wired registrar implements
// it, tasks whose ELBv2 target is unhealthy are stopped so the service scheduler replaces them.
type ELBv2TargetHealthReader interface {
	// TargetHealthState returns the target's health state; ok is false when the target is not registered.
	TargetHealthState(ctx context.Context, targetGroupARN string, target ELBTarget) (state string, ok bool)
}

type unhealthyTask struct {
	clusterArn     string
	taskArn        string
	targetGroupARN string
}

// stopELBUnhealthyTasks stops RUNNING service tasks that the load balancer reports unhealthy.
func (b *InMemoryBackend) stopELBUnhealthyTasks(ctx context.Context) {
	b.mu.RLock("StopELBUnhealthyTasks")
	reader, ok := b.elbv2Registrar.(ELBv2TargetHealthReader)

	if !ok {
		b.mu.RUnlock()

		return
	}

	type candidate struct {
		task    unhealthyTask
		targets []resolvedELBTarget
	}

	var candidates []candidate

	for _, t := range b.tasks.All() {
		if t.LastStatus != statusRunning || t.DesiredStatus != statusRunning {
			continue
		}

		targets, found := b.taskELBTargetsLocked(t, clusterKey(t.ClusterArn))
		if found {
			candidates = append(candidates, candidate{
				task:    unhealthyTask{clusterArn: t.ClusterArn, taskArn: t.TaskArn},
				targets: targets,
			})
		}
	}

	b.mu.RUnlock()

	for _, c := range candidates {
		for _, rt := range c.targets {
			state, found := reader.TargetHealthState(ctx, rt.lb.TargetGroupArn, rt.target)
			if found && strings.EqualFold(state, elbTargetUnhealthy) {
				c.task.targetGroupARN = rt.lb.TargetGroupArn
				_, _ = b.StopTask(c.task.clusterArn, c.task.taskArn, "Task failed ELB health checks in (target-group "+
					c.task.targetGroupARN+")")

				break
			}
		}
	}
}
