package ecs

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	deploymentIDPrefix = "ecs-svc/"
	percentBase        = 100
)

// taskBelongsToDeployment reports whether t was started under dep. Tasks that
// predate deployment tagging fall back to a task-definition match.
func taskBelongsToDeployment(t *Task, dep *Deployment) bool {
	if strings.HasPrefix(t.StartedBy, deploymentIDPrefix) {
		return t.StartedBy == dep.ID
	}

	return t.TaskDefinitionArn == dep.TaskDefinition
}

func taskIsLive(t *Task) bool {
	switch t.LastStatus {
	case statusRunning, statusPending, statusProvisioning, statusActivating:
		return true
	}

	return false
}

// rollingPlan is the reconciler's view of a service mid-rollout.
type rollingPlan struct {
	staleRunning []string
	currentLive  int
	runningTotal int
	staleLive    int
}

// rollingPlanLocked classifies a service's live tasks against its PRIMARY
// deployment. Must be called with at least the read lock held.
func (b *InMemoryBackend) rollingPlanLocked(clusterName string, svc *Service) rollingPlan {
	var plan rollingPlan

	idx := primaryDeploymentIndex(svc)
	if idx < 0 {
		return plan
	}

	primary := &svc.Deployments[idx]
	group := "service:" + svc.ServiceName

	type staleTask struct {
		at  time.Time
		arn string
	}

	var stale []staleTask

	for _, t := range b.tasksByCluster.Get(clusterName) {
		if t.Group != group || !taskIsLive(t) {
			continue
		}

		if t.LastStatus == statusRunning {
			plan.runningTotal++
		}

		if taskBelongsToDeployment(t, primary) {
			plan.currentLive++

			continue
		}

		plan.staleLive++

		if t.LastStatus == statusRunning {
			at := time.Time{}
			if t.StartedAt != nil {
				at = *t.StartedAt
			}

			stale = append(stale, staleTask{arn: t.TaskArn, at: at})
		}
	}

	sort.Slice(stale, func(i, j int) bool { return stale[i].at.Before(stale[j].at) })

	for _, s := range stale {
		plan.staleRunning = append(plan.staleRunning, s.arn)
	}

	return plan
}

func deploymentPercents(svc *Service) (int, int) {
	maxPct, minPct := defaultMaximumPercent, defaultMinimumHealthyPercent

	if dc := svc.DeploymentConfiguration; dc != nil {
		if dc.MaximumPercent != nil {
			maxPct = *dc.MaximumPercent
		}

		if dc.MinimumHealthyPercent != nil {
			minPct = *dc.MinimumHealthyPercent
		}
	}

	return maxPct, minPct
}

func usesRollingReplacement(svc *Service) bool {
	if lifecycleStrategy(svc.DeploymentConfiguration) {
		return false
	}

	return svc.DeploymentController == nil || svc.DeploymentController.Type == "" ||
		strings.EqualFold(svc.DeploymentController.Type, "ECS")
}

// rollForward replaces tasks of superseded deployments with tasks of the PRIMARY
// one, bounded by MaximumPercent and MinimumHealthyPercent.
func (r *Reconciler) rollForward(
	ctx context.Context, log *slog.Logger, snap serviceSnapshot, plan rollingPlan,
) {
	svc := snap.service
	desired := svc.DesiredCount
	maxPct, minPct := deploymentPercents(&svc)
	maxTotal := desired * maxPct / percentBase
	minHealthy := (desired*minPct + percentBase - 1) / percentBase

	stopBudget := min(plan.runningTotal-minHealthy, len(plan.staleRunning))
	if desired == 0 {
		stopBudget = len(plan.staleRunning)
	}

	var stoppedARNs []string

	for i := range stopBudget {
		if err := r.backend.stopServiceTask(snap.clusterName, plan.staleRunning[i], "deployment replaced"); err != nil {
			log.WarnContext(ctx, "ECS reconciler: stop superseded task failed",
				"cluster", snap.clusterName, "service", svc.ServiceName, "error", err)

			break
		}

		stoppedARNs = append(stoppedARNs, plan.staleRunning[i])
	}

	stopped := len(stoppedARNs)
	if stopped > 0 {
		r.backend.recordServiceEvent(
			snap.clusterName,
			svc.ServiceName,
			taskListMessage(svc.ServiceName, "stopped", stoppedARNs),
		)
	}

	totalLive := plan.currentLive + plan.staleLive - stopped
	toStart := min(desired-plan.currentLive, maxTotal-totalLive)

	if toStart > 0 {
		r.launchTasks(ctx, log, snap, toStart)
	}
}

// pruneSettledDeployments drops superseded deployments that no longer own any
// live task once the PRIMARY deployment has reached its desired count.
func (b *InMemoryBackend) pruneSettledDeployments(clusterName, serviceName string) {
	b.mu.Lock("pruneSettledDeployments")
	defer b.mu.Unlock()

	svc, ok := b.services.Get(scopedKey(clusterName, serviceName))
	if !ok || len(svc.Deployments) < 2 || primaryDeploymentFailed(svc) {
		return
	}

	plan := b.rollingPlanLocked(clusterName, svc)
	if plan.staleLive > 0 || plan.currentLive < svc.DesiredCount || svc.DesiredCount == 0 {
		return
	}

	kept := make([]Deployment, 0, 1)

	for _, d := range svc.Deployments {
		if d.Status == deploymentStatusPrimary {
			kept = append(kept, d)
		}
	}

	svc.Deployments = kept
	b.addServiceEventLocked(
		svc,
		fmt.Sprintf("(service %s) (deployment %s) deployment completed.", serviceName, kept[0].ID),
	)

	if d := &svc.Deployments[0]; !d.completionEmitted {
		d.completionEmitted = true
		b.emitDeploymentStateLocked(svc, d.ID, eventDeploymentCompleted, "")
	}
}

// stopServiceTask stops one service task by ARN, releasing its resources.
func (b *InMemoryBackend) stopServiceTask(clusterName, taskArn, reason string) error {
	var (
		task        *Task
		instanceArn string
	)

	func() {
		b.mu.Lock("stopServiceTask")
		defer b.mu.Unlock()

		for _, t := range b.tasksByCluster.Get(clusterName) {
			if t.TaskArn == taskArn && t.LastStatus == statusRunning {
				task = t

				break
			}
		}

		if task == nil {
			return
		}

		now := time.Now()
		task.LastStatus = statusStopped
		task.DesiredStatus = statusStopped
		task.StoppedAt = &now
		task.StoppedReason = reason
		task.StopCode = stopCodeServiceSchedulerInitiated
		syncContainerStatuses(task, nil)
		b.emitTaskStateLocked(task)
		b.deregisterTaskFromELBv2Locked(task, clusterName)
		b.deregisterTaskFromCloudMapLocked(task, clusterName)
		b.releaseTaskHostPortsLocked(clusterName, task)
		delete(b.lifecycle, task.TaskArn)

		if c, _ := b.clusters.Get(clusterName); c != nil {
			c.RunningTasksCount--
		}

		instanceArn = task.ContainerInstanceArn
	}()

	if task == nil {
		return nil
	}

	if b.runner != nil {
		_ = b.runner.StopTask(task)
	}

	b.mu.Lock("stopServiceTask-unindex")
	defer b.mu.Unlock()

	b.taskProtections.Delete(taskArn)
	b.unindexTaskFromInstance(clusterName, instanceArn, taskArn)

	return nil
}

// deploymentOwnsTask reports whether deployments[i] is the deployment t counts
// toward: its tagged deployment, or for untagged tasks the first one with a
// matching task definition.
func deploymentOwnsTask(deployments []Deployment, i int, t *Task) bool {
	if strings.HasPrefix(t.StartedBy, deploymentIDPrefix) {
		return t.StartedBy == deployments[i].ID
	}

	for j := range deployments {
		if deployments[j].TaskDefinition == t.TaskDefinitionArn {
			return j == i
		}
	}

	return false
}

const maxServiceEvents = 100

// addServiceEventLocked prepends a copy-on-write event; callers may still hold older snapshots.
func (b *InMemoryBackend) addServiceEventLocked(svc *Service, message string) {
	ev := ServiceEvent{CreatedAt: time.Now(), ID: uuid.NewString(), Message: message}
	events := make([]ServiceEvent, 0, min(len(svc.Events)+1, maxServiceEvents))
	events = append(events, ev)
	events = append(events, svc.Events...)

	if len(events) > maxServiceEvents {
		events = events[:maxServiceEvents]
	}

	svc.Events = events
	svc.steadyReported = false
}

func (b *InMemoryBackend) recordServiceEvent(clusterName, serviceName, message string) {
	b.mu.Lock("recordServiceEvent")
	defer b.mu.Unlock()

	if svc, ok := b.services.Get(scopedKey(clusterName, serviceName)); ok {
		b.addServiceEventLocked(svc, message)
	}
}

// reportSteadyState records the steady-state event once per settled period.
func (b *InMemoryBackend) reportSteadyState(clusterName, serviceName string) {
	b.mu.Lock("reportSteadyState")
	defer b.mu.Unlock()

	svc, ok := b.services.Get(scopedKey(clusterName, serviceName))
	if !ok || svc.steadyReported || len(svc.Deployments) != 1 || svc.DesiredCount == 0 {
		return
	}

	plan := b.rollingPlanLocked(clusterName, svc)
	if plan.currentLive != svc.DesiredCount || plan.runningTotal != svc.DesiredCount || plan.staleLive > 0 {
		return
	}

	b.addServiceEventLocked(svc, fmt.Sprintf("(service %s) has reached a steady state.", serviceName))
	svc.steadyReported = true

	if d := &svc.Deployments[0]; !d.completionEmitted {
		d.completionEmitted = true
		b.emitDeploymentStateLocked(svc, d.ID, eventDeploymentCompleted, "")
	}

	b.emitServiceActionLocked(svc, eventServiceSteadyState)
}

func taskListMessage(serviceName, verb string, taskARNs []string) string {
	ids := make([]string, len(taskARNs))
	for i, a := range taskARNs {
		ids[i] = "(task " + taskIDFromARN(a) + ")"
	}

	return fmt.Sprintf("(service %s) has %s %d tasks: %s.", serviceName, verb, len(ids), strings.Join(ids, " "))
}
