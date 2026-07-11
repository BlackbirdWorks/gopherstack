package ecs

import (
	"fmt"
	"strings"
)

// Deployment circuit-breaker tuning constants.
//
// AWS computes the failed-task threshold for the deployment circuit breaker from
// the deployment's desired count: it starts at a floor and scales with half the
// desired count, capped at a ceiling. See
// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-circuit-breaker.html
const (
	// circuitBreakerMinFailures is the lower bound on the number of failed tasks
	// required to trip the circuit breaker (AWS uses a floor of 3).
	circuitBreakerMinFailures = 3
	// circuitBreakerMaxFailures is the upper bound on the failure threshold.
	circuitBreakerMaxFailures = 200
	// circuitBreakerHalfDivisor scales the threshold with half the desired count.
	circuitBreakerHalfDivisor = 2
)

// circuitBreakerThreshold returns the number of failed task launches that trips
// the deployment circuit breaker for a deployment with the given desired count.
// It is clamped to [circuitBreakerMinFailures, circuitBreakerMaxFailures].
func circuitBreakerThreshold(desiredCount int) int {
	// ceil(desiredCount / 2) without floating point.
	threshold := (desiredCount + 1) / circuitBreakerHalfDivisor
	threshold = max(threshold, circuitBreakerMinFailures)
	threshold = min(threshold, circuitBreakerMaxFailures)

	return threshold
}

// circuitBreakerEnabled reports whether the service has an enabled deployment
// circuit breaker.
func circuitBreakerEnabled(svc *Service) bool {
	dc := svc.DeploymentConfiguration

	return dc != nil &&
		dc.DeploymentCircuitBreaker != nil &&
		dc.DeploymentCircuitBreaker.Enable
}

// circuitBreakerRollback reports whether the service's circuit breaker is
// configured to roll back on failure.
func circuitBreakerRollback(svc *Service) bool {
	dc := svc.DeploymentConfiguration

	return dc != nil &&
		dc.DeploymentCircuitBreaker != nil &&
		dc.DeploymentCircuitBreaker.Rollback
}

// primaryDeploymentIndex returns the index of the PRIMARY deployment in the
// service's deployment list, or -1 when there is none.
func primaryDeploymentIndex(svc *Service) int {
	for i := range svc.Deployments {
		if svc.Deployments[i].Status == deploymentStatusPrimary {
			return i
		}
	}

	return -1
}

// serviceNameFromGroup extracts the service name from a task Group of the form
// "service:<name>", returning ("", false) for non-service tasks.
func serviceNameFromGroup(group string) (string, bool) {
	const prefix = "service:"
	if !strings.HasPrefix(group, prefix) {
		return "", false
	}

	return strings.TrimPrefix(group, prefix), true
}

// recordServiceTaskFailureLocked attributes a failed task launch to its service's
// PRIMARY deployment and then evaluates the deployment circuit breaker. It is a
// no-op for tasks that do not belong to a service. Must be called with the write
// lock held.
func (b *InMemoryBackend) recordServiceTaskFailureLocked(clusterName string, task *Task) {
	serviceName, ok := serviceNameFromGroup(task.Group)
	if !ok {
		return
	}

	svc, ok := b.services.Get(scopedKey(clusterName, serviceName))
	if !ok {
		return
	}

	idx := primaryDeploymentIndex(svc)
	if idx < 0 {
		return
	}

	svc.Deployments[idx].FailedTasks++

	b.evaluateCircuitBreakerLocked(svc)
}

// evaluateCircuitBreakerLocked trips the circuit breaker on a service's PRIMARY
// deployment when the accumulated failed-task count reaches the threshold. When
// rollback is enabled it also reverts the service to the last stable task
// definition. Must be called with the write lock held.
func (b *InMemoryBackend) evaluateCircuitBreakerLocked(svc *Service) {
	if !circuitBreakerEnabled(svc) {
		return
	}

	idx := primaryDeploymentIndex(svc)
	if idx < 0 {
		return
	}

	dep := &svc.Deployments[idx]
	if dep.RolloutState != deploymentRolloutStateInProgress {
		return
	}

	// The desired count on the deployment can be zero before the reconciler has
	// stamped it; fall back to the service desired count for the threshold.
	desired := dep.DesiredCount
	if desired <= 0 {
		desired = svc.DesiredCount
	}

	if dep.FailedTasks < circuitBreakerThreshold(desired) {
		return
	}

	dep.RolloutState = deploymentRolloutStateFailed
	dep.RolloutStateReason = fmt.Sprintf(
		"ECS deployment circuit breaker: task failed to start. %d tasks failed to start.",
		dep.FailedTasks,
	)

	if circuitBreakerRollback(svc) {
		b.rollbackServiceLocked(svc)
	}

	b.syncServiceDeploymentsLocked(svc)
}

// lastStableTaskDefinition returns the task definition of the most recent
// non-PRIMARY (demoted/ACTIVE) deployment — the previously-running configuration
// to roll back to. Deployments are ordered newest-first, so the first
// non-PRIMARY entry that did not itself fail is the last stable one. Returns
// ("", false) when there is none (for example the initial deployment, which has
// no prior stable state and therefore cannot be rolled back).
func lastStableTaskDefinition(svc *Service) (string, bool) {
	for i := range svc.Deployments {
		d := svc.Deployments[i]
		if d.Status != deploymentStatusPrimary &&
			d.RolloutState != deploymentRolloutStateFailed &&
			d.TaskDefinition != "" {
			return d.TaskDefinition, true
		}
	}

	return "", false
}

// rollbackServiceLocked reverts a service to its last stable task definition and
// starts a fresh PRIMARY deployment for it. The failed deployment is demoted to
// ACTIVE but retains its FAILED rollout state so the failure remains visible.
// It is a no-op when there is no stable deployment to roll back to. Must be
// called with the write lock held.
func (b *InMemoryBackend) rollbackServiceLocked(svc *Service) {
	stableDef, ok := lastStableTaskDefinition(svc)
	if !ok {
		return
	}

	svc.TaskDefinition = stableDef

	// Demote the failed PRIMARY to ACTIVE (keeping its FAILED rollout state) and
	// prepend a new PRIMARY deployment for the stable task definition.
	for i := range svc.Deployments {
		if svc.Deployments[i].Status == deploymentStatusPrimary {
			svc.Deployments[i].Status = statusActive
		}
	}

	rollback := newActiveDeployment(svc)
	rollback.RolloutStateReason = fmt.Sprintf(
		"ECS deployment circuit breaker: rolling back to the last stable deployment (%s).",
		stableDef,
	)

	svc.Deployments = append([]Deployment{rollback}, svc.Deployments...)

	b.addServiceRevisionLocked(svc)
}

// primaryDeploymentFailed reports whether the service's PRIMARY deployment has a
// FAILED rollout state, meaning a halted (non-rolled-back) deployment.
func primaryDeploymentFailed(svc *Service) bool {
	idx := primaryDeploymentIndex(svc)

	return idx >= 0 && svc.Deployments[idx].RolloutState == deploymentRolloutStateFailed
}
