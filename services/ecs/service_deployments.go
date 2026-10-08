package ecs

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrServiceDeploymentNotFound is returned when a service deployment does not exist.
var ErrServiceDeploymentNotFound = awserr.New(
	"ServiceDeploymentNotFoundException",
	awserr.ErrNotFound,
)

// serviceDeploymentArnFor derives the ARN of the service deployment record for
// a Deployment, following the
// arn:aws:ecs:region:account:service-deployment/cluster/service/deployment-id
// scheme (mirroring serviceRevisionArnFor in services.go). deploymentID
// already carries its "ecs-svc/" prefix (see newPrimaryDeployment/
// newActiveDeployment), matching the shape of real ECS deployment IDs.
func serviceDeploymentArnFor(svc *Service, deploymentID string) string {
	return strings.Replace(svc.ServiceArn, ":service/", ":service-deployment/", 1) + "/" + deploymentID
}

const (
	serviceDeploymentRollbackSuccessful = "ROLLBACK_SUCCESSFUL"
	stopTypeRollback                    = "ROLLBACK"
)

// serviceDeploymentStatusFor maps a Deployment's RolloutState to the
// corresponding ServiceDeploymentStatus value. IN_PROGRESS is the default for
// any rollout state this backend doesn't model as a distinct terminal state.
func serviceDeploymentStatusFor(rolloutState string) string {
	switch rolloutState {
	case deploymentRolloutStateCompleted:
		return "SUCCESSFUL"
	case deploymentRolloutStateFailed:
		return statusStopped
	default:
		return "IN_PROGRESS"
	}
}

// recordServiceDeploymentLocked upserts the ServiceDeployment record tracking
// a single Deployment. Must be called with the write lock held.
func (b *InMemoryBackend) recordServiceDeploymentLocked(svc *Service, dep *Deployment) {
	depArn := serviceDeploymentArnFor(svc, dep.ID)

	createdAt := time.Now()
	if dep.CreatedAt != nil {
		createdAt = time.Unix(int64(*dep.CreatedAt), 0)
	}

	updatedAt := createdAt
	if dep.UpdatedAt != nil {
		updatedAt = time.Unix(int64(*dep.UpdatedAt), 0)
	}

	sd := &ServiceDeployment{
		ServiceDeploymentArn:     depArn,
		ClusterArn:               svc.ClusterArn,
		ServiceArn:               svc.ServiceArn,
		Status:                   serviceDeploymentStatusFor(dep.RolloutState),
		StatusReason:             dep.RolloutStateReason,
		CreatedAt:                &createdAt,
		StartedAt:                &createdAt,
		UpdatedAt:                &updatedAt,
		TargetServiceRevisionArn: dep.ServiceRevisionArn,
	}

	prev, hadPrev := b.serviceDeployments.Get(depArn)

	switch {
	case hadPrev && prev.LifecycleStage != "":
		carryLifecycle(sd, prev)
	case hadPrev && requestedTerminalStatus(prev.Status):
		sd.Status, sd.StatusReason, sd.Rollback = prev.Status, prev.StatusReason, prev.Rollback
		sd.UpdatedAt, sd.StoppedAt, sd.FinishedAt = prev.UpdatedAt, prev.StoppedAt, prev.FinishedAt
	case !hadPrev && lifecycleStrategy(svc.DeploymentConfiguration) && sd.Status == serviceDeploymentStatusInProgress:
		sd.Status = serviceDeploymentStatusInProgress
		b.serviceDeployments.Put(sd)
		b.initDeploymentLifecycleLocked(svc, sd, createdAt)

		return
	case isTerminalServiceDeploymentStatus(sd.Status):
		sd.FinishedAt = &updatedAt
		if sd.Status == statusStopped {
			sd.StoppedAt = &updatedAt
		}
	}

	b.serviceDeployments.Put(sd)
}

// requestedTerminalStatus reports statuses set by StopServiceDeployment, which a later sync from the
// deployment's rollout state must not overwrite.
func requestedTerminalStatus(status string) bool {
	return status == statusStopped || status == serviceDeploymentRollbackSuccessful
}

// deleteServiceDeploymentsForServiceLocked removes every ServiceDeployment
// record belonging to a service (keyed by ServiceDeploymentArn, which embeds
// the deployment ID — not by ServiceArn), so a deleted/purged service doesn't
// leave stale entries behind. Must be called with the write lock held.
func (b *InMemoryBackend) deleteServiceDeploymentsForServiceLocked(serviceArn string) {
	for _, sd := range b.serviceDeployments.All() {
		if sd.ServiceArn == serviceArn {
			b.serviceDeployments.Delete(sd.ServiceDeploymentArn)
		}
	}
}

// syncServiceDeploymentsLocked upserts a ServiceDeployment record for every
// entry currently on svc.Deployments. CreateService, UpdateService, and the
// deployment-circuit-breaker rollback path (deployment.go) all mutate
// svc.Deployments directly and must call this afterward, or
// DescribeServiceDeployments/ListServiceDeployments/StopServiceDeployment never
// see a real deployment (parity-principles.md rule 4). Must be called with the
// write lock held.
func (b *InMemoryBackend) syncServiceDeploymentsLocked(svc *Service) {
	for i := range svc.Deployments {
		b.recordServiceDeploymentLocked(svc, &svc.Deployments[i])
	}
}

// AddServiceDeploymentInternal adds a service deployment directly (seed helper for tests).
func (b *InMemoryBackend) AddServiceDeploymentInternal(sd *ServiceDeployment) {
	b.mu.Lock("AddServiceDeploymentInternal")
	defer b.mu.Unlock()

	c := *sd
	b.serviceDeployments.Put(&c)
}

// ListServiceDeployments returns service deployment briefs for a service in a cluster.
func (b *InMemoryBackend) ListServiceDeployments(cluster, service string) ([]ServiceDeployment, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("ListServiceDeployments")
	defer b.mu.Unlock()

	b.advanceAllDeploymentLifecyclesLocked(time.Now())

	all := b.serviceDeployments.All()
	out := make([]ServiceDeployment, 0, len(all))

	for _, sd := range all {
		if sd.ClusterArn != "" && !strings.HasSuffix(sd.ClusterArn, "/"+clusterName) {
			continue
		}

		if service != "" {
			svcKey := serviceKey(service)
			if !strings.HasSuffix(sd.ServiceArn, "/"+svcKey) {
				continue
			}
		}

		out = append(out, *sd)
	}

	slices.SortFunc(out, func(a, b ServiceDeployment) int {
		return cmp.Compare(a.ServiceDeploymentArn, b.ServiceDeploymentArn)
	})

	return out, nil
}

// StopServiceDeployment stops an in-progress service deployment. stopType ROLLBACK also reverts the
// service to its previous service revision.
func (b *InMemoryBackend) StopServiceDeployment(
	serviceDeploymentArn, stopType string,
) (*ServiceDeployment, error) {
	if serviceDeploymentArn == "" {
		return nil, fmt.Errorf("%w: serviceDeploymentArn is required", ErrInvalidParameter)
	}

	if stopType != "" && stopType != stopTypeRollback {
		return nil, fmt.Errorf("%w: invalid stopType %q", ErrInvalidParameter, stopType)
	}

	b.mu.Lock("StopServiceDeployment")
	defer b.mu.Unlock()

	sd, ok := b.serviceDeployments.Get(serviceDeploymentArn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceDeploymentNotFound, serviceDeploymentArn)
	}

	if requestedTerminalStatus(sd.Status) {
		return nil, fmt.Errorf("%w: %s", errServiceDeploymentAlreadyStopped, serviceDeploymentArn)
	}

	now := time.Now()

	if stopType == stopTypeRollback {
		if err := b.rollbackServiceDeploymentLocked(sd, now); err != nil {
			return nil, err
		}

		sd, _ = b.serviceDeployments.Get(serviceDeploymentArn)
	} else {
		sd.Status = statusStopped
		sd.StoppedAt = &now
	}

	sd.UpdatedAt = &now
	sd.FinishedAt = &now

	out := b.enrichServiceDeploymentLocked(sd)

	return &out, nil
}

// rollbackServiceDeploymentLocked reverts the owning service to its last stable revision and marks sd
// ROLLBACK_SUCCESSFUL. Caller holds b.mu.
func (b *InMemoryBackend) rollbackServiceDeploymentLocked(sd *ServiceDeployment, now time.Time) error {
	svc := b.serviceByArnLocked(sd.ServiceArn)
	if svc == nil {
		return fmt.Errorf("%w: service %s", ErrServiceNotFound, sd.ServiceArn)
	}

	if _, ok := lastStableTaskDefinition(svc); !ok {
		return fmt.Errorf("%w: no previous service revision to roll back to", ErrInvalidParameter)
	}

	if idx := primaryDeploymentIndex(svc); idx >= 0 {
		svc.Deployments[idx].RolloutState = deploymentRolloutStateFailed
		svc.Deployments[idx].RolloutStateReason = "Service deployment rolled back by StopServiceDeployment."
	}

	b.rollbackServiceLocked(svc)

	sd.Status = serviceDeploymentRollbackSuccessful
	sd.StatusReason = "Service deployment rolled back by StopServiceDeployment."
	sd.UpdatedAt, sd.FinishedAt = &now, &now
	sd.Rollback = &ServiceDeploymentRollback{
		Reason:    sd.StatusReason,
		StartedAt: &now,
	}

	if idx := primaryDeploymentIndex(svc); idx >= 0 {
		sd.Rollback.ServiceRevisionArn = svc.Deployments[idx].ServiceRevisionArn
	}

	b.syncServiceDeploymentsLocked(svc)

	return nil
}

func (b *InMemoryBackend) serviceByArnLocked(serviceArn string) *Service {
	for _, svc := range b.services.All() {
		if svc.ServiceArn == serviceArn {
			return svc
		}
	}

	return nil
}

// enrichServiceDeploymentLocked returns a copy of sd with the fields derived from the owning service's
// current state: revision summaries, circuit breaker and deployment configuration. Caller holds b.mu.
func (b *InMemoryBackend) enrichServiceDeploymentLocked(sd *ServiceDeployment) ServiceDeployment {
	out := *sd

	svc := b.serviceByArnLocked(sd.ServiceArn)
	if svc == nil {
		return out
	}

	out.deploymentConfiguration = svc.DeploymentConfiguration

	for i := range svc.Deployments {
		dep := svc.Deployments[i]
		summary := ServiceRevisionSummary{
			Arn:                dep.ServiceRevisionArn,
			RequestedTaskCount: dep.DesiredCount,
			RunningTaskCount:   dep.RunningCount,
			PendingTaskCount:   dep.PendingCount,
		}

		if serviceDeploymentArnFor(svc, dep.ID) == sd.ServiceDeploymentArn {
			out.targetRevision = &summary
			out.circuitBreaker = circuitBreakerFor(svc, &dep)

			continue
		}

		if dep.Status == statusActive && dep.RolloutState != deploymentRolloutStateFailed {
			out.sourceRevisions = append(out.sourceRevisions, summary)
		}
	}

	return out
}

func circuitBreakerFor(svc *Service, dep *Deployment) *ServiceDeploymentCircuitBreaker {
	cb := &ServiceDeploymentCircuitBreaker{FailureCount: dep.FailedTasks}

	desired := dep.DesiredCount
	if desired <= 0 {
		desired = svc.DesiredCount
	}

	cb.Threshold = circuitBreakerThreshold(desired)

	switch {
	case !circuitBreakerEnabled(svc):
		cb.Status = "DISABLED"
	case dep.RolloutState == deploymentRolloutStateFailed:
		cb.Status = "TRIGGERED"
	case dep.RolloutState == deploymentRolloutStateCompleted:
		cb.Status = "MONITORING_COMPLETE"
	default:
		cb.Status = "MONITORING"
	}

	return cb
}

const (
	deploymentLifecycleActionContinue = "CONTINUE"
	deploymentLifecycleActionRollback = "ROLLBACK"
)

// ContinueServiceDeployment continues or rolls back a service deployment paused at a PAUSE lifecycle hook.
func (b *InMemoryBackend) ContinueServiceDeployment(
	serviceDeploymentArn, hookID, action string,
) (*ServiceDeployment, error) {
	if serviceDeploymentArn == "" {
		return nil, fmt.Errorf("%w: serviceDeploymentArn is required", ErrInvalidParameter)
	}

	if hookID == "" {
		return nil, fmt.Errorf("%w: hookId is required", ErrInvalidParameter)
	}

	switch action {
	case "", deploymentLifecycleActionContinue, deploymentLifecycleActionRollback:
	default:
		return nil, fmt.Errorf("%w: invalid action %q", ErrInvalidParameter, action)
	}

	b.mu.Lock("ContinueServiceDeployment")
	defer b.mu.Unlock()

	sd, ok := b.serviceDeployments.Get(serviceDeploymentArn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceDeploymentNotFound, serviceDeploymentArn)
	}

	now := time.Now()
	b.advanceAllDeploymentLifecyclesLocked(now)

	if err := b.continueLifecycleHookLocked(sd, hookID, action, now); err != nil {
		return nil, err
	}

	cur, _ := b.serviceDeployments.Get(serviceDeploymentArn)
	out := b.enrichServiceDeploymentLocked(cur)

	return &out, nil
}

// DescribeServiceDeployments returns service deployments by ARN.
func (b *InMemoryBackend) DescribeServiceDeployments(
	serviceDeploymentArns []string,
) ([]ServiceDeployment, []Failure, error) {
	b.mu.Lock("DescribeServiceDeployments")
	defer b.mu.Unlock()

	b.advanceAllDeploymentLifecyclesLocked(time.Now())

	deployments := make([]ServiceDeployment, 0, len(serviceDeploymentArns))
	failures := make([]Failure, 0, len(serviceDeploymentArns))

	for _, arn := range serviceDeploymentArns {
		sd, ok := b.serviceDeployments.Get(arn)
		if !ok {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("service deployment %s not found", arn),
			})

			continue
		}

		deployments = append(deployments, b.enrichServiceDeploymentLocked(sd))
	}

	return deployments, failures, nil
}
