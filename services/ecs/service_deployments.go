package ecs

import (
	"fmt"
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

	b.serviceDeployments.Put(&ServiceDeployment{
		ServiceDeploymentArn:     depArn,
		ClusterArn:               svc.ClusterArn,
		ServiceArn:               svc.ServiceArn,
		Status:                   serviceDeploymentStatusFor(dep.RolloutState),
		StatusReason:             dep.RolloutStateReason,
		CreatedAt:                &createdAt,
		UpdatedAt:                &updatedAt,
		TargetServiceRevisionArn: dep.ServiceRevisionArn,
	})
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

	b.mu.RLock("ListServiceDeployments")
	defer b.mu.RUnlock()

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

	return out, nil
}

// StopServiceDeployment stops an in-progress service deployment.
func (b *InMemoryBackend) StopServiceDeployment(
	serviceDeploymentArn string,
) (*ServiceDeployment, error) {
	if serviceDeploymentArn == "" {
		return nil, fmt.Errorf("%w: serviceDeploymentArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("StopServiceDeployment")
	defer b.mu.Unlock()

	sd, ok := b.serviceDeployments.Get(serviceDeploymentArn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceDeploymentNotFound, serviceDeploymentArn)
	}

	if sd.Status == statusStopped {
		return nil, fmt.Errorf("%w: %s", errServiceDeploymentAlreadyStopped, serviceDeploymentArn)
	}

	sd.Status = statusStopped
	now := time.Now()
	sd.UpdatedAt = &now

	out := *sd

	return &out, nil
}

const (
	deploymentLifecycleActionContinue = "CONTINUE"
	deploymentLifecycleActionRollback = "ROLLBACK"
)

// ContinueServiceDeployment continues or rolls back a service deployment paused
// at a lifecycle hook. This backend does not model lifecycle hooks, so a
// deployment is never actually paused; the op still validates the deployment
// exists and hookId is present before reporting no such paused hook, rather than
// fabricating a successful continue/rollback.
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

	b.mu.RLock("ContinueServiceDeployment")
	defer b.mu.RUnlock()

	if !b.serviceDeployments.Has(serviceDeploymentArn) {
		return nil, fmt.Errorf("%w: %s", ErrServiceDeploymentNotFound, serviceDeploymentArn)
	}

	return nil, fmt.Errorf("%w: no paused lifecycle hook %q found for service deployment %s",
		errNoLifecycleHook, hookID, serviceDeploymentArn)
}

// DescribeServiceDeployments returns service deployments by ARN.
func (b *InMemoryBackend) DescribeServiceDeployments(
	serviceDeploymentArns []string,
) ([]ServiceDeployment, []Failure, error) {
	b.mu.RLock("DescribeServiceDeployments")
	defer b.mu.RUnlock()

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

		deployments = append(deployments, *sd)
	}

	return deployments, failures, nil
}
