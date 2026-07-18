package codedeploy

import "fmt"

// BatchGetDeploymentInstances returns stub instance summaries for the given instance IDs.
// Missing deployment returns an error per AWS behavior.
func (b *InMemoryBackend) BatchGetDeploymentInstances(
	deploymentID string,
	instanceIDs []string,
) ([]InstanceSummaryItem, error) {
	b.mu.RLock("BatchGetDeploymentInstances")
	defer b.mu.RUnlock()

	d, ok := b.deployments.Get(deploymentID)
	if !ok {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	result := make([]InstanceSummaryItem, 0, len(instanceIDs))

	for _, id := range instanceIDs {
		result = append(result, InstanceSummaryItem{
			DeploymentID: d.DeploymentID,
			InstanceID:   id,
			Status:       statusSucceeded,
		})
	}

	return result, nil
}

// BatchGetDeploymentTargets returns stub deployment targets for the given target IDs.
func (b *InMemoryBackend) BatchGetDeploymentTargets(
	deploymentID string,
	targetIDs []string,
) ([]*DeploymentTargetItem, error) {
	b.mu.RLock("BatchGetDeploymentTargets")
	defer b.mu.RUnlock()

	if !b.deployments.Has(deploymentID) {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	result := make([]*DeploymentTargetItem, 0, len(targetIDs))

	for _, id := range targetIDs {
		result = append(result, &DeploymentTargetItem{
			DeploymentID: deploymentID,
			TargetID:     id,
			Status:       statusSucceeded,
			TargetType:   "instanceTarget",
		})
	}

	return result, nil
}
