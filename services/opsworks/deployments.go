package opsworks

import (
	"time"

	"github.com/google/uuid"
)

// CreateDeployment creates a new deployment.
func (b *InMemoryBackend) CreateDeployment(stackID, appID, command string) (*Deployment, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return nil, ErrStackNotFound
	}

	id := uuid.NewString()
	now := time.Now().UTC()
	completedAt := now.Add(time.Second)

	d := &storedDeployment{
		CreatedAt:    now,
		CompletedAt:  completedAt,
		StackID:      stackID,
		AppID:        appID,
		DeploymentID: id,
		Command:      command,
		Status:       deploymentStatusSuccessful,
		Duration:     1,
	}
	b.deployments.Put(d)

	cmdID := uuid.NewString()
	cmd := &storedCommand{
		CreatedAt:      now,
		AcknowledgedAt: now,
		CompletedAt:    completedAt,
		DeploymentID:   id,
		InstanceID:     "",
		CommandID:      cmdID,
		Type:           command,
		Status:         commandStatusSuccessful,
		ExitCode:       0,
	}
	b.commands.Put(cmd)

	return d.toDeployment(), nil
}

// DescribeDeployments returns deployments filtered by stack, app, or IDs.
func (b *InMemoryBackend) DescribeDeployments(stackID, appID string, deploymentIDs []string) ([]*Deployment, error) {
	b.mu.RLock("DescribeDeployments")
	defer b.mu.RUnlock()

	if len(deploymentIDs) > 0 {
		result := make([]*Deployment, 0, len(deploymentIDs))
		for _, id := range deploymentIDs {
			d, ok := b.deployments.Get(id)
			if !ok {
				return nil, ErrDeploymentNotFound
			}
			result = append(result, d.toDeployment())
		}

		return result, nil
	}

	source := stackScoped(stackID, b.deployments.All, b.deploymentsByStack.Get)

	result := make([]*Deployment, 0, len(source))
	for _, d := range source {
		if appID != "" && d.AppID != appID {
			continue
		}
		result = append(result, d.toDeployment())
	}

	return result, nil
}
