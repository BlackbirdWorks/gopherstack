package opsworks

import (
	"slices"
	"time"

	"github.com/google/uuid"
)

// CreateDeployment creates a new deployment.
func (b *InMemoryBackend) CreateDeployment(
	stackID, appID, command, customJSON string, opts DeploymentOptions,
) (*Deployment, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return nil, ErrStackNotFound
	}

	targets, err := b.deploymentTargets(stackID, opts)
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()
	now := time.Now().UTC()
	completedAt := now.Add(time.Second)

	d := &storedDeployment{
		CreatedAt:    now,
		CompletedAt:  completedAt,
		InstanceIDs:  targets,
		Comment:      opts.Comment,
		StackID:      stackID,
		AppID:        appID,
		DeploymentID: id,
		Command:      command,
		Status:       deploymentStatusSuccessful,
		CustomJSON:   customJSON,
		Duration:     1,
	}
	b.deployments.Put(d)

	cmdTargets := targets
	if len(cmdTargets) == 0 {
		cmdTargets = []string{""}
	}

	for _, instanceID := range cmdTargets {
		b.commands.Put(&storedCommand{
			CreatedAt:      now,
			AcknowledgedAt: now,
			CompletedAt:    completedAt,
			DeploymentID:   id,
			InstanceID:     instanceID,
			CommandID:      uuid.NewString(),
			Type:           command,
			Status:         commandStatusSuccessful,
			ExitCode:       0,
		})
	}

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

// deploymentTargets resolves explicit InstanceIds plus the instances of LayerIds, all within stackID.
func (b *InMemoryBackend) deploymentTargets(stackID string, opts DeploymentOptions) ([]string, error) {
	var out []string

	add := func(id string) {
		if !slices.Contains(out, id) {
			out = append(out, id)
		}
	}

	for _, id := range opts.InstanceIDs {
		i, ok := b.instances.Get(id)
		if !ok {
			return nil, ErrInstanceNotFound
		}

		if i.StackID != stackID {
			return nil, ErrValidation
		}

		add(id)
	}

	for _, layerID := range opts.LayerIDs {
		l, ok := b.layers.Get(layerID)
		if !ok {
			return nil, ErrLayerNotFound
		}

		if l.StackID != stackID {
			return nil, ErrValidation
		}

		for _, i := range b.instancesByStack.Get(stackID) {
			if slices.Contains(i.LayerIDs, layerID) {
				add(i.InstanceID)
			}
		}
	}

	return out, nil
}
