package rds

import "fmt"

// CreateBlueGreenDeployment creates a new Blue/Green Deployment.
func (b *InMemoryBackend) CreateBlueGreenDeployment(
	name, source string,
) (*BlueGreenDeployment, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: BlueGreenDeploymentName must not be empty", ErrInvalidParameter)
	}
	if source == "" {
		return nil, fmt.Errorf("%w: Source must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("CreateBlueGreenDeployment")
	defer b.mu.Unlock()

	id := "bgd-" + name

	if _, exists := b.blueGreenDeployments.Get(id); exists {
		return nil, fmt.Errorf(
			"%w: Blue/Green Deployment %s already exists",
			ErrBlueGreenDeploymentAlreadyExists,
			name,
		)
	}

	target := source + "-green"
	deployment := &BlueGreenDeployment{
		BlueGreenDeploymentIdentifier: id,
		BlueGreenDeploymentName:       name,
		Source:                        source,
		Target:                        target,
		Status:                        blueGreenDeploymentStatusAvailable,
	}
	b.blueGreenDeployments.Put(deployment)

	cp := *deployment

	return &cp, nil
}

// DescribeBlueGreenDeployments returns blue/green deployments, optionally by ID.
func (b *InMemoryBackend) DescribeBlueGreenDeployments(id string) ([]BlueGreenDeployment, error) {
	b.mu.RLock("DescribeBlueGreenDeployments")
	defer b.mu.RUnlock()
	if id != "" {
		d, exists := b.blueGreenDeployments.Get(id)
		if !exists {
			return nil, fmt.Errorf("%w: blue/green deployment %s not found", ErrBlueGreenDeploymentNotFound, id)
		}
		cp := *d

		return []BlueGreenDeployment{cp}, nil
	}
	result := make([]BlueGreenDeployment, 0, b.blueGreenDeployments.Len())
	for _, d := range b.blueGreenDeployments.All() {
		result = append(result, *d)
	}

	return result, nil
}

// DeleteBlueGreenDeployment deletes the named blue/green deployment.
func (b *InMemoryBackend) DeleteBlueGreenDeployment(id string) (*BlueGreenDeployment, error) {
	b.mu.Lock("DeleteBlueGreenDeployment")
	defer b.mu.Unlock()
	d, exists := b.blueGreenDeployments.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: blue/green deployment %s not found", ErrBlueGreenDeploymentNotFound, id)
	}
	cp := *d
	b.blueGreenDeployments.Delete(id)

	return &cp, nil
}

// SwitchoverBlueGreenDeployment switches over a blue/green deployment.
func (b *InMemoryBackend) SwitchoverBlueGreenDeployment(id string) (*BlueGreenDeployment, error) {
	b.mu.Lock("SwitchoverBlueGreenDeployment")
	defer b.mu.Unlock()
	d, exists := b.blueGreenDeployments.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: blue/green deployment %s not found", ErrBlueGreenDeploymentNotFound, id)
	}
	d.Status = "switchover-completed"
	cp := *d

	return &cp, nil
}
