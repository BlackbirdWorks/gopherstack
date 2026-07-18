package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// newCustomModelDeployID generates a unique custom model deployment ID.
func (b *InMemoryBackend) newCustomModelDeployID() string {
	b.customModelDeployCounter++

	return fmt.Sprintf("cmd-%07d", b.customModelDeployCounter)
}

// CreateCustomModelDeployment creates a new deployment for a custom model.
func (b *InMemoryBackend) CreateCustomModelDeployment(
	modelARN, deploymentName string,
	tags []Tag,
) (*CustomModelDeployment, error) {
	b.mu.Lock("CreateCustomModelDeployment")
	defer b.mu.Unlock()

	if modelARN == "" {
		return nil, fmt.Errorf("%w: modelArn is required", ErrValidation)
	}

	if deploymentName == "" {
		return nil, fmt.Errorf("%w: modelDeploymentName is required", ErrValidation)
	}

	if _, exists := b.customModelDeployByName[deploymentName]; exists {
		return nil, fmt.Errorf(
			"%w: custom model deployment %s already exists",
			ErrAlreadyExists,
			deploymentName,
		)
	}

	id := b.newCustomModelDeployID()
	deploymentARN := arn.Build("bedrock", b.region, b.accountID, "custom-model-deployment/"+id)
	now := time.Now().UTC()

	deployment := &CustomModelDeployment{
		CustomModelDeploymentArn: deploymentARN,
		ModelDeploymentName:      deploymentName,
		ModelArn:                 modelARN,
		Status:                   statusCreating,
		CreationTime:             now,
		LastModifiedTime:         now,
		Tags:                     copyTags(tags),
	}
	b.customModelDeployments.Put(deployment)
	b.customModelDeployByName[deploymentName] = deploymentARN
	cp := *deployment
	cp.Tags = copyTags(deployment.Tags)

	return &cp, nil
}

// GetCustomModelDeployment returns a deployment by ARN.
func (b *InMemoryBackend) GetCustomModelDeployment(deployARN string) (*CustomModelDeployment, error) {
	b.mu.RLock("GetCustomModelDeployment")
	defer b.mu.RUnlock()

	d, ok := b.customModelDeployments.Get(deployARN)
	if !ok {
		return nil, fmt.Errorf("%w: custom model deployment %s not found", ErrNotFound, deployARN)
	}

	cp := *d
	cp.Tags = copyTags(d.Tags)

	return &cp, nil
}

// ListCustomModelDeployments returns all deployments.
func (b *InMemoryBackend) ListCustomModelDeployments() []*CustomModelDeployment {
	b.mu.RLock("ListCustomModelDeployments")
	defer b.mu.RUnlock()

	deployments := make([]*CustomModelDeployment, 0, b.customModelDeployments.Len())
	for _, d := range b.customModelDeployments.All() {
		cp := *d
		cp.Tags = copyTags(d.Tags)
		deployments = append(deployments, &cp)
	}

	sort.Slice(deployments, func(i, k int) bool {
		return deployments[i].CreationTime.Before(deployments[k].CreationTime)
	})

	return deployments
}

// UpdateCustomModelDeployment updates mutable fields of a deployment.
func (b *InMemoryBackend) UpdateCustomModelDeployment(deployARN string) (*CustomModelDeployment, error) {
	b.mu.Lock("UpdateCustomModelDeployment")
	defer b.mu.Unlock()

	d, ok := b.customModelDeployments.Get(deployARN)
	if !ok {
		return nil, fmt.Errorf("%w: custom model deployment %s not found", ErrNotFound, deployARN)
	}

	d.LastModifiedTime = time.Now().UTC()

	cp := *d
	cp.Tags = copyTags(d.Tags)

	return &cp, nil
}

// DeleteCustomModelDeployment removes a deployment.
func (b *InMemoryBackend) DeleteCustomModelDeployment(deployARN string) error {
	b.mu.Lock("DeleteCustomModelDeployment")
	defer b.mu.Unlock()

	d, ok := b.customModelDeployments.Get(deployARN)
	if !ok {
		return fmt.Errorf("%w: custom model deployment %s not found", ErrNotFound, deployARN)
	}

	delete(b.customModelDeployByName, d.ModelDeploymentName)
	b.customModelDeployments.Delete(deployARN)

	return nil
}
