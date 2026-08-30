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

// ListCustomModelDeployments returns deployments matching in's filters,
// sorted and paginated. in may be nil, matching an unfiltered call.
// Structurally similar to ListModelCopyJobs/ListModelImportJobs/
// ListProvisionedModelThroughputs (same filter/sort/paginate shape) but over
// a distinct resource type and filter set; see
// matchesCustomModelDeploymentFilter.
//
//nolint:dupl // see doc comment above.
func (b *InMemoryBackend) ListCustomModelDeployments(
	in *ListCustomModelDeploymentsInput,
) ([]*CustomModelDeployment, string) {
	b.mu.RLock("ListCustomModelDeployments")
	defer b.mu.RUnlock()

	deployments := make([]*CustomModelDeployment, 0, b.customModelDeployments.Len())
	for _, d := range b.customModelDeployments.All() {
		if !matchesCustomModelDeploymentFilter(d, in) {
			continue
		}

		cp := *d
		cp.Tags = copyTags(d.Tags)
		deployments = append(deployments, &cp)
	}

	descending := in != nil && in.SortOrder == sortOrderDescending
	sort.Slice(deployments, func(i, k int) bool {
		if descending {
			return deployments[i].CreationTime.After(deployments[k].CreationTime)
		}

		return deployments[i].CreationTime.Before(deployments[k].CreationTime)
	})

	if in == nil {
		deployments, _ = paginate(deployments, 0, "")

		return deployments, ""
	}

	return paginate(deployments, int(in.MaxResults), in.NextToken)
}

// matchesCustomModelDeploymentFilter reports whether a custom model
// deployment satisfies the list filters (statusEquals, modelArnEquals,
// nameContains, createdAfter/Before).
func matchesCustomModelDeploymentFilter(
	d *CustomModelDeployment, in *ListCustomModelDeploymentsInput,
) bool {
	if in == nil {
		return true
	}
	if in.StatusEquals != "" && d.Status != in.StatusEquals {
		return false
	}
	if in.ModelArnEquals != "" && d.ModelArn != in.ModelArnEquals {
		return false
	}
	if in.NameContains != "" && !containsIgnoreCase(d.ModelDeploymentName, in.NameContains) {
		return false
	}
	if in.CreatedAfter != nil && !d.CreationTime.After(*in.CreatedAfter) {
		return false
	}
	if in.CreatedBefore != nil && !d.CreationTime.Before(*in.CreatedBefore) {
		return false
	}

	return true
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

// AdvanceCustomModelDeploymentStatuses transitions deployments from Creating
// to Active. Called by the janitor (janitor.go), the same shape as
// AdvanceProvisionedModelThroughputStatuses -- CreateCustomModelDeployment
// stamped Status Creating and nothing else in this backend ever advanced it.
func (b *InMemoryBackend) AdvanceCustomModelDeploymentStatuses() int {
	b.mu.Lock("AdvanceCustomModelDeploymentStatuses")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	advanced := 0

	for _, d := range b.customModelDeployments.All() {
		if d.Status != statusCreating {
			continue
		}

		d.Status = statusActive
		d.LastModifiedTime = now
		advanced++
	}

	return advanced
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
