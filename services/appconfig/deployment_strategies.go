package appconfig

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

// CreateDeploymentStrategy creates a new deployment strategy. See
// CreateExperimentDefinition's doc comment for why tags are applied
// directly to b.tags rather than via TagResource.
func (b *InMemoryBackend) CreateDeploymentStrategy(
	name, description string,
	deploymentDuration, bakeTime int32,
	growthFactor float32,
	growthType, replicateTo string,
	tags map[string]string,
) (*DeploymentStrategy, error) {
	b.mu.Lock("CreateDeploymentStrategy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	if len(b.deploymentStrategiesByName.Get(name)) > 0 {
		return nil, fmt.Errorf(
			"%w: deployment strategy with name %q already exists",
			ErrConflict,
			name,
		)
	}

	now := time.Now()
	strategy := &DeploymentStrategy{
		ID:                          newResourceID(),
		Name:                        name,
		Description:                 description,
		DeploymentDurationInMinutes: deploymentDuration,
		FinalBakeTimeInMinutes:      bakeTime,
		GrowthFactor:                growthFactor,
		GrowthType:                  growthType,
		ReplicateTo:                 replicateTo,
		CreatedAt:                   now,
		UpdatedAt:                   now,
	}
	b.deploymentStrategies.Put(strategy)

	if len(tags) > 0 {
		b.tags[b.appconfigARN("deploymentstrategy/"+strategy.ID)] = maps.Clone(tags)
	}

	cp := *strategy

	return &cp, nil
}

// GetDeploymentStrategy retrieves a deployment strategy by ID.
func (b *InMemoryBackend) GetDeploymentStrategy(strategyID string) (*DeploymentStrategy, error) {
	b.mu.RLock("GetDeploymentStrategy")
	defer b.mu.RUnlock()

	strategy, ok := b.deploymentStrategies.Get(strategyID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: deployment strategy %s",
			ErrDeploymentStrategyNotFound,
			strategyID,
		)
	}

	cp := *strategy

	return &cp, nil
}

// ListDeploymentStrategies returns paginated deployment strategies.
func (b *InMemoryBackend) ListDeploymentStrategies(
	nextToken string,
	maxResults int,
) ([]DeploymentStrategy, string) {
	b.mu.RLock("ListDeploymentStrategies")
	defer b.mu.RUnlock()

	all := b.deploymentStrategies.All()
	out := make([]DeploymentStrategy, 0, len(all))
	for _, s := range all {
		out = append(out, *s)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token
}

// UpdateDeploymentStrategy updates a deployment strategy. A nil description
// means the request omitted that field, and AWS AppConfig leaves an omitted
// field unchanged rather than clearing it.
func (b *InMemoryBackend) UpdateDeploymentStrategy(
	strategyID, name string,
	description *string,
	deploymentDuration, bakeTime int32,
	growthFactor float32,
) (*DeploymentStrategy, error) {
	b.mu.Lock("UpdateDeploymentStrategy")
	defer b.mu.Unlock()

	existing, ok := b.deploymentStrategies.Get(strategyID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: deployment strategy %s",
			ErrDeploymentStrategyNotFound,
			strategyID,
		)
	}

	updated := *existing
	if name != "" {
		updated.Name = name
	}

	if description != nil {
		updated.Description = *description
	}

	updated.DeploymentDurationInMinutes = deploymentDuration
	updated.FinalBakeTimeInMinutes = bakeTime
	updated.GrowthFactor = growthFactor
	updated.UpdatedAt = time.Now()
	b.deploymentStrategies.Put(&updated)
	cp := updated

	return &cp, nil
}

// DeleteDeploymentStrategy deletes a deployment strategy.
func (b *InMemoryBackend) DeleteDeploymentStrategy(strategyID string) error {
	b.mu.Lock("DeleteDeploymentStrategy")
	defer b.mu.Unlock()

	if !b.deploymentStrategies.Has(strategyID) {
		return fmt.Errorf("%w: deployment strategy %s", ErrDeploymentStrategyNotFound, strategyID)
	}

	b.deploymentStrategies.Delete(strategyID)
	delete(b.tags, b.appconfigARN("deploymentstrategy/"+strategyID))

	return nil
}
