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

	growthType, replicateTo, err := validateStrategyParams(
		deploymentDuration,
		bakeTime,
		growthFactor,
		growthType,
		replicateTo,
	)
	if err != nil {
		return nil, err
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
	growthType *string,
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

	gt := existing.GrowthType
	if growthType != nil {
		gt = *growthType
	}

	gt, _, err := validateStrategyParams(deploymentDuration, bakeTime, growthFactor, gt, "")
	if err != nil {
		return nil, err
	}

	updated := *existing
	updated.GrowthType = gt

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

// deploymentStrategyOutput is the real API response shape for a deployment
// strategy -- types.DeploymentStrategy (aws-sdk-go-v2/service/appconfig@v1.48.4
// types/types.go, checked 2026-08-13) has DeploymentDurationInMinutes/
// Description/FinalBakeTimeInMinutes/GrowthFactor/GrowthType/Id/Name/
// ReplicateTo only, no CreatedAt/UpdatedAt. DeploymentStrategy itself keeps
// those two fields (with JSON tags) for Snapshot/Restore; this converter
// strips them for the wire.
type deploymentStrategyOutput struct {
	ID                          string  `json:"Id"`
	Name                        string  `json:"Name"`
	Description                 string  `json:"Description,omitempty"`
	GrowthType                  string  `json:"GrowthType"`
	ReplicateTo                 string  `json:"ReplicateTo"`
	DeploymentDurationInMinutes int32   `json:"DeploymentDurationInMinutes"`
	GrowthFactor                float32 `json:"GrowthFactor"`
	FinalBakeTimeInMinutes      int32   `json:"FinalBakeTimeInMinutes"`
}

func deploymentStrategyToOutput(d DeploymentStrategy) deploymentStrategyOutput {
	return deploymentStrategyOutput{
		ID:                          d.ID,
		Name:                        d.Name,
		Description:                 d.Description,
		GrowthType:                  d.GrowthType,
		ReplicateTo:                 d.ReplicateTo,
		DeploymentDurationInMinutes: d.DeploymentDurationInMinutes,
		GrowthFactor:                d.GrowthFactor,
		FinalBakeTimeInMinutes:      d.FinalBakeTimeInMinutes,
	}
}

const (
	maxStrategyMinutes = 1440
	minGrowthFactor    = 1
	maxGrowthFactor    = 100
	growthTypeLinear   = "LINEAR"
	growthTypeExp      = "EXPONENTIAL"
	replicateNone      = "NONE"
	replicateSSMDoc    = "SSM_DOCUMENT"
)

// validateStrategyParams range-checks a deployment strategy and returns GrowthType/ReplicateTo with defaults applied.
func validateStrategyParams(
	duration, bake int32,
	growthFactor float32,
	growthType, replicateTo string,
) (string, string, error) {
	switch {
	case duration < 0 || duration > maxStrategyMinutes:
		return "", "", fmt.Errorf("%w: DeploymentDurationInMinutes must be 0-%d", ErrBadRequest, maxStrategyMinutes)
	case bake < 0 || bake > maxStrategyMinutes:
		return "", "", fmt.Errorf("%w: FinalBakeTimeInMinutes must be 0-%d", ErrBadRequest, maxStrategyMinutes)
	case growthFactor < minGrowthFactor || growthFactor > maxGrowthFactor:
		return "", "", fmt.Errorf(
			"%w: GrowthFactor must be between %d and %d",
			ErrBadRequest,
			minGrowthFactor,
			maxGrowthFactor,
		)
	}

	switch growthType {
	case "":
		growthType = growthTypeLinear
	case growthTypeLinear, growthTypeExp:
	default:
		return "", "", fmt.Errorf("%w: GrowthType must be LINEAR or EXPONENTIAL", ErrBadRequest)
	}

	switch replicateTo {
	case "":
		replicateTo = replicateNone
	case replicateNone, replicateSSMDoc:
	default:
		return "", "", fmt.Errorf("%w: ReplicateTo must be NONE or SSM_DOCUMENT", ErrBadRequest)
	}

	return growthType, replicateTo, nil
}
