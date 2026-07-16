package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrFlowDefinitionNotFound is returned when a flow definition does not exist.
var ErrFlowDefinitionNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// FlowDefinition
// ---------------------------------------------------------------------------

// FlowDefinition represents a SageMaker Augmented AI flow definition.
type FlowDefinition struct {
	CreationTime         time.Time         `json:"CreationTime"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	FlowDefinitionName   string            `json:"FlowDefinitionName"`
	FlowDefinitionArn    string            `json:"FlowDefinitionArn"`
	FlowDefinitionStatus string            `json:"FlowDefinitionStatus"`
	RoleArn              string            `json:"RoleArn,omitempty"`
}

func cloneFlowDefinition(f *FlowDefinition) *FlowDefinition {
	cp := *f
	cp.Tags = maps.Clone(f.Tags)

	return &cp
}

// CreateFlowDefinition creates a flow definition.
func (b *InMemoryBackend) CreateFlowDefinition(
	ctx context.Context,
	name, roleArn string,
	tags map[string]string,
) (*FlowDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateFlowDefinition")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", ErrValidation)
	}

	store := b.flowDefinitionsStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: flow definition %q already exists", ErrValidation, name)
	}

	flowARN := arn.Build("sagemaker", region, b.accountID, "flow-definition/"+name)

	f := &FlowDefinition{
		FlowDefinitionName:   name,
		FlowDefinitionArn:    flowARN,
		FlowDefinitionStatus: statusActive,
		RoleArn:              roleArn,
		Tags:                 mergeTags(nil, tags),
		CreationTime:         time.Now(),
	}
	store.Put(f)

	return cloneFlowDefinition(f), nil
}

// DescribeFlowDefinition returns a flow definition by name.
func (b *InMemoryBackend) DescribeFlowDefinition(ctx context.Context, name string) (*FlowDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeFlowDefinition")
	defer b.mu.RUnlock()

	f, ok := b.flowDefinitionsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: flow definition %q not found", ErrFlowDefinitionNotFound, name)
	}

	return cloneFlowDefinition(f), nil
}

// DeleteFlowDefinition removes a flow definition by name.
func (b *InMemoryBackend) DeleteFlowDefinition(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteFlowDefinition")
	defer b.mu.Unlock()

	store := b.flowDefinitionsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: flow definition %q not found", ErrFlowDefinitionNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ListFlowDefinitions returns all flow definitions.
func (b *InMemoryBackend) ListFlowDefinitions(ctx context.Context, nextToken string) ([]*FlowDefinition, string) {
	b.mu.RLock("ListFlowDefinitions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.flowDefinitionsStoreRO(region),
		nextToken,
		cloneFlowDefinition,
		func(v *FlowDefinition) string { return v.FlowDefinitionName },
	)
}
