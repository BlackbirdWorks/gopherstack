package sagemaker

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrModelNotFound is returned when a model does not exist.
	ErrModelNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrModelAlreadyExists is returned when a model already exists.
	ErrModelAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// CreateModel creates a new SageMaker model.
func (b *InMemoryBackend) CreateModel(
	ctx context.Context,
	name string,
	executionRoleARN string,
	primaryContainer *ContainerDefinition,
	containers []ContainerDefinition,
	tags map[string]string,
) (*Model, error) {
	b.mu.Lock("CreateModel")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	models := b.modelsStore(region)

	if _, ok := models.Get(name); ok {
		return nil, fmt.Errorf("%w: model %s already exists", ErrModelAlreadyExists, name)
	}

	modelARN := arn.Build("sagemaker", region, b.accountID, "model/"+name)

	var storedPrimaryContainer *ContainerDefinition

	if primaryContainer != nil {
		pc := cloneContainer(*primaryContainer)
		storedPrimaryContainer = &pc
	}

	storedContainers := make([]ContainerDefinition, len(containers))

	for i, c := range containers {
		storedContainers[i] = cloneContainer(c)
	}

	m := &Model{
		ModelName:        name,
		ModelARN:         modelARN,
		ExecutionRoleARN: executionRoleARN,
		PrimaryContainer: storedPrimaryContainer,
		Containers:       storedContainers,
		CreationTime:     time.Now(),
		Tags:             mergeTags(nil, tags),
	}
	models.Put(m)
	b.modelARNIndexStore(region)[modelARN] = name

	return cloneModel(m), nil
}

// DescribeModel returns a model by name.
func (b *InMemoryBackend) DescribeModel(ctx context.Context, name string) (*Model, error) {
	b.mu.RLock("DescribeModel")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	m, ok := b.modelsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	return cloneModel(m), nil
}

// ListModels returns models sorted by name, with optional pagination.
func (b *InMemoryBackend) ListModels(ctx context.Context, nextToken string) ([]*Model, string) {
	b.mu.RLock("ListModels")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.modelsStoreRO(region), nextToken, cloneModel,
		func(a, b *Model) bool { return a.ModelName < b.ModelName })
}

// DeleteModel deletes a model by name.
func (b *InMemoryBackend) DeleteModel(ctx context.Context, name string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	models := b.modelsStore(region)

	m, ok := models.Get(name)
	if !ok {
		return fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	arnIndex := b.modelARNIndexStore(region)
	delete(arnIndex, m.ModelARN)
	models.Delete(name)

	return nil
}

// SetModelExtras sets optional fields on an existing model that were not included
// in the original CreateModel signature (VpcConfig, EnableNetworkIsolation, InferenceExecutionConfig).
func (b *InMemoryBackend) SetModelExtras(
	ctx context.Context,
	name string,
	vpcConfig *VpcConfig,
	enableNetworkIsolation bool,
	inferenceExecConfig *InferenceExecutionConfig,
) error {
	b.mu.Lock("SetModelExtras")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	m, ok := b.modelsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	if vpcConfig != nil {
		vpc := *vpcConfig
		vpc.SecurityGroupIDs = append([]string(nil), vpcConfig.SecurityGroupIDs...)
		vpc.Subnets = append([]string(nil), vpcConfig.Subnets...)
		m.VpcConfig = &vpc
	}

	m.EnableNetworkIsolation = enableNetworkIsolation

	if inferenceExecConfig != nil {
		iec := *inferenceExecConfig
		m.InferenceExecutionConfig = &iec
	}

	return nil
}
