package sagemaker

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrModelNotFound is returned when a model does not exist.
	ErrModelNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrModelAlreadyExists is returned when a model already exists.
	ErrModelAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrEndpointConfigNotFound is returned when an endpoint config does not exist.
	ErrEndpointConfigNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrEndpointConfigAlreadyExists is returned when an endpoint config already exists.
	ErrEndpointConfigAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// ContainerDefinition holds image details for a model container.
type ContainerDefinition struct {
	Image        string            `json:"Image,omitempty"`
	Environment  map[string]string `json:"Environment,omitempty"`
	ModelDataURL string            `json:"ModelDataUrl,omitempty"`
}

// Model represents a SageMaker model.
type Model struct {
	CreationTime     time.Time
	Tags             map[string]string
	ModelName        string
	ModelARN         string
	ExecutionRoleARN string
	PrimaryContainer *ContainerDefinition
	Containers       []ContainerDefinition
}

// cloneContainer returns a deep copy of a ContainerDefinition, including its Environment map.
func cloneContainer(c ContainerDefinition) ContainerDefinition {
	c.Environment = maps.Clone(c.Environment)

	return c
}

// cloneModel returns a deep copy of m, including nested maps and slices.
func cloneModel(m *Model) *Model {
	cp := *m
	cp.Tags = maps.Clone(m.Tags)

	if m.PrimaryContainer != nil {
		pc := cloneContainer(*m.PrimaryContainer)
		cp.PrimaryContainer = &pc
	}

	cp.Containers = make([]ContainerDefinition, len(m.Containers))

	for i, c := range m.Containers {
		cp.Containers[i] = cloneContainer(c)
	}

	return &cp
}

// ProductionVariant holds configuration for a production variant in an endpoint config.
type ProductionVariant struct {
	VariantName          string  `json:"VariantName"`
	ModelName            string  `json:"ModelName"`
	InstanceType         string  `json:"InstanceType,omitempty"`
	InitialInstanceCount int32   `json:"InitialInstanceCount,omitempty"`
	InitialVariantWeight float64 `json:"InitialVariantWeight,omitempty"`
}

// EndpointConfig represents a SageMaker endpoint configuration.
type EndpointConfig struct {
	CreationTime       time.Time
	Tags               map[string]string
	EndpointConfigName string
	EndpointConfigARN  string
	ProductionVariants []ProductionVariant
}

// cloneEndpointConfig returns a deep copy of ec.
func cloneEndpointConfig(ec *EndpointConfig) *EndpointConfig {
	cp := *ec
	cp.Tags = maps.Clone(ec.Tags)
	cp.ProductionVariants = make([]ProductionVariant, len(ec.ProductionVariants))
	copy(cp.ProductionVariants, ec.ProductionVariants)

	return &cp
}

// InMemoryBackend is an in-memory store for SageMaker resources.
type InMemoryBackend struct {
	models          map[string]*Model
	endpointConfigs map[string]*EndpointConfig
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
}

// NewInMemoryBackend creates a new in-memory SageMaker backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		models:          make(map[string]*Model),
		endpointConfigs: make(map[string]*EndpointConfig),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("sagemaker"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateModel creates a new SageMaker model.
func (b *InMemoryBackend) CreateModel(
	name string,
	executionRoleARN string,
	primaryContainer *ContainerDefinition,
	containers []ContainerDefinition,
	tags map[string]string,
) (*Model, error) {
	b.mu.Lock("CreateModel")
	defer b.mu.Unlock()

	if _, ok := b.models[name]; ok {
		return nil, fmt.Errorf("%w: model %s already exists", ErrModelAlreadyExists, name)
	}

	modelARN := arn.Build("sagemaker", b.region, b.accountID, "model/"+name)

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
	b.models[name] = m

	return cloneModel(m), nil
}

// DescribeModel returns a model by name.
func (b *InMemoryBackend) DescribeModel(name string) (*Model, error) {
	b.mu.RLock("DescribeModel")
	defer b.mu.RUnlock()

	m, ok := b.models[name]
	if !ok {
		return nil, fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	return cloneModel(m), nil
}

// ListModels returns all models sorted by name.
func (b *InMemoryBackend) ListModels() []*Model {
	b.mu.RLock("ListModels")
	defer b.mu.RUnlock()

	list := make([]*Model, 0, len(b.models))

	for _, m := range b.models {
		list = append(list, cloneModel(m))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ModelName < list[j].ModelName
	})

	return list
}

// DeleteModel deletes a model by name.
func (b *InMemoryBackend) DeleteModel(name string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()

	if _, ok := b.models[name]; !ok {
		return fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	delete(b.models, name)

	return nil
}

// CreateEndpointConfig creates a new SageMaker endpoint configuration.
func (b *InMemoryBackend) CreateEndpointConfig(
	name string,
	productionVariants []ProductionVariant,
	tags map[string]string,
) (*EndpointConfig, error) {
	b.mu.Lock("CreateEndpointConfig")
	defer b.mu.Unlock()

	if _, ok := b.endpointConfigs[name]; ok {
		return nil, fmt.Errorf("%w: endpoint config %s already exists", ErrEndpointConfigAlreadyExists, name)
	}

	configARN := arn.Build("sagemaker", b.region, b.accountID, "endpoint-config/"+name)

	storedVariants := make([]ProductionVariant, len(productionVariants))
	copy(storedVariants, productionVariants)

	ec := &EndpointConfig{
		EndpointConfigName: name,
		EndpointConfigARN:  configARN,
		ProductionVariants: storedVariants,
		CreationTime:       time.Now(),
		Tags:               mergeTags(nil, tags),
	}
	b.endpointConfigs[name] = ec

	return cloneEndpointConfig(ec), nil
}

// DescribeEndpointConfig returns an endpoint config by name.
func (b *InMemoryBackend) DescribeEndpointConfig(name string) (*EndpointConfig, error) {
	b.mu.RLock("DescribeEndpointConfig")
	defer b.mu.RUnlock()

	ec, ok := b.endpointConfigs[name]
	if !ok {
		return nil, fmt.Errorf("%w: could not find endpoint configuration %q", ErrEndpointConfigNotFound, name)
	}

	return cloneEndpointConfig(ec), nil
}

// ListEndpointConfigs returns all endpoint configurations sorted by name.
func (b *InMemoryBackend) ListEndpointConfigs() []*EndpointConfig {
	b.mu.RLock("ListEndpointConfigs")
	defer b.mu.RUnlock()

	list := make([]*EndpointConfig, 0, len(b.endpointConfigs))

	for _, ec := range b.endpointConfigs {
		list = append(list, cloneEndpointConfig(ec))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].EndpointConfigName < list[j].EndpointConfigName
	})

	return list
}

// DeleteEndpointConfig deletes an endpoint configuration by name.
func (b *InMemoryBackend) DeleteEndpointConfig(name string) error {
	b.mu.Lock("DeleteEndpointConfig")
	defer b.mu.Unlock()

	if _, ok := b.endpointConfigs[name]; !ok {
		return fmt.Errorf("%w: could not find endpoint configuration %q", ErrEndpointConfigNotFound, name)
	}

	delete(b.endpointConfigs, name)

	return nil
}

// AddTags adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) AddTags(resourceARN string, tags map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	for _, m := range b.models {
		if m.ModelARN == resourceARN {
			m.Tags = mergeTags(m.Tags, tags)

			return nil
		}
	}

	for _, ec := range b.endpointConfigs {
		if ec.EndpointConfigARN == resourceARN {
			ec.Tags = mergeTags(ec.Tags, tags)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrModelNotFound, resourceARN)
}

// ListTags returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	for _, m := range b.models {
		if m.ModelARN == resourceARN {
			result := make(map[string]string, len(m.Tags))
			maps.Copy(result, m.Tags)

			return result, nil
		}
	}

	for _, ec := range b.endpointConfigs {
		if ec.EndpointConfigARN == resourceARN {
			result := make(map[string]string, len(ec.Tags))
			maps.Copy(result, ec.Tags)

			return result, nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrModelNotFound, resourceARN)
}

// DeleteTags removes tag keys from a resource identified by ARN.
func (b *InMemoryBackend) DeleteTags(resourceARN string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	keySet := make(map[string]struct{}, len(tagKeys))

	for _, k := range tagKeys {
		keySet[k] = struct{}{}
	}

	for _, m := range b.models {
		if m.ModelARN == resourceARN {
			for k := range keySet {
				delete(m.Tags, k)
			}

			return nil
		}
	}

	for _, ec := range b.endpointConfigs {
		if ec.EndpointConfigARN == resourceARN {
			for k := range keySet {
				delete(ec.Tags, k)
			}

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrModelNotFound, resourceARN)
}

// mergeTags merges new tags into existing ones, returning a new map.
func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}
