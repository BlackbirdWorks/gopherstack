package mediastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrContainerNotFound is returned when a container does not exist.
	ErrContainerNotFound = awserr.New("ResourceNotFoundException: container not found", awserr.ErrNotFound)
	// ErrContainerAlreadyExists is returned when a container already exists.
	ErrContainerAlreadyExists = awserr.New(
		"ContainerInUseException: container already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrPolicyNotFound is returned when no container policy has been set.
	ErrPolicyNotFound = awserr.New("PolicyNotFoundException: no policy found for container", awserr.ErrNotFound)
	// ErrCorsPolicyNotFound is returned when no CORS policy has been set.
	ErrCorsPolicyNotFound = awserr.New(
		"CorsPolicyNotFoundException: no CORS policy found for container",
		awserr.ErrNotFound,
	)
	// ErrLifecyclePolicyNotFound is returned when no lifecycle policy has been set.
	ErrLifecyclePolicyNotFound = awserr.New(
		"PolicyNotFoundException: no lifecycle policy found for container",
		awserr.ErrNotFound,
	)
	// ErrMetricPolicyNotFound is returned when no metric policy has been set.
	ErrMetricPolicyNotFound = awserr.New(
		"PolicyNotFoundException: no metric policy found for container",
		awserr.ErrNotFound,
	)
	// ErrMissingContainerName is returned when the container name is missing.
	ErrMissingContainerName = errors.New("ContainerName is required")
)

const (
	// containerStatusActive is the status for a ready container.
	containerStatusActive = "ACTIVE"
	// defaultEndpointFormat is the format for the container endpoint.
	defaultEndpointFormat = "https://%s.data.mediastore.%s.amazonaws.com"
)

// StorageBackend is the interface for the MediaStore in-memory backend.
type StorageBackend interface {
	CreateContainer(region, accountID, name string, tags map[string]string) (*Container, error)
	DeleteContainer(name string) error
	DescribeContainer(name string) (*Container, error)
	ListContainers() ([]*Container, error)
	PutContainerPolicy(name, policy string) error
	GetContainerPolicy(name string) (string, error)
	DeleteContainerPolicy(name string) error
	PutCorsPolicy(name string, rules []CorsRule) error
	GetCorsPolicy(name string) ([]CorsRule, error)
	DeleteCorsPolicy(name string) error
	PutLifecyclePolicy(name, policy string) error
	GetLifecyclePolicy(name string) (string, error)
	DeleteLifecyclePolicy(name string) error
	PutMetricPolicy(name string, policy MetricPolicy) error
	GetMetricPolicy(name string) (MetricPolicy, error)
	DeleteMetricPolicy(name string) error
	StartAccessLogging(name string) error
	StopAccessLogging(name string) error
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	containers map[string]*Container
	mu         *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new in-memory MediaStore backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		containers: make(map[string]*Container),
		mu:         lockmetrics.New("mediastore"),
	}
}

// containerARN builds the ARN for a MediaStore container.
func containerARN(region, accountID, name string) string {
	return arn.Build("mediastore", region, accountID, fmt.Sprintf("container/%s", name))
}

// containerEndpoint returns the data plane endpoint for a container.
func containerEndpoint(name, region string) string {
	return fmt.Sprintf(defaultEndpointFormat, name, region)
}

// CreateContainer creates a new MediaStore container.
func (b *InMemoryBackend) CreateContainer(region, accountID, name string, tags map[string]string) (*Container, error) {
	b.mu.Lock("CreateContainer")
	defer b.mu.Unlock()

	if _, exists := b.containers[name]; exists {
		return nil, ErrContainerAlreadyExists
	}

	now := time.Now().UTC()

	t := make(map[string]string)
	maps.Copy(t, tags)

	c := &Container{
		Name:         name,
		ARN:          containerARN(region, accountID, name),
		Endpoint:     containerEndpoint(name, region),
		Status:       containerStatusActive,
		CreationTime: &now,
		Tags:         t,
	}

	b.containers[name] = c

	return c, nil
}

// DeleteContainer removes a container.
func (b *InMemoryBackend) DeleteContainer(name string) error {
	b.mu.Lock("DeleteContainer")
	defer b.mu.Unlock()

	if _, exists := b.containers[name]; !exists {
		return ErrContainerNotFound
	}

	delete(b.containers, name)

	return nil
}

// DescribeContainer returns details about a container.
func (b *InMemoryBackend) DescribeContainer(name string) (*Container, error) {
	b.mu.RLock("DescribeContainer")
	defer b.mu.RUnlock()

	c, exists := b.containers[name]
	if !exists {
		return nil, ErrContainerNotFound
	}

	return copyContainer(c), nil
}

// ListContainers returns all containers sorted by name.
func (b *InMemoryBackend) ListContainers() ([]*Container, error) {
	b.mu.RLock("ListContainers")
	defer b.mu.RUnlock()

	all := make([]*Container, 0, len(b.containers))

	for _, c := range b.containers {
		all = append(all, copyContainer(c))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	return all, nil
}

// PutContainerPolicy stores a container access policy.
func (b *InMemoryBackend) PutContainerPolicy(name, policy string) error {
	b.mu.Lock("PutContainerPolicy")
	defer b.mu.Unlock()

	c, exists := b.containers[name]
	if !exists {
		return ErrContainerNotFound
	}

	c.ContainerPolicy = policy

	return nil
}

// GetContainerPolicy retrieves the container access policy.
func (b *InMemoryBackend) GetContainerPolicy(name string) (string, error) {
	b.mu.RLock("GetContainerPolicy")
	defer b.mu.RUnlock()

	c, exists := b.containers[name]
	if !exists {
		return "", ErrContainerNotFound
	}

	if c.ContainerPolicy == "" {
		return "", ErrPolicyNotFound
	}

	return c.ContainerPolicy, nil
}

// DeleteContainerPolicy removes the container access policy.
func (b *InMemoryBackend) DeleteContainerPolicy(name string) error {
	b.mu.Lock("DeleteContainerPolicy")
	defer b.mu.Unlock()

	c, exists := b.containers[name]
	if !exists {
		return ErrContainerNotFound
	}

	c.ContainerPolicy = ""

	return nil
}

// PutCorsPolicy stores a CORS policy for a container.
func (b *InMemoryBackend) PutCorsPolicy(name string, rules []CorsRule) error {
	b.mu.Lock("PutCorsPolicy")
	defer b.mu.Unlock()

	c, exists := b.containers[name]
	if !exists {
		return ErrContainerNotFound
	}

	data, err := json.Marshal(rules)
	if err != nil {
		return fmt.Errorf("marshal cors rules: %w", err)
	}

	c.CorsPolicy = string(data)

	return nil
}

// GetCorsPolicy retrieves the CORS policy for a container.
func (b *InMemoryBackend) GetCorsPolicy(name string) ([]CorsRule, error) {
	b.mu.RLock("GetCorsPolicy")
	defer b.mu.RUnlock()

	c, exists := b.containers[name]
	if !exists {
		return nil, ErrContainerNotFound
	}

	if c.CorsPolicy == "" {
		return nil, ErrCorsPolicyNotFound
	}

	var rules []CorsRule
	if err := json.Unmarshal([]byte(c.CorsPolicy), &rules); err != nil {
		return nil, fmt.Errorf("unmarshal cors rules: %w", err)
	}

	return rules, nil
}

// DeleteCorsPolicy removes the CORS policy from a container.
func (b *InMemoryBackend) DeleteCorsPolicy(name string) error {
	b.mu.Lock("DeleteCorsPolicy")
	defer b.mu.Unlock()

	c, exists := b.containers[name]
	if !exists {
		return ErrContainerNotFound
	}

	c.CorsPolicy = ""

	return nil
}

// PutLifecyclePolicy stores a lifecycle policy for a container.
func (b *InMemoryBackend) PutLifecyclePolicy(name, policy string) error {
	b.mu.Lock("PutLifecyclePolicy")
	defer b.mu.Unlock()

	c, exists := b.containers[name]
	if !exists {
		return ErrContainerNotFound
	}

	c.LifecyclePolicy = policy

	return nil
}

// GetLifecyclePolicy retrieves the lifecycle policy for a container.
func (b *InMemoryBackend) GetLifecyclePolicy(name string) (string, error) {
	b.mu.RLock("GetLifecyclePolicy")
	defer b.mu.RUnlock()

	c, exists := b.containers[name]
	if !exists {
		return "", ErrContainerNotFound
	}

	if c.LifecyclePolicy == "" {
		return "", ErrLifecyclePolicyNotFound
	}

	return c.LifecyclePolicy, nil
}

// DeleteLifecyclePolicy removes the lifecycle policy from a container.
func (b *InMemoryBackend) DeleteLifecyclePolicy(name string) error {
	b.mu.Lock("DeleteLifecyclePolicy")
	defer b.mu.Unlock()

	c, exists := b.containers[name]
	if !exists {
		return ErrContainerNotFound
	}

	c.LifecyclePolicy = ""

	return nil
}

// PutMetricPolicy stores a metric policy for a container.
func (b *InMemoryBackend) PutMetricPolicy(name string, policy MetricPolicy) error {
	b.mu.Lock("PutMetricPolicy")
	defer b.mu.Unlock()

	c, exists := b.containers[name]
	if !exists {
		return ErrContainerNotFound
	}

	data, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal metric policy: %w", err)
	}

	c.MetricPolicy = string(data)

	return nil
}

// GetMetricPolicy retrieves the metric policy for a container.
func (b *InMemoryBackend) GetMetricPolicy(name string) (MetricPolicy, error) {
	b.mu.RLock("GetMetricPolicy")
	defer b.mu.RUnlock()

	c, exists := b.containers[name]
	if !exists {
		return MetricPolicy{}, ErrContainerNotFound
	}

	if c.MetricPolicy == "" {
		return MetricPolicy{}, ErrMetricPolicyNotFound
	}

	var policy MetricPolicy
	if err := json.Unmarshal([]byte(c.MetricPolicy), &policy); err != nil {
		return MetricPolicy{}, fmt.Errorf("unmarshal metric policy: %w", err)
	}

	return policy, nil
}

// DeleteMetricPolicy removes the metric policy from a container.
func (b *InMemoryBackend) DeleteMetricPolicy(name string) error {
	b.mu.Lock("DeleteMetricPolicy")
	defer b.mu.Unlock()

	c, exists := b.containers[name]
	if !exists {
		return ErrContainerNotFound
	}

	c.MetricPolicy = ""

	return nil
}

// StartAccessLogging enables access logging for a container.
func (b *InMemoryBackend) StartAccessLogging(name string) error {
	b.mu.Lock("StartAccessLogging")
	defer b.mu.Unlock()

	c, exists := b.containers[name]
	if !exists {
		return ErrContainerNotFound
	}

	c.AccessLoggingEnabled = true

	return nil
}

// StopAccessLogging disables access logging for a container.
func (b *InMemoryBackend) StopAccessLogging(name string) error {
	b.mu.Lock("StopAccessLogging")
	defer b.mu.Unlock()

	c, exists := b.containers[name]
	if !exists {
		return ErrContainerNotFound
	}

	c.AccessLoggingEnabled = false

	return nil
}

// TagResource adds or updates tags on a container identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, c := range b.containers {
		if c.ARN == resourceARN {
			if c.Tags == nil {
				c.Tags = make(map[string]string)
			}

			maps.Copy(c.Tags, tags)

			return nil
		}
	}

	return ErrContainerNotFound
}

// UntagResource removes tags from a container identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, c := range b.containers {
		if c.ARN == resourceARN {
			for _, k := range tagKeys {
				delete(c.Tags, k)
			}

			return nil
		}
	}

	return ErrContainerNotFound
}

// ListTagsForResource returns tags for a container identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, c := range b.containers {
		if c.ARN == resourceARN {
			result := make(map[string]string, len(c.Tags))
			maps.Copy(result, c.Tags)

			return result, nil
		}
	}

	return nil, ErrContainerNotFound
}

// copyContainer returns a shallow copy of the Container with the Tags map deep-copied.
func copyContainer(c *Container) *Container {
	if c == nil {
		return nil
	}

	cp := *c

	if c.Tags != nil {
		cp.Tags = make(map[string]string, len(c.Tags))
		maps.Copy(cp.Tags, c.Tags)
	}

	return &cp
}
