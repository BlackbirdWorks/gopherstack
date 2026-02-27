package opensearch

import (
	"errors"
	"fmt"
	"sync"
)

// Errors returned by the OpenSearch backend.
var (
	ErrDomainNotFound     = errors.New("ResourceNotFoundException")
	ErrDomainAlreadyExists = errors.New("ResourceAlreadyExistsException")
	ErrInvalidParameter   = errors.New("ValidationException")
)

// ClusterConfig represents the cluster configuration for an OpenSearch domain.
type ClusterConfig struct {
	InstanceType  string
	InstanceCount int
}

// Domain represents an OpenSearch domain.
type Domain struct {
	ClusterConfig ClusterConfig
	Name          string
	ARN           string
	EngineVersion string
	Endpoint      string
	Status        string
}

// InMemoryBackend is the in-memory store for OpenSearch domains.
type InMemoryBackend struct {
	domains   map[string]*Domain
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		domains:   make(map[string]*Domain),
		accountID: accountID,
		region:    region,
	}
}

// CreateDomain creates a new OpenSearch domain.
func (b *InMemoryBackend) CreateDomain(name, engineVersion string, clusterConfig ClusterConfig) (*Domain, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.domains[name]; exists {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, name)
	}

	if engineVersion == "" {
		engineVersion = "OpenSearch_2.11"
	}

	arn := fmt.Sprintf("arn:aws:es:%s:%s:domain/%s", b.region, b.accountID, name)
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", name, b.accountID, b.region)

	if clusterConfig.InstanceCount == 0 {
		clusterConfig.InstanceCount = 1
	}

	if clusterConfig.InstanceType == "" {
		clusterConfig.InstanceType = "t3.small.search"
	}

	d := &Domain{
		Name:          name,
		ARN:           arn,
		EngineVersion: engineVersion,
		Endpoint:      endpoint,
		Status:        "Active",
		ClusterConfig: clusterConfig,
	}
	b.domains[name] = d

	cp := *d

	return &cp, nil
}

// DeleteDomain removes a domain by name.
func (b *InMemoryBackend) DeleteDomain(name string) (*Domain, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, exists := b.domains[name]
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := *d
	delete(b.domains, name)

	return &cp, nil
}

// DescribeDomain returns details about a domain.
func (b *InMemoryBackend) DescribeDomain(name string) (*Domain, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, exists := b.domains[name]
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := *d

	return &cp, nil
}

// ListDomainNames returns the names of all domains.
func (b *InMemoryBackend) ListDomainNames() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.domains))
	for name := range b.domains {
		names = append(names, name)
	}

	return names
}
