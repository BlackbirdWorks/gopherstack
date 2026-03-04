package opensearch

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Errors returned by the OpenSearch backend.
var (
	ErrDomainNotFound      = errors.New("ResourceNotFoundException")
	ErrDomainAlreadyExists = errors.New("ResourceAlreadyExistsException")
	ErrInvalidParameter    = errors.New("ValidationException")
)

// ClusterConfig represents the cluster configuration for an OpenSearch domain.
type ClusterConfig struct {
	InstanceType  string `json:"instanceType"`
	InstanceCount int    `json:"instanceCount"`
}

// Domain represents an OpenSearch domain.
type Domain struct {
	Tags          *tags.Tags    `json:"tags,omitempty"`
	Name          string        `json:"name"`
	ARN           string        `json:"arn"`
	EngineVersion string        `json:"engineVersion"`
	Endpoint      string        `json:"endpoint"`
	Status        string        `json:"status"`
	ClusterConfig ClusterConfig `json:"clusterConfig"`
}

// InMemoryBackend is the in-memory store for OpenSearch domains.
type InMemoryBackend struct {
	domains   map[string]*Domain
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		domains:   make(map[string]*Domain),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("opensearch"),
	}
}

// CreateDomain creates a new OpenSearch domain.
func (b *InMemoryBackend) CreateDomain(name, engineVersion string, clusterConfig ClusterConfig) (*Domain, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	if _, exists := b.domains[name]; exists {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, name)
	}

	if engineVersion == "" {
		engineVersion = "OpenSearch_2.11"
	}

	domainARN := arn.Build("es", b.region, b.accountID, "domain/"+name)
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", name, b.accountID, b.region)

	if clusterConfig.InstanceCount == 0 {
		clusterConfig.InstanceCount = 1
	}

	if clusterConfig.InstanceType == "" {
		clusterConfig.InstanceType = "t3.small.search"
	}

	d := &Domain{
		Name:          name,
		ARN:           domainARN,
		EngineVersion: engineVersion,
		Endpoint:      endpoint,
		Status:        "Active",
		ClusterConfig: clusterConfig,
		Tags:          tags.New("opensearch." + name + ".tags"),
	}
	b.domains[name] = d

	cp := *d

	return &cp, nil
}

// DeleteDomain removes a domain by name.
func (b *InMemoryBackend) DeleteDomain(name string) (*Domain, error) {
	b.mu.Lock("DeleteDomain")
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
	b.mu.RLock("DescribeDomain")
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
	b.mu.RLock("ListDomainNames")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.domains))
	for name := range b.domains {
		names = append(names, name)
	}

	return names
}

// findDomainByARN returns the domain matching the given ARN, or nil if not found.
func (b *InMemoryBackend) findDomainByARN(domainARN string) *Domain {
	for _, d := range b.domains {
		if d.ARN == domainARN {
			return d
		}
	}

	return nil
}

// ListTags returns tags for the domain identified by ARN.
func (b *InMemoryBackend) ListTags(domainARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	d := b.findDomainByARN(domainARN)
	if d == nil {
		return nil, fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	return d.Tags.Clone(), nil
}

// AddTags adds or updates tags on the domain identified by ARN.
func (b *InMemoryBackend) AddTags(domainARN string, kv map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	d := b.findDomainByARN(domainARN)
	if d == nil {
		return fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	d.Tags.Merge(kv)

	return nil
}

// RemoveTags removes tag keys from the domain identified by ARN.
func (b *InMemoryBackend) RemoveTags(domainARN string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	d := b.findDomainByARN(domainARN)
	if d == nil {
		return fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	d.Tags.DeleteKeys(keys)

	return nil
}
