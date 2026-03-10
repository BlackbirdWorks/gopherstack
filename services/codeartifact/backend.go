package codeartifact

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
)

// Domain represents an AWS CodeArtifact domain.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateDomain.
type Domain struct {
	CreatedTime    time.Time  `json:"createdTime"`
	Tags           *tags.Tags `json:"tags,omitempty"`
	Name           string     `json:"name"`
	ARN            string     `json:"arn"`
	EncryptionKey  string     `json:"encryptionKey,omitempty"`
	Owner          string     `json:"owner"`
	Region         string     `json:"region"`
	Status         string     `json:"status"`
	S3BucketARN    string     `json:"s3BucketArn,omitempty"`
	AssetSizeBytes int64      `json:"assetSizeBytes"`
}

// Repository represents an AWS CodeArtifact repository.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateRepository.
type Repository struct {
	CreatedTime          time.Time  `json:"createdTime"`
	Tags                 *tags.Tags `json:"tags,omitempty"`
	Name                 string     `json:"name"`
	ARN                  string     `json:"arn"`
	DomainName           string     `json:"domainName"`
	DomainOwner          string     `json:"domainOwner"`
	Description          string     `json:"description,omitempty"`
	AdministratorAccount string     `json:"administratorAccount"`
	Region               string     `json:"region"`
}

// InMemoryBackend is the in-memory store for CodeArtifact resources.
type InMemoryBackend struct {
	domains      map[string]*Domain
	repositories map[string]*Repository // key: domainName/repoName
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new in-memory CodeArtifact backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		domains:      make(map[string]*Domain),
		repositories: make(map[string]*Repository),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("codeartifact"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateDomain creates a new CodeArtifact domain.
func (b *InMemoryBackend) CreateDomain(name, encryptionKey string, kv map[string]string) (*Domain, error) {
	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	if _, ok := b.domains[name]; ok {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrAlreadyExists, name)
	}

	domainARN := arn.Build("codeartifact", b.region, b.accountID, "domain/"+name)
	t := tags.New("codeartifact.domain." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	d := &Domain{
		Name:          name,
		ARN:           domainARN,
		EncryptionKey: encryptionKey,
		Owner:         b.accountID,
		Region:        b.region,
		Status:        "Active",
		S3BucketARN:   "arn:aws:s3:::assets-" + uuid.NewString()[:8],
		CreatedTime:   time.Now().UTC(),
		Tags:          t,
	}
	b.domains[name] = d
	cp := *d

	return &cp, nil
}

// DescribeDomain returns a domain by name.
func (b *InMemoryBackend) DescribeDomain(name string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	d, ok := b.domains[name]
	if !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, name)
	}
	cp := *d

	return &cp, nil
}

// ListDomains returns all domains.
func (b *InMemoryBackend) ListDomains() []*Domain {
	b.mu.RLock("ListDomains")
	defer b.mu.RUnlock()

	list := make([]*Domain, 0, len(b.domains))
	for _, d := range b.domains {
		cp := *d
		list = append(list, &cp)
	}

	return list
}

// DeleteDomain deletes a domain by name.
func (b *InMemoryBackend) DeleteDomain(name string) (*Domain, error) {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	d, ok := b.domains[name]
	if !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, name)
	}
	cp := *d
	delete(b.domains, name)

	return &cp, nil
}

// repoKey returns the map key for a repository.
func repoKey(domainName, repoName string) string {
	return domainName + "/" + repoName
}

// CreateRepository creates a new CodeArtifact repository.
func (b *InMemoryBackend) CreateRepository(
	domainName, repoName, description string,
	kv map[string]string,
) (*Repository, error) {
	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	key := repoKey(domainName, repoName)
	if _, ok := b.repositories[key]; ok {
		return nil, fmt.Errorf("%w: repository %s already exists in domain %s", ErrAlreadyExists, repoName, domainName)
	}

	repoARN := arn.Build("codeartifact", b.region, b.accountID, "repository/"+domainName+"/"+repoName)
	t := tags.New("codeartifact.repository." + key + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	r := &Repository{
		Name:                 repoName,
		ARN:                  repoARN,
		DomainName:           domainName,
		DomainOwner:          b.accountID,
		Description:          description,
		AdministratorAccount: b.accountID,
		Region:               b.region,
		CreatedTime:          time.Now().UTC(),
		Tags:                 t,
	}
	b.repositories[key] = r
	cp := *r

	return &cp, nil
}

// DescribeRepository returns a repository by domain and name.
func (b *InMemoryBackend) DescribeRepository(domainName, repoName string) (*Repository, error) {
	b.mu.RLock("DescribeRepository")
	defer b.mu.RUnlock()

	r, ok := b.repositories[repoKey(domainName, repoName)]
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}
	cp := *r

	return &cp, nil
}

// ListRepositoriesInDomain returns all repositories in a domain.
func (b *InMemoryBackend) ListRepositoriesInDomain(domainName string) []*Repository {
	b.mu.RLock("ListRepositoriesInDomain")
	defer b.mu.RUnlock()

	list := make([]*Repository, 0)
	for _, r := range b.repositories {
		if r.DomainName == domainName {
			cp := *r
			list = append(list, &cp)
		}
	}

	return list
}

// ListRepositories returns all repositories across all domains.
func (b *InMemoryBackend) ListRepositories() []*Repository {
	b.mu.RLock("ListRepositories")
	defer b.mu.RUnlock()

	list := make([]*Repository, 0, len(b.repositories))
	for _, r := range b.repositories {
		cp := *r
		list = append(list, &cp)
	}

	return list
}

// DeleteRepository deletes a repository by domain and name.
func (b *InMemoryBackend) DeleteRepository(domainName, repoName string) (*Repository, error) {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	key := repoKey(domainName, repoName)
	r, ok := b.repositories[key]
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}
	cp := *r
	delete(b.repositories, key)

	return &cp, nil
}

// TagResource adds or replaces tags on a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, d := range b.domains {
		if d.ARN == resourceARN {
			d.Tags.Merge(kv)

			return nil
		}
	}
	for _, r := range b.repositories {
		if r.ARN == resourceARN {
			r.Tags.Merge(kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, d := range b.domains {
		if d.ARN == resourceARN {
			d.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}
	for _, r := range b.repositories {
		if r.ARN == resourceARN {
			r.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListTagsForResource returns tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, d := range b.domains {
		if d.ARN == resourceARN {
			return d.Tags.Clone(), nil
		}
	}
	for _, r := range b.repositories {
		if r.ARN == resourceARN {
			return r.Tags.Clone(), nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}
