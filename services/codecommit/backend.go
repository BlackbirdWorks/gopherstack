package codecommit

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
	ErrNotFound = awserr.New("RepositoryDoesNotExistException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("RepositoryNameExistsException", awserr.ErrConflict)
)

// Repository represents an AWS CodeCommit repository.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateRepository.
type Repository struct {
	CreationDate    time.Time  `json:"creationDate"`
	LastModifiedDate time.Time `json:"lastModifiedDate"`
	Tags            *tags.Tags `json:"tags,omitempty"`
	RepositoryName  string     `json:"repositoryName"`
	RepositoryID    string     `json:"repositoryId"`
	ARN             string     `json:"arn"`
	Description     string     `json:"repositoryDescription,omitempty"`
	AccountID       string     `json:"accountId"`
	Region          string     `json:"-"`
	CloneURLHTTP    string     `json:"cloneUrlHttp"`
	CloneURLSSH     string     `json:"cloneUrlSsh"`
}

// InMemoryBackend is the in-memory store for CodeCommit resources.
type InMemoryBackend struct {
	repositories map[string]*Repository // key: repositoryName
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new in-memory CodeCommit backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		repositories: make(map[string]*Repository),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("codecommit"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateRepository creates a new CodeCommit repository.
func (b *InMemoryBackend) CreateRepository(name, description string, kv map[string]string) (*Repository, error) {
	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if _, ok := b.repositories[name]; ok {
		return nil, fmt.Errorf("%w: repository %s already exists", ErrAlreadyExists, name)
	}

	repoARN := arn.Build("codecommit", b.region, b.accountID, name)
	repoID := uuid.NewString()
	t := tags.New("codecommit.repository." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	now := time.Now().UTC()
	r := &Repository{
		RepositoryName:   name,
		RepositoryID:     repoID,
		ARN:              repoARN,
		Description:      description,
		AccountID:        b.accountID,
		Region:           b.region,
		CreationDate:     now,
		LastModifiedDate: now,
		CloneURLHTTP:     fmt.Sprintf("https://git-codecommit.%s.amazonaws.com/v1/repos/%s", b.region, name),
		CloneURLSSH:      fmt.Sprintf("ssh://git-codecommit.%s.amazonaws.com/v1/repos/%s", b.region, name),
		Tags:             t,
	}
	b.repositories[name] = r
	cp := *r

	return &cp, nil
}

// GetRepository returns a repository by name.
func (b *InMemoryBackend) GetRepository(name string) (*Repository, error) {
	b.mu.RLock("GetRepository")
	defer b.mu.RUnlock()

	r, ok := b.repositories[name]
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, name)
	}
	cp := *r

	return &cp, nil
}

// DeleteRepository deletes a repository by name.
func (b *InMemoryBackend) DeleteRepository(name string) (*Repository, error) {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	r, ok := b.repositories[name]
	if !ok {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, name)
	}
	cp := *r
	delete(b.repositories, name)

	return &cp, nil
}

// ListRepositories returns all repositories.
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

// TagResource adds or replaces tags on a repository by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, r := range b.repositories {
		if r.ARN == resourceARN {
			r.Tags.Merge(kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes tags from a repository by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, r := range b.repositories {
		if r.ARN == resourceARN {
			r.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListTagsForResource returns tags for a repository by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, r := range b.repositories {
		if r.ARN == resourceARN {
			return r.Tags.Clone(), nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}
