package ecr

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrRepositoryNotFound is returned when a repository does not exist.
	ErrRepositoryNotFound = awserr.New("RepositoryNotFoundException", awserr.ErrNotFound)
	// ErrRepositoryAlreadyExists is returned when a repository already exists.
	ErrRepositoryAlreadyExists = awserr.New("RepositoryAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrInvalidRepositoryName is returned when the repository name is invalid.
	ErrInvalidRepositoryName = errors.New("InvalidParameterException")
)

// Repository represents an ECR repository.
type Repository struct {
	CreatedAt      time.Time `json:"createdAt"`
	RegistryID     string    `json:"registryId"`
	RepositoryARN  string    `json:"repositoryArn"`
	RepositoryName string    `json:"repositoryName"`
	RepositoryURI  string    `json:"repositoryUri"`
}

// InMemoryBackend stores ECR repository state in memory.
type InMemoryBackend struct {
	repos     map[string]*Repository
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
	endpoint  string
}

// NewInMemoryBackend creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackend(accountID, region, endpoint string) *InMemoryBackend {
	return &InMemoryBackend{
		repos:     make(map[string]*Repository),
		mu:        lockmetrics.New("ecr"),
		accountID: accountID,
		region:    region,
		endpoint:  endpoint,
	}
}

// SetEndpoint updates the registry endpoint used in repository URIs.
func (b *InMemoryBackend) SetEndpoint(endpoint string) {
	b.mu.Lock("SetEndpoint")
	defer b.mu.Unlock()

	b.endpoint = endpoint
}

// CreateRepository creates a new ECR repository.
func (b *InMemoryBackend) CreateRepository(name string) (*Repository, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", ErrInvalidRepositoryName)
	}

	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if _, ok := b.repos[name]; ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryAlreadyExists, name)
	}

	repoID := uuid.New().String()
	_ = repoID

	endpoint := b.endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com", b.accountID, b.region)
	}

	repo := &Repository{
		CreatedAt:      time.Now(),
		RegistryID:     b.accountID,
		RepositoryARN:  fmt.Sprintf("arn:aws:ecr:%s:%s:repository/%s", b.region, b.accountID, name),
		RepositoryName: name,
		RepositoryURI:  fmt.Sprintf("%s/%s", endpoint, name),
	}
	b.repos[name] = repo

	cp := *repo

	return &cp, nil
}

// DescribeRepositories returns all repositories, optionally filtered by name.
func (b *InMemoryBackend) DescribeRepositories(names []string) ([]Repository, error) {
	b.mu.RLock("DescribeRepositories")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		out := make([]Repository, 0, len(b.repos))
		for _, r := range b.repos {
			out = append(out, *r)
		}

		return out, nil
	}

	out := make([]Repository, 0, len(names))

	for _, name := range names {
		r, ok := b.repos[name]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
		}

		out = append(out, *r)
	}

	return out, nil
}

// DeleteRepository removes a repository by name.
func (b *InMemoryBackend) DeleteRepository(name string) (*Repository, error) {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	r, ok := b.repos[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
	}

	delete(b.repos, name)

	cp := *r

	return &cp, nil
}
