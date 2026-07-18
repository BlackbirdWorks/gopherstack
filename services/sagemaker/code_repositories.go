package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrCodeRepositoryNotFound is returned when a code repository does not exist.
var ErrCodeRepositoryNotFound = awserr.New("ValidationException", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// CodeRepository
// ---------------------------------------------------------------------------

// CodeRepository represents a SageMaker code repository.
type CodeRepository struct {
	CreationTime       time.Time         `json:"CreationTime"`
	LastModifiedTime   time.Time         `json:"LastModifiedTime"`
	Tags               map[string]string `json:"Tags,omitempty"`
	GitConfig          map[string]string `json:"GitConfig,omitempty"`
	CodeRepositoryName string            `json:"CodeRepositoryName"`
	CodeRepositoryArn  string            `json:"CodeRepositoryArn"`
}

func cloneCodeRepository(r *CodeRepository) *CodeRepository {
	cp := *r
	cp.Tags = maps.Clone(r.Tags)
	cp.GitConfig = maps.Clone(r.GitConfig)

	return &cp
}

// CreateCodeRepository creates a code repository.
func (b *InMemoryBackend) CreateCodeRepository(
	ctx context.Context,
	name string,
	gitConfig map[string]string,
	tags map[string]string,
) (*CodeRepository, error) {
	b.mu.Lock("CreateCodeRepository")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", ErrValidation)
	}

	if _, ok := b.codeRepositoriesStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: code repository %q already exists", ErrValidation, name)
	}

	repoARN := arn.Build("sagemaker", region, b.accountID, "code-repository/"+name)
	now := time.Now()

	r := &CodeRepository{
		CodeRepositoryName: name,
		CodeRepositoryArn:  repoARN,
		GitConfig:          maps.Clone(gitConfig),
		Tags:               mergeTags(nil, tags),
		CreationTime:       now,
		LastModifiedTime:   now,
	}
	b.codeRepositoriesStore(region).Put(r)

	return cloneCodeRepository(r), nil
}

// DescribeCodeRepository returns a code repository by name.
func (b *InMemoryBackend) DescribeCodeRepository(ctx context.Context, name string) (*CodeRepository, error) {
	b.mu.RLock("DescribeCodeRepository")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	r, ok := b.codeRepositoriesStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: code repository %q not found", ErrCodeRepositoryNotFound, name)
	}

	return cloneCodeRepository(r), nil
}

// UpdateCodeRepository updates the git config of a code repository.
func (b *InMemoryBackend) UpdateCodeRepository(
	ctx context.Context,
	name string,
	gitConfig map[string]string,
) (*CodeRepository, error) {
	b.mu.Lock("UpdateCodeRepository")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	r, ok := b.codeRepositoriesStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: code repository %q not found", ErrCodeRepositoryNotFound, name)
	}

	if gitConfig != nil {
		r.GitConfig = maps.Clone(gitConfig)
	}

	r.LastModifiedTime = time.Now()

	return cloneCodeRepository(r), nil
}

// DeleteCodeRepository removes a code repository by name.
func (b *InMemoryBackend) DeleteCodeRepository(ctx context.Context, name string) error {
	b.mu.Lock("DeleteCodeRepository")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.codeRepositoriesStore(region).Get(name); !ok {
		return fmt.Errorf("%w: code repository %q not found", ErrCodeRepositoryNotFound, name)
	}

	store := b.codeRepositoriesStore(region)
	store.Delete(name)

	return nil
}

// ListCodeRepositories returns all code repositories sorted by name.
func (b *InMemoryBackend) ListCodeRepositories(ctx context.Context, nextToken string) ([]*CodeRepository, string) {
	b.mu.RLock("ListCodeRepositories")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.codeRepositoriesStoreRO(region),
		nextToken,
		cloneCodeRepository,
		func(v *CodeRepository) string { return v.CodeRepositoryName },
	)
}
