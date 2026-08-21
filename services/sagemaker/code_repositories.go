package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
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

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeCodeRepository, so the wire
// timestamp encoding must match the JSON protocol's numeric convention (a
// real AWS SDK client fails to deserialize a string where it expects a
// number).
func (r *CodeRepository) MarshalJSON() ([]byte, error) {
	type alias CodeRepository

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(r),
		CreationTime:     epochSeconds(r.CreationTime),
		LastModifiedTime: epochSeconds(r.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [CodeRepository.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (r *CodeRepository) UnmarshalJSON(data []byte) error {
	type alias CodeRepository

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(r)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	r.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	r.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
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

	// api_op_CreateCodeRepository.go:44: GitConfig is required, and
	// types.GitConfig.RepositoryUrl (types.go:9216-9221) is itself required
	// within it -- previously neither was enforced, so a request omitting
	// GitConfig entirely still succeeded.
	if gitConfig["RepositoryUrl"] == "" {
		return nil, fmt.Errorf("%w: GitConfig.RepositoryUrl is required", ErrValidation)
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

// UpdateCodeRepository updates a code repository's SecretArn, the only
// field types.GitConfigForUpdate (api_op_UpdateCodeRepository.go:35-38,
// types.go:9239-9248) declares -- unlike CreateCodeRepositoryInput's
// GitConfig, Update's GitConfigForUpdate has no RepositoryUrl/Branch at
// all, so those are immutable after Create. Previously this replaced the
// entire stored GitConfig map wholesale with whatever the client sent,
// silently wiping RepositoryUrl/Branch on any Update call that (correctly,
// per the real type) omitted them.
func (b *InMemoryBackend) UpdateCodeRepository(
	ctx context.Context,
	name string,
	secretArn string,
) (*CodeRepository, error) {
	b.mu.Lock("UpdateCodeRepository")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	r, ok := b.codeRepositoriesStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: code repository %q not found", ErrCodeRepositoryNotFound, name)
	}

	if secretArn != "" {
		if r.GitConfig == nil {
			r.GitConfig = make(map[string]string, 1)
		}

		r.GitConfig["SecretArn"] = secretArn
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

// ListCodeRepositoriesFilter bundles the filter/sort criteria for
// ListCodeRepositories (api_op_ListCodeRepositories.go:29-64).
type ListCodeRepositoriesFilter struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	NameContains           string
	SortBy                 string
	SortOrder              string
	MaxResults             int32
}

// ListCodeRepositories lists code repositories, optionally filtered and
// sorted. Previously this decoded only NextToken and dropped every filter
// and sort control the op's own request shape declares.
func (b *InMemoryBackend) ListCodeRepositories(
	ctx context.Context,
	nextToken string,
	f ListCodeRepositoriesFilter,
) ([]*CodeRepository, string) {
	b.mu.RLock("ListCodeRepositories")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*CodeRepository, 0, b.codeRepositoriesStoreRO(region).Len())

	for _, r := range b.codeRepositoriesStoreRO(region).All() {
		if codeRepositoryMatchesFilter(r, f) {
			list = append(list, cloneCodeRepository(r))
		}
	}

	// api_op_ListCodeRepositories.go:60,63: real defaults are Name/Ascending.
	desc := f.SortOrder == sortOrderDescending
	sort.Slice(list, func(i, k int) bool {
		var less bool

		switch f.SortBy {
		case keyCreationTime:
			less = list[i].CreationTime.Before(list[k].CreationTime)
		case keyLastModifiedTime:
			less = list[i].LastModifiedTime.Before(list[k].LastModifiedTime)
		default:
			less = list[i].CodeRepositoryName < list[k].CodeRepositoryName
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, f.MaxResults)
}

func codeRepositoryMatchesFilter(r *CodeRepository, f ListCodeRepositoriesFilter) bool {
	if f.NameContains != "" &&
		!strings.Contains(strings.ToLower(r.CodeRepositoryName), strings.ToLower(f.NameContains)) {
		return false
	}

	if !timeWindowOK(r.CreationTime, f.CreationTimeAfter, f.CreationTimeBefore) {
		return false
	}

	return timeWindowOK(r.LastModifiedTime, f.LastModifiedTimeAfter, f.LastModifiedTimeBefore)
}
