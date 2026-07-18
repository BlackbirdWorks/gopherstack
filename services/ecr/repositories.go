package ecr

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateRepository creates a new ECR repository.
func (b *InMemoryBackend) CreateRepository(
	ctx context.Context,
	name, imageTagMutability string,
	scanOnPush bool,
	encryptionType, kmsKey string,
) (*Repository, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", ErrInvalidRepositoryName)
	}

	if imageTagMutability == "" {
		imageTagMutability = mutabilityMutable
	}

	if encryptionType == "" {
		encryptionType = "AES256"
	}

	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if b.repos.Has(name) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryAlreadyExists, name)
	}

	region := b.regionFor(ctx)

	endpoint := b.endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com", b.accountID, region)
	}

	repo := &Repository{
		CreatedAt:      time.Now(),
		EncryptionType: encryptionType,
		KMSKey:         kmsKey,
		RegistryID:     b.accountID,
		RepositoryARN: arn.Build(
			"ecr",
			region,
			b.accountID,
			fmt.Sprintf("repository/%s", name),
		),
		RepositoryName:     name,
		RepositoryURI:      fmt.Sprintf("%s/%s", endpoint, name),
		ImageTagMutability: imageTagMutability,
		ScanOnPush:         scanOnPush,
	}
	b.repos.Put(repo)

	cp := *repo

	return &cp, nil
}

// DescribeRepositories returns all repositories, optionally filtered by name.
func (b *InMemoryBackend) DescribeRepositories(
	ctx context.Context, //nolint:revive // existing issue.
	names []string,
) ([]Repository, error) {
	b.mu.RLock("DescribeRepositories")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		all := b.repos.All()
		out := make([]Repository, 0, len(all))
		for _, r := range all {
			out = append(out, *r)
		}

		sort.Slice(out, func(i, j int) bool {
			return out[i].RepositoryName < out[j].RepositoryName
		})

		return out, nil
	}

	out := make([]Repository, 0, len(names))

	for _, name := range names {
		r, ok := b.repos.Get(name)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
		}

		out = append(out, *r)
	}

	return out, nil
}

// DeleteRepository removes a repository by name.
func (b *InMemoryBackend) DeleteRepository(
	ctx context.Context, //nolint:revive // existing issue.
	name string,
) (*Repository, error) {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	r, ok := b.repos.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
	}

	b.repos.Delete(name)

	// slices.Clone the index results before deleting in the loop: Table.Delete
	// mutates the very index b.imagesByRepo.Get/b.imageScanFindingsByRepo.Get
	// returned, so iterating the live (unsloned) slice while deleting from it
	// would skip entries.
	for _, img := range slices.Clone(b.imagesByRepo.Get(name)) {
		b.images.Delete(imageTableKey(img.RepositoryName, img.ImageDigest))
	}

	for _, f := range slices.Clone(b.imageScanFindingsByRepo.Get(name)) {
		b.imageScanFindings.Delete(findingsTableKey(f.RepositoryName, f.ImageID.ImageDigest))
	}

	delete(b.tagIndex, name)
	delete(b.digestTagsIndex, name)
	delete(b.uploadedLayers, name)
	b.lifecyclePolicies.Delete(name)
	b.lifecyclePolicyPreviews.Delete(name)
	delete(b.repoTags, r.RepositoryARN)
	b.repositoryPolicies.Delete(name)

	// Clean up any in-progress layer uploads associated with this repository.
	for uploadID := range b.repoUploadIndex[name] {
		delete(b.layerUploads, uploadID)
	}
	delete(b.repoUploadIndex, name)

	cp := *r

	return &cp, nil
}

// repoMatchesFilters returns true when repositoryName matches any filter in the
// slice, or when the slice is empty (no filter = match-all). AWS ECR supports
// WILDCARD (with '*' glob) and PREFIX filter types.
func repoMatchesFilters(name string, filters []RepositoryFilter) bool {
	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		switch f.FilterType {
		case "WILDCARD":
			if wildcardMatch(f.Filter, name) {
				return true
			}
		case "PREFIX":
			if strings.HasPrefix(name, f.Filter) {
				return true
			}
		}
	}

	return false
}

// wildcardMatch returns true when pattern matches name using '*' as a
// zero-or-more-characters wildcard, matching ECR registry filter semantics.
func wildcardMatch(pattern, name string) bool {
	for len(pattern) > 0 {
		if pattern[0] == '*' {
			pattern = pattern[1:]
			if len(pattern) == 0 {
				return true
			}
			for i := range len(name) + 1 {
				if wildcardMatch(pattern, name[i:]) {
					return true
				}
			}

			return false
		}
		if len(name) == 0 || pattern[0] != name[0] {
			return false
		}
		pattern = pattern[1:]
		name = name[1:]
	}

	return len(name) == 0
}

// AddRepositoryInternal seeds a repository directly into the backend for testing.
func (b *InMemoryBackend) AddRepositoryInternal(repo Repository) {
	b.mu.Lock("AddRepositoryInternal")
	defer b.mu.Unlock()

	cp := repo
	b.repos.Put(&cp)
}
