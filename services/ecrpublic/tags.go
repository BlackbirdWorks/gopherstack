package ecrpublic

import (
	"fmt"
	"maps"
	"strings"
)

// repositoryNameFromARN extracts the repository name from a well-formed
// public-repository ARN (arn:{partition}:ecr-public::{account}:repository/{name}).
func repositoryNameFromARN(resourceARN string) (string, bool) {
	parts := strings.SplitN(resourceARN, ":", 6) //nolint:mnd // arn:partition:service::account:resource
	if len(parts) != 6 || !strings.HasPrefix(parts[5], "repository/") {
		return "", false
	}

	name := strings.TrimPrefix(parts[5], "repository/")
	if name == "" {
		return "", false
	}

	return name, true
}

func (b *InMemoryBackend) resolveByARNLocked(resourceARN string) (*Repository, error) {
	name, ok := repositoryNameFromARN(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: malformed resource ARN: %s", ErrInvalidParameter, resourceARN)
	}

	repo, ok := b.repos.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, resourceARN)
	}

	return repo, nil
}

// TagResource adds or replaces tags on a repository.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	repo, err := b.resolveByARNLocked(resourceARN)
	if err != nil {
		return err
	}

	merged := cloneTagMap(repo.Tags)
	maps.Copy(merged, tags)

	if validateErr := validateTags(merged); validateErr != nil {
		return validateErr
	}

	repo.Tags = merged

	return nil
}

// UntagResource removes tags from a repository by key.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	repo, err := b.resolveByARNLocked(resourceARN)
	if err != nil {
		return err
	}

	for _, k := range tagKeys {
		delete(repo.Tags, k)
	}

	return nil
}

// ListTagsForResource returns all tags on a repository.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	repo, err := b.resolveByARNLocked(resourceARN)
	if err != nil {
		return nil, err
	}

	return cloneTagMap(repo.Tags), nil
}
