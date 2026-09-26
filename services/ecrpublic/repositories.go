package ecrpublic

import (
	"fmt"
	"maps"
	"regexp"
	"sort"
	"time"
)

const (
	maxRepositoryNameLen = 205
	minRepositoryNameLen = 2
	maxTagsPerResource   = 50
)

// repositoryNameRE matches AWS's public repository naming rule: lowercase
// letters, numbers, hyphens, underscores, periods, and forward slashes for
// namespacing (e.g. "project-a/nginx-web-app"), confirmed against
// CreateRepositoryInput's docs in aws-sdk-go-v2/service/ecrpublic@v1.47.1.
var repositoryNameRE = regexp.MustCompile(`^[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*$`)

func validateRepositoryName(name string) error {
	if len(name) < minRepositoryNameLen || len(name) > maxRepositoryNameLen || !repositoryNameRE.MatchString(name) {
		return fmt.Errorf("%w: invalid repository name: %s", ErrInvalidParameter, name)
	}

	return nil
}

func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf("%w: a resource can have a maximum of %d tags", ErrTooManyTags, maxTagsPerResource)
	}

	for k := range tags {
		if k == "" {
			return fmt.Errorf("%w: tag key must not be empty", ErrInvalidTagParameter)
		}
	}

	return nil
}

// CreateRepository creates a new public repository under the caller's account.
func (b *InMemoryBackend) CreateRepository(
	name string, catalogData *CatalogData, tags map[string]string,
) (*Repository, error) {
	if err := validateRepositoryName(name); err != nil {
		return nil, err
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if b.repos.Has(name) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryAlreadyExists, name)
	}

	cd := CatalogData{}
	if catalogData != nil {
		cd = *catalogData
	}

	repo := &Repository{
		RepositoryName: name,
		RepositoryArn:  repositoryARN(b.region, b.accountID, name),
		RegistryID:     b.accountID,
		RepositoryURI:  repositoryURI(b.registryAlias, name),
		CreatedAt:      time.Now(),
		CatalogData:    cd,
		Tags:           cloneTagMap(tags),
	}

	b.repos.Put(repo)

	cp := *repo
	cp.Tags = cloneTagMap(repo.Tags)

	return &cp, nil
}

// DescribeRepositories returns repositories in the given registry, optionally
// filtered by name. An unknown name in a non-empty filter is a hard error,
// matching AWS.
func (b *InMemoryBackend) DescribeRepositories(registryID string, names []string) ([]*Repository, error) {
	b.mu.RLock("DescribeRepositories")
	defer b.mu.RUnlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return nil, err
	}

	if len(names) == 0 {
		all := b.repos.All()
		out := make([]*Repository, 0, len(all))

		for _, r := range all {
			cp := *r
			cp.Tags = cloneTagMap(r.Tags)
			out = append(out, &cp)
		}

		sort.Slice(out, func(i, j int) bool { return out[i].RepositoryName < out[j].RepositoryName })

		return out, nil
	}

	out := make([]*Repository, 0, len(names))

	for _, name := range names {
		r, ok := b.repos.Get(name)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
		}

		cp := *r
		cp.Tags = cloneTagMap(r.Tags)
		out = append(out, &cp)
	}

	return out, nil
}

// DeleteRepository deletes a repository. Non-empty repositories are rejected
// unless force is set, matching AWS.
func (b *InMemoryBackend) DeleteRepository(registryID, name string, force bool) (*Repository, error) {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return nil, err
	}

	repo, ok := b.repos.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
	}

	if !force && len(b.imagesByRepo.Get(name)) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotEmpty, name)
	}

	for _, img := range b.imagesByRepo.Get(name) {
		b.images.Delete(imageTableKey(name, img.ImageDigest))
	}

	b.repos.Delete(name)
	delete(b.tagIndex, name)
	delete(b.uploadedLayers, name)

	cp := *repo
	cp.Tags = cloneTagMap(repo.Tags)

	return &cp, nil
}

func cloneTagMap(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}
