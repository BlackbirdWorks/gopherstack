package codecommit

import "fmt"

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingCodeCommit).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every CodeCommit repository ARN that currently has
// at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	repos := b.repositories.All()
	out := make([]TaggedEntry, 0, len(repos))

	for _, r := range repos {
		if r.Tags == nil || r.Tags.Len() == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: r.ARN, Tags: r.Tags.Clone()})
	}

	return out
}

// TagResource adds or replaces tags on a repository by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	name, ok := b.repositoriesByARN[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	r, _ := b.repositories.Get(name)
	r.Tags.Merge(kv)

	return nil
}

// UntagResource removes tags from a repository by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	name, ok := b.repositoriesByARN[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	r, _ := b.repositories.Get(name)
	r.Tags.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns tags for a repository by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	name, ok := b.repositoriesByARN[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	r, _ := b.repositories.Get(name)

	return r.Tags.Clone(), nil
}
