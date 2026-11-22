package codeartifact

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every domain, repository and package group that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every domain, repository and package group, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, v := range b.domains.All() {
		out = appendEntry(out, v.ARN, v.Tags, taggedOnly)
	}

	for _, v := range b.repositories.All() {
		out = appendEntry(out, v.ARN, v.Tags, taggedOnly)
	}

	for _, v := range b.packageGroups.All() {
		out = appendEntry(out, v.ARN, v.Tags, taggedOnly)
	}

	return out
}

func appendEntry(out []TaggedEntry, arn string, t *tags.Tags, taggedOnly bool) []TaggedEntry {
	empty := t == nil || t.Len() == 0
	if taggedOnly && empty {
		return out
	}

	entry := TaggedEntry{ARN: arn}
	if !empty {
		entry.Tags = t.Clone()
	}

	return append(out, entry)
}

// TagResource adds or replaces tags on a resource by ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, kv map[string]string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, d := range b.domainsByRegion.Get(region) {
		if d.ARN == resourceARN {
			d.Tags.Merge(kv)

			return nil
		}
	}
	for _, r := range b.repositoriesByRegion.Get(region) {
		if r.ARN == resourceARN {
			r.Tags.Merge(kv)

			return nil
		}
	}
	for _, pg := range b.packageGroupsByRegion.Get(region) {
		if pg.ARN == resourceARN {
			pg.Tags.Merge(kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, d := range b.domainsByRegion.Get(region) {
		if d.ARN == resourceARN {
			d.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}
	for _, r := range b.repositoriesByRegion.Get(region) {
		if r.ARN == resourceARN {
			r.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}
	for _, pg := range b.packageGroupsByRegion.Get(region) {
		if pg.ARN == resourceARN {
			pg.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListTagsForResource returns tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, d := range b.domainsByRegion.Get(region) {
		if d.ARN == resourceARN {
			return d.Tags.Clone(), nil
		}
	}
	for _, r := range b.repositoriesByRegion.Get(region) {
		if r.ARN == resourceARN {
			return r.Tags.Clone(), nil
		}
	}
	for _, pg := range b.packageGroupsByRegion.Get(region) {
		if pg.ARN == resourceARN {
			return pg.Tags.Clone(), nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}
