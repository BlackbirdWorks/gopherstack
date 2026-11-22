package mediastore

import (
	"context"
	"maps"
	"strings"
)

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every container that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every container, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, table := range b.containers {
		for _, v := range table.All() {
			if !taggedOnly || len(v.Tags) > 0 {
				out = append(out, TaggedEntry{ARN: v.ARN, Tags: maps.Clone(v.Tags)})
			}
		}
	}

	return out
}

// containerNameFromARN extracts the container name from a MediaStore ARN.
// ARN format: arn:aws:mediastore:region:account:container/name.
func containerNameFromARN(resourceARN string) string {
	_, after, ok := strings.Cut(resourceARN, ":container/")
	if !ok {
		return ""
	}

	return after
}

// TagResource adds or updates tags on a container identified by ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags map[string]string) error {
	for k := range tags {
		if k == "" {
			return ErrEmptyTagKey
		}
	}

	region := regionFromContext(ctx)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	name := containerNameFromARN(resourceARN)
	c, ok := b.getContainer(region, name)
	if !ok {
		return ErrContainerNotFound
	}

	if c.Tags == nil {
		c.Tags = make(map[string]string)
	}

	maps.Copy(c.Tags, tags)

	return nil
}

// UntagResource removes tags from a container identified by ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	name := containerNameFromARN(resourceARN)
	c, ok := b.getContainer(region, name)
	if !ok {
		return ErrContainerNotFound
	}

	for _, k := range tagKeys {
		delete(c.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a container identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceARN string,
) (map[string]string, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	name := containerNameFromARN(resourceARN)
	c, ok := b.getContainer(region, name)
	if !ok {
		return nil, ErrContainerNotFound
	}

	result := make(map[string]string, len(c.Tags))
	maps.Copy(result, c.Tags)

	return result, nil
}
