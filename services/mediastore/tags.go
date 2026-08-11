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

// TaggedResources returns every MediaStore container ARN, across every region, that
// currently has at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, table := range b.containers {
		table.Range(func(c *Container) bool {
			if len(c.Tags) > 0 {
				result := make(map[string]string, len(c.Tags))
				maps.Copy(result, c.Tags)
				out = append(out, TaggedEntry{ARN: c.ARN, Tags: result})
			}

			return true
		})
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
