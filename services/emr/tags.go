package emr

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// AddTags adds or updates tags on a cluster identified by ARN or ID.
// When resourceID is an ARN the region is resolved from the ARN, otherwise the
// ctx region (falling back to the backend default) is used.
func (b *InMemoryBackend) AddTags(ctx context.Context, resourceID string, tags []Tag) error {
	region := regionFromARN(resourceID, getRegion(ctx, b.region))

	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	cluster := b.findClusterByIDOrARN(region, resourceID)
	if cluster == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	existing := tagsToMap(cluster.Tags)
	for _, t := range tags {
		existing[t.Key] = t.Value
	}

	cluster.Tags = mapToTags(existing)

	return nil
}

// RemoveTags removes tags from a cluster identified by ARN or ID.
func (b *InMemoryBackend) RemoveTags(ctx context.Context, resourceID string, tagKeys []string) error {
	region := regionFromARN(resourceID, getRegion(ctx, b.region))

	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	cluster := b.findClusterByIDOrARN(region, resourceID)
	if cluster == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	existing := tagsToMap(cluster.Tags)
	for _, k := range tagKeys {
		delete(existing, k)
	}

	cluster.Tags = mapToTags(existing)

	return nil
}

// ListTagsForResource returns tags for a cluster identified by ARN or ID, sorted by key.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceID string) ([]Tag, error) {
	region := regionFromARN(resourceID, getRegion(ctx, b.region))

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	cluster := b.findClusterByIDOrARN(region, resourceID)
	if cluster == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	tags := make([]Tag, len(cluster.Tags))
	copy(tags, cluster.Tags)

	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Key < tags[j].Key
	})

	return tags, nil
}

func tagsToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

func mapToTags(m map[string]string) []Tag {
	keys := sortedTagKeys(m)
	tags := make([]Tag, 0, len(keys))

	for _, k := range keys {
		tags = append(tags, Tag{Key: k, Value: m[k]})
	}

	return tags
}

func sortedTagKeys(m map[string]string) []string {
	keys := collections.SortedKeys(m)

	return keys
}
