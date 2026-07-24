package emr

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// taggedResourceTags returns a pointer to the Tags field of whichever
// tag-bearing resource (cluster or studio) idOrARN identifies within region,
// or nil if neither matches. Real EMR's AddTags/RemoveTags/ListTagsForResource
// accept "a cluster identifier or an Amazon EMR Studio ID" (see
// aws-sdk-go-v2/service/emr's AddTagsInput.ResourceId doc) -- this backend
// previously only ever looked up clusters, so tagging a studio silently
// 400'd as "not found" even though real AWS supports it. Caller must hold
// at least a read lock.
func (b *InMemoryBackend) taggedResourceTags(region, idOrARN string) *[]Tag {
	if cluster := b.findClusterByIDOrARN(region, idOrARN); cluster != nil {
		return &cluster.Tags
	}

	if studio := b.findStudioByIDOrARN(region, idOrARN); studio != nil {
		return &studio.Tags
	}

	return nil
}

// AddTags adds or updates tags on a cluster or studio identified by ARN or
// ID. When resourceID is an ARN the region is resolved from the ARN,
// otherwise the ctx region (falling back to the backend default) is used.
func (b *InMemoryBackend) AddTags(ctx context.Context, resourceID string, tags []Tag) error {
	region := regionFromARN(resourceID, getRegion(ctx, b.region))

	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	target := b.taggedResourceTags(region, resourceID)
	if target == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	existing := tagsToMap(*target)
	for _, t := range tags {
		existing[t.Key] = t.Value
	}

	*target = mapToTags(existing)

	return nil
}

// RemoveTags removes tags from a cluster or studio identified by ARN or ID.
func (b *InMemoryBackend) RemoveTags(ctx context.Context, resourceID string, tagKeys []string) error {
	region := regionFromARN(resourceID, getRegion(ctx, b.region))

	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	target := b.taggedResourceTags(region, resourceID)
	if target == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	existing := tagsToMap(*target)
	for _, k := range tagKeys {
		delete(existing, k)
	}

	*target = mapToTags(existing)

	return nil
}

// ListTagsForResource returns tags for a cluster or studio identified by ARN
// or ID, sorted by key.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceID string) ([]Tag, error) {
	region := regionFromARN(resourceID, getRegion(ctx, b.region))

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	target := b.taggedResourceTags(region, resourceID)
	if target == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	tags := make([]Tag, len(*target))
	copy(tags, *target)

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
