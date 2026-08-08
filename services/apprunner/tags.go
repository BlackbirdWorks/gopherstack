package apprunner

import (
	"fmt"
	"maps"
)

// resourceExists reports whether resourceArn refers to any known resource type.
func (b *InMemoryBackend) resourceExists(resourceArn string) bool {
	if b.services.Has(resourceArn) {
		return true
	}
	if b.autoScalingConfigs.Has(resourceArn) {
		return true
	}
	if b.connections.Has(resourceArn) {
		return true
	}
	if b.observabilityConfigs.Has(resourceArn) {
		return true
	}
	if b.vpcConnectors.Has(resourceArn) {
		return true
	}
	if b.vpcIngressConnections.Has(resourceArn) {
		return true
	}

	return false
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceArn) {
		return fmt.Errorf("resource %s not found: %w", resourceArn, ErrNotFound)
	}

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceArn) {
		return fmt.Errorf("resource %s not found: %w", resourceArn, ErrNotFound)
	}

	for _, k := range keys {
		delete(b.tags[resourceArn], k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.resourceExists(resourceArn) {
		return nil, fmt.Errorf("resource %s not found: %w", resourceArn, ErrNotFound)
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[resourceArn])

	return result, nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every App Runner resource ARN that currently has
// at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for resourceArn, tags := range b.tags {
		if len(tags) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: resourceArn, Tags: maps.Clone(tags)})
	}

	return out
}
