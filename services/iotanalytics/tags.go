package iotanalytics

import (
	"fmt"
	"strings"
)

// resolveARNResource checks whether a resource ARN corresponds to an existing resource.
// Returns true if the resource exists.
func (b *InMemoryBackend) resolveARNResource(arn string) bool {
	// ARN format: arn:aws:iotanalytics:<region>:<account>:<resourceType>/<name>
	// Parse without assuming a specific region or account.
	const arnSplitParts = 6
	parts := strings.SplitN(arn, ":", arnSplitParts)
	if len(parts) != arnSplitParts || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "iotanalytics" {
		return false
	}

	resource := parts[5]
	resourceType, name, found := strings.Cut(resource, "/")
	if !found {
		return false
	}

	switch resourceType {
	case "channel":
		return b.channels.Has(name)

	case "datastore":
		return b.datastores.Has(name)

	case "dataset":
		return b.datasets.Has(name)

	case "pipeline":
		return b.pipelines.Has(name)
	}

	return false
}

// ListTagsForResource returns tags for a resource ARN, sorted by key.
// Returns empty slice (not error) when the resource exists but has no tags.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]TagDTO, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	// Check the tags map first (fast path for resources with tags initialized).
	m, ok := b.tags[resourceARN]
	if ok {
		return mapToTagsSorted(m), nil
	}

	// Distinguish "resource has no tag map" from "resource does not exist".
	if !b.resolveARNResource(resourceARN) {
		return nil, ErrResourceNotFound
	}

	return []TagDTO{}, nil
}

// TagResource adds or updates tags on a resource, enforcing the per-resource tag limit.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []TagDTO) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	m, ok := b.tags[resourceARN]
	if !ok {
		return ErrResourceNotFound
	}

	for _, t := range tags {
		if _, exists := m[t.Key]; !exists && len(m) >= maxTagsPerResource {
			return fmt.Errorf("%w: resource may not have more than %d tags", ErrValidation, maxTagsPerResource)
		}

		m[t.Key] = t.Value
	}

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	m, ok := b.tags[resourceARN]
	if !ok {
		return ErrResourceNotFound
	}

	for _, k := range tagKeys {
		delete(m, k)
	}

	return nil
}
