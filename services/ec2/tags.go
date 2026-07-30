package ec2

import (
	"fmt"
	"maps"
)

// resourceTypeByID and resourceExistsLocked live in resource_types.go
// (split out: the full taggable-resource-type table is large and orthogonal
// to the rest of this file).

// TagEntry holds a single resource-tag association returned by DescribeTags.
type TagEntry struct {
	ResourceID   string `json:"resourceID,omitempty"`
	ResourceType string `json:"resourceType,omitempty"`
	Key          string `json:"key,omitempty"`
	Value        string `json:"value,omitempty"`
}

// CreateTags adds or updates tags on one or more resources.
// Returns ErrInvalidParameter if any resource ID does not exist.
// All IDs are validated before any tags are written, making the operation atomic
// with respect to failures: either all resources are tagged or none are.
func (b *InMemoryBackend) CreateTags(resourceIDs []string, tags map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	// Pre-validate: all resource IDs must exist before any tags are written.
	for _, id := range resourceIDs {
		if !b.resourceExistsLocked(id) {
			return fmt.Errorf("%w: resource %s does not exist", ErrInvalidParameter, id)
		}
	}

	for _, id := range resourceIDs {
		b.setTagsLocked(id, tags)
	}

	return nil
}

// DeleteTags removes the specified tag keys from one or more resources.
// If keys is empty, the operation is a no-op (EC2 requires at least one tag key).
// Empty per-resource tag maps are removed after deletions.
func (b *InMemoryBackend) DeleteTags(resourceIDs []string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	for _, id := range resourceIDs {
		if b.tags[id] == nil {
			continue
		}

		for _, k := range keys {
			delete(b.tags[id], k)
		}

		if len(b.tags[id]) == 0 {
			delete(b.tags, id)
		}
	}

	return nil
}

// setTagsLocked writes the given tags for id directly into the shared tag store.
// Unlike CreateTags, callers must already hold b.mu (write lock) and the resource
// existence pre-check is skipped — this is meant to be called from within a
// Create<Resource> method for the resource it just created in the same critical
// section, so existence is already guaranteed. This is the single source of truth
// for tags: resource structs must NOT carry their own embedded Tags field, or the
// two copies drift (a tag written via CreateTags becomes invisible to a Describe
// that reads the embedded field, and vice versa).
func (b *InMemoryBackend) setTagsLocked(id string, tags map[string]string) {
	if len(tags) == 0 {
		return
	}

	if b.tags[id] == nil {
		b.tags[id] = make(map[string]string, len(tags))
	}

	maps.Copy(b.tags[id], tags)
}

// TagsForResource returns a copy of the tags currently set on the given
// resource, or an empty map when nothing is tagged. Safe for concurrent use.
func (b *InMemoryBackend) TagsForResource(resourceID string) map[string]string {
	b.mu.RLock("TagsForResource")
	defer b.mu.RUnlock()

	src, ok := b.tags[resourceID]
	if !ok || len(src) == 0 {
		return map[string]string{}
	}

	out := make(map[string]string, len(src))
	maps.Copy(out, src)

	return out
}

// DescribeTags returns all tag entries, optionally filtered by resource IDs.
func (b *InMemoryBackend) DescribeTags(resourceIDs []string) []TagEntry {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	// Only build the filter set when callers actually supply IDs; avoids an
	// unnecessary allocation on the common unfiltered path.
	var filterSet map[string]bool
	if len(resourceIDs) > 0 {
		filterSet = make(map[string]bool, len(resourceIDs))
		for _, id := range resourceIDs {
			filterSet[id] = true
		}
	}

	var entries []TagEntry

	for resourceID, tagMap := range b.tags {
		if filterSet != nil && !filterSet[resourceID] {
			continue
		}

		resType := resourceTypeByID(resourceID)

		for k, v := range tagMap {
			entries = append(entries, TagEntry{
				ResourceID:   resourceID,
				ResourceType: resType,
				Key:          k,
				Value:        v,
			})
		}
	}

	return entries
}
