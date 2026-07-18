package organizations

import (
	"cmp"
	"slices"
)

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceID string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if !b.resourceExistsLocked(resourceID) {
		return ErrTargetNotFound
	}

	b.setTagsLocked(resourceID, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceID string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if !b.resourceExistsLocked(resourceID) {
		return ErrTargetNotFound
	}

	t := b.tags[resourceID]
	if t == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(t, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceID string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !b.resourceExistsLocked(resourceID) {
		return nil, ErrTargetNotFound
	}

	t := b.tags[resourceID]
	out := make([]Tag, 0, len(t))

	for k, v := range t {
		out = append(out, Tag{Key: k, Value: v})
	}

	slices.SortFunc(out, func(a, b Tag) int { return cmp.Compare(a.Key, b.Key) })

	return out, nil
}

// setTagsLocked merges tags onto a resource. Must be called with lock held.
func (b *InMemoryBackend) setTagsLocked(resourceID string, tags []Tag) {
	if len(tags) == 0 {
		return
	}

	if b.tags[resourceID] == nil {
		b.tags[resourceID] = make(map[string]string)
	}

	for _, t := range tags {
		b.tags[resourceID][t.Key] = t.Value
	}
}
