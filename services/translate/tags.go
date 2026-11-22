package translate

import (
	"fmt"
	"maps"
)

// TagResource adds or replaces tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, newTags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	if tooManyTags(b.tags[resourceARN], newTags) {
		return fmt.Errorf(
			"%w: resource %q would exceed the %d-tag limit",
			ErrTooManyTags,
			resourceARN,
			maxTagsPerResource,
		)
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], newTags)

	return nil
}

// UntagResource removes specific tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	for _, k := range keys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.arnExists(resourceARN) {
		return nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	return copyMap(b.tags[resourceARN]), nil
}

// arnExists checks whether the ARN corresponds to any stored resource.
func (b *InMemoryBackend) arnExists(resourceARN string) bool {
	found := false

	b.terminologies.Range(func(t *Terminology) bool {
		if t.ARN == resourceARN {
			found = true

			return false
		}

		return true
	})

	if found {
		return true
	}

	b.parallelData.Range(func(pd *ParallelData) bool {
		if pd.ARN == resourceARN {
			found = true

			return false
		}

		return true
	})

	return found
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every terminology and parallel data plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))
	seen := make(map[string]struct{}, len(b.tags))

	for key, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: copyMap(t)})
	}

	if withUntagged {
		for _, key := range b.resourceARNsLocked() {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				out = append(out, TaggedEntry{ARN: key})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	out := make([]string, 0, b.terminologies.Len()+b.parallelData.Len())

	for _, t := range b.terminologies.All() {
		out = append(out, t.ARN)
	}

	for _, p := range b.parallelData.All() {
		out = append(out, p.ARN)
	}

	return out
}
