package detective

import (
	"fmt"
	"maps"
)

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownResource(resourceARN) {
		return ErrGraphNotFound
	}

	existing := b.tags[resourceARN]
	if existing == nil {
		existing = make(map[string]string)
		b.tags[resourceARN] = existing
	}

	newCount := len(existing)
	for k := range tags {
		if _, alreadyExists := existing[k]; !alreadyExists {
			newCount++
		}
	}

	if newCount > maxTagCount {
		return fmt.Errorf("%w: resource would exceed the %d tag limit", ErrValidation, maxTagCount)
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.isKnownResource(resourceARN) {
		return ErrGraphNotFound
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.isKnownResource(resourceARN) {
		return nil, ErrGraphNotFound
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[resourceARN])

	return result, nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingDetective).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every graph plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))
	seen := make(map[string]struct{}, len(b.tags))

	for arn, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		seen[arn] = struct{}{}
		out = append(out, TaggedEntry{ARN: arn, Tags: maps.Clone(t)})
	}

	if withUntagged {
		for _, arn := range b.resourceARNsLocked() {
			if _, ok := seen[arn]; !ok {
				seen[arn] = struct{}{}
				out = append(out, TaggedEntry{ARN: arn})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	out := make([]string, 0, b.graphs.Len())

	for _, g := range b.graphs.All() {
		out = append(out, g.Arn)
	}

	return out
}

// isKnownResource returns true if the ARN corresponds to a known graph.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) isKnownResource(arn string) bool {
	return b.graphs.Has(arn)
}
