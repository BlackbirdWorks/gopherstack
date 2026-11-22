package rekognition

import (
	"fmt"
	"maps"
)

const (
	maxTagCount    = 200
	maxTagKeyLen   = 128
	maxTagValueLen = 256
)

// validateTags enforces AWS tag limits: key 1-128 chars, value 0-256 chars, max 200 tags.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot specify more than %d tags", ErrValidation, maxTagCount)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be between 1 and %d characters", ErrValidation, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrValidation, maxTagValueLen)
		}
	}

	return nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrCollectionNotFound
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

	if !b.resourceExists(resourceARN) {
		return ErrCollectionNotFound
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

	if !b.resourceExists(resourceARN) {
		return nil, ErrCollectionNotFound
	}

	tags := make(map[string]string)
	maps.Copy(tags, b.tags[resourceARN])

	return tags, nil
}

// resourceExists reports whether resourceARN is a taggable collection, stream
// processor, project version, project or dataset.
func (b *InMemoryBackend) resourceExists(resourceARN string) bool {
	for _, c := range b.collections.All() {
		if c.CollectionARN == resourceARN {
			return true
		}
	}

	for _, p := range b.streamProcessors.All() {
		if p.StreamProcessorARN == resourceARN {
			return true
		}
	}

	for _, v := range b.projectVersions.All() {
		if v.ProjectVersionARN == resourceARN {
			return true
		}
	}

	return b.projects.Has(resourceARN) || b.datasets.Has(resourceARN)
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

// InventoryResources lists every collection, stream processor, project, dataset and project version plus
// every tagged ARN of the tag store.
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
		out = append(out, TaggedEntry{ARN: key, Tags: maps.Clone(t)})
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
	out := make(
		[]string,
		0,
		b.collections.Len()+b.streamProcessors.Len()+b.projects.Len()+b.datasets.Len()+b.projectVersions.Len(),
	)

	for _, c := range b.collections.All() {
		out = append(out, c.CollectionARN)
	}

	for _, p := range b.streamProcessors.All() {
		out = append(out, p.StreamProcessorARN)
	}

	for _, p := range b.projects.All() {
		out = append(out, p.ProjectARN)
	}

	for _, d := range b.datasets.All() {
		out = append(out, d.DatasetARN)
	}

	for _, v := range b.projectVersions.All() {
		out = append(out, v.ProjectVersionARN)
	}

	return out
}
