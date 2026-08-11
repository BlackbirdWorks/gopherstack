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

// resourceExists reports whether resourceARN identifies a taggable
// Rekognition resource. Per aws-sdk-go-v2/service/rekognition's
// api_op_TagResource.go doc comment, TagResource applies to "an Amazon
// Rekognition collection, stream processor, or Custom Labels model" --
// the latter is a project *version* ARN, not the project ARN itself.
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

	return false
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every collection, stream processor, and project
// version ARN that currently has at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for arn, tags := range b.tags {
		if len(tags) == 0 {
			continue
		}

		cp := make(map[string]string, len(tags))
		maps.Copy(cp, tags)
		out = append(out, TaggedEntry{ARN: arn, Tags: cp})
	}

	return out
}
