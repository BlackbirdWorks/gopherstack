package xray

import (
	"fmt"
	"maps"
)

// resourceExists reports whether resourceARN corresponds to a known X-Ray group or
// sampling rule -- the only two resource kinds TagResource/UntagResource/
// ListTagsForResource operate on ("The ARN of an X-Ray group or sampling rule").
// Must be called with b.mu held (read or write lock).
func (b *InMemoryBackend) resourceExists(resourceARN string) bool {
	if list := b.groupsByARN.Get(resourceARN); len(list) > 0 {
		return true
	}

	if list := b.samplingRulesByARN.Get(resourceARN); len(list) > 0 {
		return true
	}

	return false
}

// TagResource adds or updates tags on a resource identified by ARN.
// Tags are stored in a per-ARN map on the backend.
// Returns ErrResourceNotFound if resourceARN is not a known group or sampling rule.
// Returns ErrTooManyTags if applying tags would exceed maxTagsPerResource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceARN)
	}

	if b.resourceTags == nil {
		b.resourceTags = make(map[string]map[string]string)
	}

	existing, ok := b.resourceTags[resourceARN]
	if !ok {
		existing = make(map[string]string)
	}

	merged := make(map[string]string, len(existing)+len(tags))
	maps.Copy(merged, existing)
	maps.Copy(merged, tags)

	if len(merged) > maxTagsPerResource {
		return fmt.Errorf("%w: resource %s would have more than %d tags",
			ErrTooManyTags, resourceARN, maxTagsPerResource)
	}

	b.resourceTags[resourceARN] = merged

	return nil
}

// UntagResource removes the specified tag keys from a resource.
// Returns ErrResourceNotFound if resourceARN is not a known group or sampling rule.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceARN)
	}

	if b.resourceTags == nil {
		return nil
	}

	existing := b.resourceTags[resourceARN]
	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns all tags for the given resource ARN as a slice of key/value maps.
// Returns ErrResourceNotFound if resourceARN is not a known group or sampling rule.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.resourceExists(resourceARN) {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceARN)
	}

	tags := b.resourceTags[resourceARN]
	out := make([]map[string]string, 0, len(tags))

	for k, v := range tags {
		out = append(out, map[string]string{"Key": k, "Value": v})
	}

	return out, nil
}

const (
	// maxTagsPerResource is the maximum number of user-applied tags per resource.
	maxTagsPerResource = 50
)

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every X-Ray group or sampling rule ARN that
// currently has at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))

	for resourceARN, tags := range b.resourceTags {
		if len(tags) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: resourceARN, Tags: maps.Clone(tags)})
	}

	return out
}
