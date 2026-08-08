package transcribe

import (
	"fmt"
	"maps"
)

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.resourceTags[resourceArn]; !ok {
		b.resourceTags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[resourceArn], tags)

	return nil
}

// recordResourceTagsLocked attaches tags supplied at resource-creation time (Start*Job
// and Create* Tags parameters) to the resource's ARN, so they are retrievable via
// ListTagsForResource exactly like tags added later via TagResource. Real AWS treats
// creation-time Tags as immediately-attached resource tags. Callers must hold b.mu.
func (b *InMemoryBackend) recordResourceTagsLocked(resourceArn string, tags map[string]string) {
	if len(tags) == 0 {
		return
	}

	dst := make(map[string]string, len(tags))
	maps.Copy(dst, tags)
	b.resourceTags[resourceArn] = dst
}

// forgetResourceTagsLocked removes any tags recorded for resourceArn. Called on resource
// deletion so ListTagsForResource doesn't keep returning tags for a resource that no
// longer exists. Callers must hold b.mu.
func (b *InMemoryBackend) forgetResourceTagsLocked(resourceArn string) {
	delete(b.resourceTags, resourceArn)
}

// UntagResource removes specific tag keys from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if existing, ok := b.resourceTags[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(existing, k)
		}
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.resourceTags[resourceArn]
	if !ok {
		return map[string]string{}, nil
	}

	// Return a copy so callers can't mutate the stored map.
	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every Transcribe resource ARN that currently has
// at least one tag applied via TagResource or recorded at creation time.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))

	for resourceArn, tags := range b.resourceTags {
		if len(tags) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: resourceArn, Tags: maps.Clone(tags)})
	}

	return out
}
