package verifiedpermissions

import (
	"fmt"
	"maps"
	"strings"
)

// validateTagInput checks tag count, key/value length, and reserved prefix
// constraints. tooManyErr is the sentinel returned when the tag count would
// be exceeded: real AWS only declares TooManyTagsException as a possible
// TagResource error (CreatePolicyStore's tag-count overflow is a plain
// ValidationException), so callers pass the sentinel matching their op.
func validateTagInput(existing map[string]string, newTags map[string]string, tooManyErr error) error {
	const maxTagCount = 50
	const maxKeyLen = 128
	const maxValLen = 256

	total := len(existing) + len(newTags)
	// Adjust for overwrites
	for k := range newTags {
		if _, exists := existing[k]; exists {
			total--
		}
	}

	if total > maxTagCount {
		return fmt.Errorf("%w: tag count would exceed %d", tooManyErr, maxTagCount)
	}

	for k, v := range newTags {
		if strings.HasPrefix(k, "aws:") {
			return fmt.Errorf("%w: tag key %q uses reserved prefix \"aws:\"", ErrValidation, k)
		}

		if len(k) > maxKeyLen {
			return fmt.Errorf("%w: tag key %q exceeds maximum length %d", ErrValidation, k, maxKeyLen)
		}

		if len(v) > maxValLen {
			return fmt.Errorf("%w: tag value for key %q exceeds maximum length %d", ErrValidation, k, maxValLen)
		}
	}

	return nil
}

// resolveARN reports whether resourceARN is a known registered resource ARN.
func (b *InMemoryBackend) resolveARN(resourceARN string) bool {
	_, exists := b.arnIndex[resourceARN]

	return exists
}

// TagResource adds or updates tags on a resource identified by its ARN.
// Supports policy stores, policies, policy templates, and identity sources.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	ok := b.resolveARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	existing := b.resourceTags[resourceARN]
	if existing == nil {
		existing = make(map[string]string)
	}

	if err := validateTagInput(existing, tags, ErrTooManyTags); err != nil {
		return err
	}

	if b.resourceTags[resourceARN] == nil {
		b.resourceTags[resourceARN] = make(map[string]string, len(tags))
	}

	maps.Copy(b.resourceTags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
// Supports policy stores, policies, policy templates, and identity sources.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	ok := b.resolveARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(b.resourceTags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns the tags for a resource identified by its ARN.
// Supports policy stores, policies, policy templates, and identity sources.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	ok := b.resolveARN(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	return maps.Clone(b.resourceTags[resourceARN]), nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every Verified Permissions resource ARN that
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
