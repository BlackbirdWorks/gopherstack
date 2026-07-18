package mq

import (
	"fmt"
	"maps"
)

const (
	// maxTagsPerResource is the maximum number of tags per resource.
	// AWS MQ enforces a limit of 50 tags per broker or configuration.
	maxTagsPerResource = 50

	// maxTagKeyLen is the maximum length of a tag key in bytes.
	maxTagKeyLen = 128

	// maxTagValueLen is the maximum length of a tag value in bytes.
	maxTagValueLen = 256
)

// validateTagKey enforces AWS MQ tag key constraints: 1-128 characters.
func validateTagKey(key string) error {
	if len(key) < 1 || len(key) > maxTagKeyLen {
		return fmt.Errorf(
			"%w: tag key must be 1-%d characters (got %d)",
			ErrValidation, maxTagKeyLen, len(key),
		)
	}

	return nil
}

// validateTagValue enforces AWS MQ tag value constraints: 0-256 characters.
func validateTagValue(value string) error {
	if len(value) > maxTagValueLen {
		return fmt.Errorf(
			"%w: tag value must be 0-%d characters (got %d)",
			ErrValidation, maxTagValueLen, len(value),
		)
	}

	return nil
}

// validateTagsMap checks that all tag keys and values satisfy AWS MQ limits.
func validateTagsMap(tags map[string]string) error {
	for k, v := range tags {
		if err := validateTagKey(k); err != nil {
			return err
		}

		if err := validateTagValue(v); err != nil {
			return err
		}
	}

	return nil
}

// ListTags returns tags for a resource ARN.
func (b *InMemoryBackend) ListTags(resourceARN string) map[string]string {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	t := b.tags[resourceARN]
	cp := make(map[string]string, len(t))
	maps.Copy(cp, t)

	return cp
}

// CreateTags adds or updates tags for a resource ARN.
// Note: b.tags[arn] and the corresponding broker/config Tags field share
// the same map pointer, so a single write here updates both automatically.
func (b *InMemoryBackend) CreateTags(resourceARN string, tags map[string]string) error {
	if err := validateTagsMap(tags); err != nil {
		return err
	}

	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	if existing == nil {
		existing = make(map[string]string)
		b.tags[resourceARN] = existing
	}

	// Count how many new distinct keys will be added.
	newCount := 0
	for k := range tags {
		if _, ok := existing[k]; !ok {
			newCount++
		}
	}

	if len(existing)+newCount > maxTagsPerResource {
		return fmt.Errorf(
			"%w: resource cannot have more than %d tags",
			ErrValidation, maxTagsPerResource,
		)
	}

	maps.Copy(existing, tags)

	return nil
}

// DeleteTags removes the specified tag keys from a resource ARN.
// Note: b.tags[arn] and the corresponding broker/config Tags field share
// the same map pointer, so a single delete here updates both automatically.
func (b *InMemoryBackend) DeleteTags(resourceARN string, tagKeys []string) {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}
}
