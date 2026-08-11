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

// resourceExistsLocked reports whether resourceARN names a real broker or
// configuration. Caller must hold b.mu (read or write).
func (b *InMemoryBackend) resourceExistsLocked(resourceARN string) bool {
	exists := false

	b.brokers.Range(func(br *Broker) bool {
		if br.BrokerArn == resourceARN {
			exists = true

			return false
		}

		return true
	})

	if exists {
		return true
	}

	b.configurations.Range(func(c *Configuration) bool {
		if c.Arn == resourceARN {
			exists = true

			return false
		}

		return true
	})

	return exists
}

// ListTags returns tags for a resource ARN. Real Amazon MQ returns
// NotFoundException for an ARN that names no broker or configuration
// (confirmed from the pinned SDK: ListTags's error list in
// aws-sdk-go-v2/service/mq/deserializers.go includes NotFoundException).
func (b *InMemoryBackend) ListTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	if !b.resourceExistsLocked(resourceARN) {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t := b.tags[resourceARN]
	cp := make(map[string]string, len(t))
	maps.Copy(cp, t)

	return cp, nil
}

// CreateTags adds or updates tags for a resource ARN. Real Amazon MQ returns
// NotFoundException for an ARN that names no broker or configuration (same
// SDK error-list evidence as ListTags).
// Note: b.tags[arn] and the corresponding broker/config Tags field share
// the same map pointer, so a single write here updates both automatically.
func (b *InMemoryBackend) CreateTags(resourceARN string, tags map[string]string) error {
	if err := validateTagsMap(tags); err != nil {
		return err
	}

	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	if !b.resourceExistsLocked(resourceARN) {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

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

// DeleteTags removes the specified tag keys from a resource ARN. Real Amazon
// MQ returns NotFoundException for an ARN that names no broker or
// configuration (same SDK error-list evidence as ListTags).
// Note: b.tags[arn] and the corresponding broker/config Tags field share
// the same map pointer, so a single delete here updates both automatically.
func (b *InMemoryBackend) DeleteTags(resourceARN string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	if !b.resourceExistsLocked(resourceARN) {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingMQ). MQ keeps tags for both resource kinds it supports (brokers
// and configurations) in one flat ARN-keyed map, so this is a direct walk of
// that map instead of one per-kind loop.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every MQ resource ARN that currently has at least
// one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for resourceARN, tagMap := range b.tags {
		if len(tagMap) == 0 {
			continue
		}

		cp := make(map[string]string, len(tagMap))
		maps.Copy(cp, tagMap)
		out = append(out, TaggedEntry{ARN: resourceARN, Tags: cp})
	}

	return out
}
