package macie2

import (
	"fmt"
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTagInput(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownARN(resourceARN) {
		return fmt.Errorf("%w: %s", ErrTaggedResourceNotFound, resourceARN)
	}

	existing := b.tags[resourceARN]
	netNew := 0

	for k := range tags {
		if _, alreadySet := existing[k]; !alreadySet {
			netNew++
		}
	}

	if len(existing)+netNew > maxTagCount {
		return fmt.Errorf("%w: resource would exceed %d tag limit", ErrValidation, maxTagCount)
	}

	if existing == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.isKnownARN(resourceARN) {
		return fmt.Errorf("%w: %s", ErrTaggedResourceNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns the tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.isKnownARN(resourceARN) {
		return nil, fmt.Errorf("%w: %s", ErrTaggedResourceNotFound, resourceARN)
	}

	if b.tags[resourceARN] == nil {
		return map[string]string{}, nil
	}

	return maps.Clone(b.tags[resourceARN]), nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every Macie2 resource ARN that currently has at
// least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for resourceARN, tags := range b.tags {
		if len(tags) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: resourceARN, Tags: maps.Clone(tags)})
	}

	return out
}

// isKnownARN reports whether arn refers to a live resource owned by this backend.
func (b *InMemoryBackend) isKnownARN(arnStr string) bool {
	prefix := arn.Build("macie2", b.region, b.accountID, "")
	if !strings.HasPrefix(arnStr, prefix) {
		return false
	}

	rest := strings.TrimPrefix(arnStr, prefix)

	resourceType, id, found := strings.Cut(rest, "/")
	if !found {
		return false
	}

	switch resourceType {
	case "allow-list":
		return b.allowLists.Has(id)
	case "custom-data-identifier":
		cdi, exists := b.customDataIDs.Get(id)

		return exists && !cdi.Deleted
	case "findings-filter":
		return b.findingsFilters.Has(id)
	case "classification-job":
		return b.classificationJobs.Has(id)
	}

	return false
}

func validateTagInput(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: tags must not exceed %d entries", ErrValidation, maxTagCount)
	}

	for k, v := range tags {
		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must not exceed %d characters", ErrValidation, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must not exceed %d characters", ErrValidation, maxTagValueLen)
		}
	}

	return nil
}

// Snapshot and Restore -- implementing persistence.Persistable for
// InMemoryBackend -- live in persistence.go alongside the Handler-level
// delegates, following the services/sesv2 and services/codecommit Phase 3.3
// pattern.
