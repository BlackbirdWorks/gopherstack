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

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every allow list, custom data identifier, findings filter and classification job
// plus every tagged ARN of the tag store.
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
		b.allowLists.Len()+b.customDataIDs.Len()+b.findingsFilters.Len()+b.classificationJobs.Len(),
	)

	for _, l := range b.allowLists.All() {
		out = append(out, l.Arn)
	}

	for _, c := range b.customDataIDs.All() {
		if !c.Deleted {
			out = append(out, c.Arn)
		}
	}

	for _, f := range b.findingsFilters.All() {
		out = append(out, f.Arn)
	}

	for _, j := range b.classificationJobs.All() {
		out = append(out, j.Arn)
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
