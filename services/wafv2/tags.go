package wafv2

import (
	"context"
	"fmt"
	"maps"
	"strings"
)

// validateTags checks that tags conform to AWS constraints:
// - Keys: 1–128 chars, cannot start with "aws:"
// - Values: 0–256 chars
// - Max 50 tags per resource.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: too many tags: %d (max %d)",
			ErrTagOperation,
			len(tags),
			maxTagsPerResource,
		)
	}

	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key %q must be 1–%d characters", ErrTagOperation, k, maxTagKeyLen)
		}

		if strings.HasPrefix(k, "aws:") {
			return fmt.Errorf("%w: tag key %q uses reserved prefix aws", ErrTagOperation, k)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value for key %q must be 0–%d characters", ErrTagOperation, k, maxTagValueLen)
		}
	}

	return nil
}

// lookupTaggedResource resolves the tags pointer for a resource ARN via the
// by-ARN store.Index on each resource table (ARN is already globally unique,
// so no region partitioning is needed for this lookup). Returns nil if not found.
func (b *InMemoryBackend) lookupTaggedResource(resourceARN string) *map[string]string {
	if ws := b.webACLsByARN.Get(resourceARN); len(ws) > 0 {
		return &ws[0].Tags
	}

	if ss := b.ipSetsByARN.Get(resourceARN); len(ss) > 0 {
		return &ss[0].Tags
	}

	if rs := b.regexPatternSetsByARN.Get(resourceARN); len(rs) > 0 {
		return &rs[0].Tags
	}

	if rgs := b.ruleGroupsByARN.Get(resourceARN); len(rgs) > 0 {
		return &rgs[0].Tags
	}

	return nil
}

// TagResource adds tags to a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	tagsPtr := b.lookupTaggedResource(resourceARN)
	if tagsPtr == nil {
		return fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
	}

	if *tagsPtr == nil {
		*tagsPtr = make(map[string]string)
	}

	maps.Copy(*tagsPtr, tags)

	return nil
}

// ListTagsForResource returns the tags for a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(_ context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tagsPtr := b.lookupTaggedResource(resourceARN)
	if tagsPtr == nil {
		return nil, fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
	}

	return maps.Clone(*tagsPtr), nil
}

// UntagResource removes tags from a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	tagsPtr := b.lookupTaggedResource(resourceARN)
	if tagsPtr == nil {
		return fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(*tagsPtr, k)
	}

	return nil
}
