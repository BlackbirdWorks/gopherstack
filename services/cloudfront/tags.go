package cloudfront

import (
	"fmt"
	"maps"
	"strings"
)

// taggableTags returns a pointer to the Tags map for the resource identified by resourceARN,
// searching every taggable resource kind (distributions, streaming distributions, trust stores).
// Must be called with the lock held.
func (b *InMemoryBackend) taggableTags(resourceARN string) (*map[string]string, bool) {
	if id, ok := b.distributionARNs[resourceARN]; ok {
		d, _ := b.distributions.Get(id)

		return &d.Tags, true
	}

	if id, ok := b.streamingDistributionARNs[resourceARN]; ok {
		sd, _ := b.streamingDistributions.Get(id)

		return &sd.Tags, true
	}

	if id, ok := b.trustStoreARNs[resourceARN]; ok {
		ts, _ := b.trustStores.Get(id)

		return &ts.Tags, true
	}

	if id, ok := b.distributionTenantARNs[resourceARN]; ok {
		t, _ := b.distributionTenants.Get(id)

		return &t.Tags, true
	}

	if id, ok := b.connectionGroupARNs[resourceARN]; ok {
		cg, _ := b.connectionGroups.Get(id)

		return &cg.Tags, true
	}

	if id, ok := b.connectionFunctionARNs[resourceARN]; ok {
		fn, _ := b.connectionFunctions.Get(id)

		return &fn.Tags, true
	}

	if id, ok := b.anycastIPListARNs[resourceARN]; ok {
		list, _ := b.anycastIPLists.Get(id)

		return &list.Tags, true
	}

	return nil, false
}

// TagResource adds or updates tags on a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	if err := validateCFTags(kv); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	tags, ok := b.taggableTags(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	if *tags == nil {
		*tags = make(map[string]string, len(kv))
	}

	netNew := 0
	for k := range kv {
		if _, exists := (*tags)[k]; !exists {
			netNew++
		}
	}
	if len(*tags)+netNew > maxTagCount {
		return fmt.Errorf("%w: resource cannot have more than %d tags", ErrInvalidTagging, maxTagCount)
	}

	maps.Copy(*tags, kv)

	return nil
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	tags, ok := b.taggableTags(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	for _, k := range keys {
		delete(*tags, k)
	}

	return nil
}

// ListTags returns the tags for a resource by ARN.
func (b *InMemoryBackend) ListTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	tags, ok := b.taggableTags(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	cp := make(map[string]string, len(*tags))
	maps.Copy(cp, *tags)

	return cp, nil
}

const (
	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxTagCount    = 50
)

// validateCFTags enforces CloudFront tag constraints: key 1-128 chars, value 0-256 chars,
// no "aws:" prefix on keys, max 50 tags total.
func validateCFTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot have more than %d tags per resource", ErrInvalidTagging, maxTagCount)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be between 1 and %d characters", ErrInvalidTagging, maxTagKeyLen)
		}

		if strings.HasPrefix(k, "aws:") {
			return fmt.Errorf("%w: tag key must not start with \"aws:\"", ErrInvalidTagging)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrInvalidTagging, maxTagValueLen)
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// DistributionTenant type
// ---------------------------------------------------------------------------
