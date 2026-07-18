package elb

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// validateAddTagsKVs validates tag key/value lengths and rejects a request that
// specifies the same tag key more than once (AWS: DuplicateTagKeysException).
// The duplicate-key check is a same-request check, distinct from overwriting an
// existing tag value across separate AddTags calls, which is the documented
// "add or update" behavior and remains allowed.
func validateAddTagsKVs(kvs []tags.KV) error {
	const maxTagKeyLen = 128
	const maxTagValueLen = 256

	seenKeys := make(map[string]struct{}, len(kvs))

	for _, kv := range kvs {
		if kv.Key == "" || len(kv.Key) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be 1-%d characters", ErrInvalidParameter, maxTagKeyLen)
		}

		if len(kv.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be 0-%d characters", ErrInvalidParameter, maxTagValueLen)
		}

		if _, dup := seenKeys[kv.Key]; dup {
			return fmt.Errorf("%w: %q", ErrDuplicateTagKeys, kv.Key)
		}

		seenKeys[kv.Key] = struct{}{}
	}

	return nil
}

// AddTags adds or updates tags on one or more load balancers.
func (b *InMemoryBackend) AddTags(ctx context.Context, names []string, kvs []tags.KV) error {
	if err := validateAddTagsKVs(kvs); err != nil {
		return err
	}

	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	const maxTagsPerLB = 10

	for _, name := range names {
		lb, ok := b.lbs.Get(lbTableKey(region, name))
		if !ok {
			return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}

		// Count existing tags that won't be overwritten.
		newKeys := make(map[string]struct{}, len(kvs))
		for _, kv := range kvs {
			newKeys[kv.Key] = struct{}{}
		}

		existingCount := 0
		lb.Tags.Range(func(k, _ string) bool {
			if _, isNew := newKeys[k]; !isNew {
				existingCount++
			}

			return true
		})

		if existingCount+len(newKeys) > maxTagsPerLB {
			return fmt.Errorf("%w: cannot have more than %d tags on a load balancer", ErrTooManyTags, maxTagsPerLB)
		}

		for _, kv := range kvs {
			lb.Tags.Set(kv.Key, kv.Value)
		}
	}

	return nil
}

// DescribeTags returns the tags for the given load balancers.
func (b *InMemoryBackend) DescribeTags(ctx context.Context, names []string) (map[string][]tags.KV, error) {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	result := make(map[string][]tags.KV, len(names))

	for _, name := range names {
		lb, ok := b.lbs.Get(lbTableKey(region, name))
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}

		kvs := make([]tags.KV, 0, lb.Tags.Len())
		lb.Tags.Range(func(k, v string) bool {
			kvs = append(kvs, tags.KV{Key: k, Value: v})

			return true
		})

		sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

		result[name] = kvs
	}

	return result, nil
}

// RemoveTags removes the specified tag keys from one or more load balancers.
func (b *InMemoryBackend) RemoveTags(ctx context.Context, names []string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, name := range names {
		lb, ok := b.lbs.Get(lbTableKey(region, name))
		if !ok {
			return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}

		lb.Tags.DeleteKeys(keys)
	}

	return nil
}
