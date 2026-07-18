package elbv2

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// findTagsLocked returns the *tags.Tags for the given resource ARN.
// Caller must hold b.mu (read or write).
func (b *InMemoryBackend) findTagsLocked(resArn string) *tags.Tags {
	if lb, ok := b.loadBalancers.Get(resArn); ok {
		return lb.Tags
	}

	if tg, ok := b.targetGroups.Get(resArn); ok {
		return tg.Tags
	}

	if l, ok := b.listeners.Get(resArn); ok {
		return l.Tags
	}

	if r, ok := b.rules.Get(resArn); ok {
		return r.Tags
	}

	if ts, ok := b.trustStores.Get(resArn); ok {
		return ts.Tags
	}

	return nil
}

// validateTagKVs returns ErrInvalidParameter when any tag key or value violates AWS limits.
func validateTagKVs(kvs []tags.KV) error {
	for _, kv := range kvs {
		if len(kv.Key) == 0 || len(kv.Key) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key must be between 1 and %d characters",
				ErrInvalidParameter,
				maxTagKeyLen,
			)
		}

		if len(kv.Value) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value must not exceed %d characters",
				ErrInvalidParameter,
				maxTagValueLen,
			)
		}
	}

	return nil
}

// AddTags adds or updates tags on ELBv2 resources.
func (b *InMemoryBackend) AddTags(resourceArns []string, kvs []tags.KV) error {
	if err := validateTagKVs(kvs); err != nil {
		return err
	}

	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	for _, resArn := range resourceArns {
		t := b.findTagsLocked(resArn)
		if t == nil {
			continue
		}

		if t.Len()+len(kvs) > maxTagsPerRes {
			// Count net-new keys only to avoid over-counting updates to existing keys.
			netNew := 0
			for _, kv := range kvs {
				if !t.HasTag(kv.Key) {
					netNew++
				}
			}

			if t.Len()+netNew > maxTagsPerRes {
				return fmt.Errorf(
					"%w: resource cannot have more than %d tags",
					ErrInvalidParameter,
					maxTagsPerRes,
				)
			}
		}

		for _, kv := range kvs {
			t.Set(kv.Key, kv.Value)
		}
	}

	return nil
}

// RemoveTags removes tags from ELBv2 resources.
func (b *InMemoryBackend) RemoveTags(resourceArns []string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	for _, resArn := range resourceArns {
		t := b.findTagsLocked(resArn)
		if t != nil {
			t.DeleteKeys(keys)
		}
	}

	return nil
}

func tagsToKVs(t *tags.Tags) []tags.KV {
	kvs := make([]tags.KV, 0, t.Len())
	t.Range(func(k, v string) bool {
		kvs = append(kvs, tags.KV{Key: k, Value: v})

		return true
	})

	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

	return kvs
}

// DescribeTags returns tags for the specified resource ARNs.
func (b *InMemoryBackend) DescribeTags(resourceArns []string) (map[string][]tags.KV, error) {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	result := make(map[string][]tags.KV, len(resourceArns))

	for _, resArn := range resourceArns {
		t := b.findTagsLocked(resArn)
		if t != nil {
			result[resArn] = tagsToKVs(t)
		} else {
			result[resArn] = []tags.KV{}
		}
	}

	return result, nil
}

// GetResourcePolicy returns the stored resource policy for a resource ARN.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (string, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	policy, ok := b.resourcePolicies[resourceArn]
	if !ok {
		return "", ErrResourcePolicyNotFound
	}

	return policy, nil
}

// PutResourcePolicy stores a resource policy keyed by resource ARN.
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policy string) error {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	if b.resourcePolicies == nil {
		b.resourcePolicies = make(map[string]string)
	}

	b.resourcePolicies[resourceArn] = policy

	return nil
}
