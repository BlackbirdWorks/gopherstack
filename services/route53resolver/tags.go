package route53resolver

import (
	"context"
	"sort"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (b *InMemoryBackend) tagsStore(region string) map[string][]svcTags.KV {
	if b.tags[region] == nil {
		b.tags[region] = make(map[string][]svcTags.KV)
	}

	return b.tags[region]
}

// tagsStoreRO returns the region-scoped tags map for region without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty map
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) tagsStoreRO(region string) map[string][]svcTags.KV {
	if v := b.tags[region]; v != nil {
		return v
	}

	return map[string][]svcTags.KV{}
}

// TagResource adds or updates tags on a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, kvs []svcTags.KV) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tags := b.tagsStore(region)
	existing := tags[resourceARN]
	keyIdx := make(map[string]int, len(existing))
	for i, kv := range existing {
		keyIdx[kv.Key] = i
	}
	for _, kv := range kvs {
		if i, ok := keyIdx[kv.Key]; ok {
			existing[i].Value = kv.Value
		} else {
			existing = append(existing, kv)
			keyIdx[kv.Key] = len(existing) - 1
		}
	}
	sort.Slice(existing, func(i, j int) bool { return existing[i].Key < existing[j].Key })
	tags[resourceARN] = existing

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tags := b.tagsStore(region)
	existing := tags[resourceARN]
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}
	remaining := make([]svcTags.KV, 0, len(existing))
	for _, kv := range existing {
		if !keySet[kv.Key] {
			remaining = append(remaining, kv)
		}
	}
	tags[resourceARN] = remaining

	return nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every Route 53 Resolver resource ARN that
// currently has at least one tag, across every region (unlike
// TagResource/UntagResource/ListTagsForResource, which are scoped to the
// caller's own region via getRegion).
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for _, regionTags := range b.tags {
		for resourceARN, kvs := range regionTags {
			if len(kvs) == 0 {
				continue
			}

			m := make(map[string]string, len(kvs))
			for _, kv := range kvs {
				m[kv.Key] = kv.Value
			}

			out = append(out, TaggedEntry{ARN: resourceARN, Tags: m})
		}
	}

	return out
}

// ListTagsForResource returns the tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) []svcTags.KV {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	kvs := b.tagsStoreRO(region)[resourceARN]
	if len(kvs) == 0 {
		return []svcTags.KV{}
	}
	cp := make([]svcTags.KV, len(kvs))
	copy(cp, kvs)

	return cp
}
