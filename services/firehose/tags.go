package firehose

import (
	"context"
	"fmt"
)

// ListTagsForDeliveryStream returns tags for a delivery stream.
func (b *InMemoryBackend) ListTagsForDeliveryStream(ctx context.Context, name string) (map[string]string, error) {
	b.mu.RLock("ListTagsForDeliveryStream")
	defer b.mu.RUnlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	return s.Tags.Clone(), nil
}

// TagDeliveryStream adds or updates tags on a delivery stream.
func (b *InMemoryBackend) TagDeliveryStream(ctx context.Context, name string, kv map[string]string) error {
	b.mu.Lock("TagDeliveryStream")
	defer b.mu.Unlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	s.Tags.Merge(kv)

	return nil
}

// UntagDeliveryStream removes tag keys from a delivery stream.
func (b *InMemoryBackend) UntagDeliveryStream(ctx context.Context, name string, keys []string) error {
	b.mu.Lock("UntagDeliveryStream")
	defer b.mu.Unlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	s.Tags.DeleteKeys(keys)

	return nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingFirehose).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every delivery stream ARN, across all regions,
// that currently has at least one tag. b.streams is a single store.Table
// keyed by "region|name" (see regionKey), so All() already spans every
// region without a per-region loop.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	all := b.streams.All()
	out := make([]TaggedEntry, 0, len(all))

	for _, s := range all {
		if s.Tags == nil || s.Tags.Len() == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: s.ARN, Tags: s.Tags.Clone()})
	}

	return out
}
