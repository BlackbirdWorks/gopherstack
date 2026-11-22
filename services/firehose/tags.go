package firehose

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
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

// TaggedResources lists every delivery stream that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every delivery stream, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, v := range b.streams.All() {
		out = appendEntry(out, v.ARN, v.Tags, taggedOnly)
	}

	return out
}

func appendEntry(out []TaggedEntry, arn string, t *tags.Tags, taggedOnly bool) []TaggedEntry {
	empty := t == nil || t.Len() == 0
	if taggedOnly && empty {
		return out
	}

	entry := TaggedEntry{ARN: arn}
	if !empty {
		entry.Tags = t.Clone()
	}

	return append(out, entry)
}
