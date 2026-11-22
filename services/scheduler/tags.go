package scheduler

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every schedule and schedule group that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every schedule and schedule group, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, v := range b.schedules.All() {
		out = appendEntry(out, v.ARN, v.Tags, taggedOnly)
	}

	for _, v := range b.scheduleGroups.All() {
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

func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, kv map[string]string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if s := b.scheduleByARN(region, resourceARN); s != nil {
		s.Tags.Merge(kv)

		return nil
	}

	if g := b.scheduleGroupByARN(region, resourceARN); g != nil {
		g.Tags.Merge(kv)

		return nil
	}

	return notFound("Resource", resourceARN)
}

func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if s := b.scheduleByARN(region, resourceARN); s != nil {
		s.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if g := b.scheduleGroupByARN(region, resourceARN); g != nil {
		g.Tags.DeleteKeys(tagKeys)

		return nil
	}

	return notFound("Resource", resourceARN)
}

func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if s := b.scheduleByARN(region, resourceARN); s != nil {
		return s.Tags.Clone(), nil
	}

	if g := b.scheduleGroupByARN(region, resourceARN); g != nil {
		return g.Tags.Clone(), nil
	}

	return nil, notFound("Resource", resourceARN)
}
