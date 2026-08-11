package scheduler

import (
	"context"
	"fmt"
)

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every EventBridge Scheduler schedule and
// schedule-group ARN that currently has at least one tag, across every
// region (unlike TagResource/UntagResource/ListTagsForResource, which are
// scoped to the caller's own region via getRegion).
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, b.schedules.Len()+b.scheduleGroups.Len())

	for _, s := range b.schedules.All() {
		if s.Tags == nil || s.Tags.Len() == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: s.ARN, Tags: s.Tags.Clone()})
	}

	for _, g := range b.scheduleGroups.All() {
		if g.Tags == nil || g.Tags.Len() == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: g.ARN, Tags: g.Tags.Clone()})
	}

	return out
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

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
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

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
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

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}
