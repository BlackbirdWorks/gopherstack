package docdb

import (
	"context"
	"sort"
)

func (b *InMemoryBackend) AddTagsToResource(ctx context.Context, arnStr string, tags []Tag) error {
	if err := validateTagList(tags); err != nil {
		return err
	}
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()
	tagStore := b.tagsStore(region)
	current := tagStore[arnStr]
	idx := make(map[string]int, len(current))
	for i, t := range current {
		idx[t.Key] = i
	}
	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			current[i].Value = t.Value
		} else {
			idx[t.Key] = len(current)
			current = append(current, t)
		}
	}
	tagStore[arnStr] = current

	return nil
}

func (b *InMemoryBackend) RemoveTagsFromResource(ctx context.Context, arnStr string, keys []string) {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()
	tagStore := b.tagsStore(region)
	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}
	current := tagStore[arnStr]
	kept := make([]Tag, 0, len(current))
	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}
	tagStore[arnStr] = kept
}

func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, arnStr string) []Tag {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	src := b.tagsStoreRO(region)[arnStr]
	cp := make([]Tag, len(src))
	copy(cp, src)
	sort.Slice(cp, func(i, j int) bool {
		return cp[i].Key < cp[j].Key
	})

	return cp
}
