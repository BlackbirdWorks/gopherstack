package directoryservice

import (
	"context"
	"sort"
)

// AddTagsToResource adds or updates tags on a directory.
func (b *InMemoryBackend) AddTagsToResource(
	ctx context.Context,
	resourceID string,
	tags []Tag,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, resourceID)
	if !ok {
		return ErrDirectoryNotFound
	}

	if d.Tags == nil {
		d.Tags = make(map[string]string)
	}

	for _, t := range tags {
		d.Tags[t.Key] = t.Value
	}

	return nil
}

// RemoveTagsFromResource removes tags from a directory.
func (b *InMemoryBackend) RemoveTagsFromResource(
	ctx context.Context,
	resourceID string,
	tagKeys []string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, resourceID)
	if !ok {
		return ErrDirectoryNotFound
	}

	for _, k := range tagKeys {
		delete(d.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a directory with pagination.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceID string,
	limit int32,
	nextToken string,
) ([]Tag, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	d, ok := b.directoryGet(region, resourceID)
	if !ok {
		return nil, "", ErrDirectoryNotFound
	}

	all := make([]Tag, 0, len(d.Tags))
	for k, v := range d.Tags {
		all = append(all, Tag{Key: k, Value: v})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })

	start := 0
	if nextToken != "" {
		if n, err := decodePageToken(nextToken); err == nil && n > 0 {
			start = n
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(all))
	result := all[start:end]

	var outToken string
	if end < len(all) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}
