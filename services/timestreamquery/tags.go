package timestreamquery

import (
	"context"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// TagResource adds tags to a resource identified by its ARN.
// The ARN self-encodes the region, so no separate region resolution is needed.
func (b *InMemoryBackend) TagResource(_ context.Context, arnStr string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	sq, err := b.lookupByARN(arnStr)
	if err != nil {
		return err
	}

	maps.Copy(sq.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
// The ARN self-encodes the region, so no separate region resolution is needed.
func (b *InMemoryBackend) UntagResource(_ context.Context, arnStr string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	sq, err := b.lookupByARN(arnStr)
	if err != nil {
		return err
	}

	for _, k := range tagKeys {
		delete(sq.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource identified by its ARN.
// The ARN self-encodes the region, so no separate region resolution is needed.
func (b *InMemoryBackend) ListTagsForResource(_ context.Context, arnStr string) ([]map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	sq, err := b.lookupByARN(arnStr)
	if err != nil {
		return nil, err
	}

	keys := collections.SortedKeys(sq.Tags)

	out := make([]map[string]string, 0, len(keys))

	for _, k := range keys {
		out = append(out, map[string]string{"Key": k, "Value": sq.Tags[k]})
	}

	return out, nil
}
