package s3files

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxTags        = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
)

func validateTags(m map[string]string) error {
	if len(m) > maxTags {
		return validation("A resource supports at most 50 tags")
	}

	for k, v := range m {
		if k == "" || len(k) > maxTagKeyLen || len(v) > maxTagValueLen ||
			strings.HasPrefix(strings.ToLower(k), "aws:") {
			return validation("Invalid tag " + k)
		}
	}

	return nil
}

// tagsOf returns the mutable tag map for a file system or access point ID. Callers hold the lock.
func (b *InMemoryBackend) tagsOf(region, ref string) (*map[string]string, error) {
	if fs, err := b.liveFileSystem(region, ref); err == nil {
		return &fs.Tags, nil
	}

	if ap, err := b.liveAccessPoint(region, ref); err == nil {
		return &ap.Tags, nil
	}

	return nil, notFound("Resource " + ref + " not found")
}

// TagResource adds or overwrites tags on a file system or access point.
func (b *InMemoryBackend) TagResource(ctx context.Context, ref string, add map[string]string) error {
	if len(add) == 0 {
		return validation("Tags are required")
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	b.sweepLocked()

	m, err := b.tagsOf(region, ref)
	if err != nil {
		return err
	}

	merged := maps.Clone(*m)
	if merged == nil {
		merged = map[string]string{}
	}

	maps.Copy(merged, add)

	if err = validateTags(merged); err != nil {
		return err
	}

	*m = merged

	return nil
}

// UntagResource removes tags by key; absent keys are ignored.
func (b *InMemoryBackend) UntagResource(ctx context.Context, ref string, keys []string) error {
	if len(keys) == 0 {
		return validation("TagKeys are required")
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	b.sweepLocked()

	m, err := b.tagsOf(region, ref)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(*m, k)
	}

	return nil
}

// KV is a tag key/value pair.
type KV struct{ Key, Value string }

// ListTagsForResource lists a resource's tags sorted by key.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	ref string,
	maxResults int,
	token string,
) (page.Page[KV], error) {
	if err := validatePaging(maxResults, token); err != nil {
		return page.Page[KV]{}, err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	m, err := b.tagsOf(region, ref)
	if err != nil {
		return page.Page[KV]{}, err
	}

	keys := slices.Sorted(maps.Keys(*m))
	all := make([]KV, 0, len(keys))

	for _, k := range keys {
		all = append(all, KV{Key: k, Value: (*m)[k]})
	}

	return page.New(all, token, maxResults, defaultPageSize), nil
}

func sortedKV(m map[string]string) []KV {
	out := make([]KV, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, KV{Key: k, Value: m[k]})
	}

	return out
}
