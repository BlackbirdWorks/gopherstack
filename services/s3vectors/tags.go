package s3vectors

import (
	"maps"
	"strings"
)

// taggable returns a pointer to the tag map of the bucket or index named by resourceARN.
func (b *InMemoryBackend) taggable(resourceARN string) (*map[string]string, error) {
	parsed, ok := parseARN(resourceARN)
	rest, found := strings.CutPrefix(parsed.resource, "bucket/")

	if !ok || !found {
		return nil, invalid("resourceArn %q is not a vector bucket or index ARN", resourceARN)
	}

	bname, iname, isIndex := strings.Cut(rest, "/index/")
	if !isIndex {
		if bk, exists := b.buckets.Get(bucketID(parsed.account, parsed.region, bname)); exists {
			return &bk.Tags, nil
		}

		return nil, notFound("The specified vector bucket could not be found")
	}

	if ix, exists := b.indexes.Get(indexID(parsed.account, parsed.region, bname, iname)); exists {
		return &ix.Tags, nil
	}

	return nil, notFound("The specified vector index could not be found")
}

func (b *InMemoryBackend) tagResource(resourceARN string, add map[string]string) (any, error) {
	if len(add) == 0 {
		return nil, invalid("tags must not be empty")
	}

	if err := validateTags(add); err != nil {
		return nil, err
	}

	b.mu.Lock("tagResource")
	defer b.mu.Unlock()

	tags, err := b.taggable(resourceARN)
	if err != nil {
		return nil, err
	}

	merged := map[string]string{}
	maps.Copy(merged, *tags)

	maps.Copy(merged, add)

	if err = validateTags(merged); err != nil {
		return nil, err
	}

	*tags = merged

	return struct{}{}, nil
}

func (b *InMemoryBackend) untagResource(resourceARN string, keys []string) (any, error) {
	if len(keys) == 0 {
		return nil, invalid("tagKeys must not be empty")
	}

	b.mu.Lock("untagResource")
	defer b.mu.Unlock()

	tags, err := b.taggable(resourceARN)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		delete(*tags, k)
	}

	return struct{}{}, nil
}

func (b *InMemoryBackend) listTags(resourceARN string) (any, error) {
	b.mu.RLock("listTags")
	defer b.mu.RUnlock()

	tags, err := b.taggable(resourceARN)
	if err != nil {
		return nil, err
	}

	out := map[string]string{}
	maps.Copy(out, *tags)

	return map[string]any{"tags": out}, nil
}
