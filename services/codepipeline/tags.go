package codepipeline

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// resolveResourceARN looks up a resource by ARN within the given region, returning
// its type ("pipeline" or "webhook") and name. The ARN's region segment is used
// when present so callers cannot resolve resources outside their region. Returns
// ErrNotFound if unknown. Callers must hold b.mu.
func (b *InMemoryBackend) resolveResourceARN(region, resourceARN string) (string, string, error) {
	if matches := b.pipelinesByARN.Get(regionKey(region, resourceARN)); len(matches) > 0 {
		return kindPipeline, matches[0].Declaration.Name, nil
	}

	if matches := b.webhooksByARN.Get(regionKey(region, resourceARN)); len(matches) > 0 {
		return kindWebhook, matches[0].Name, nil
	}

	return "", "", ErrNotFound
}

// ListTagsForResource returns the sorted tags for a pipeline by ARN.
// Returns ResourceNotFoundException when the ARN refers to a non-pipeline resource.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	kind, name, err := b.resolveResourceARN(region, resourceARN)
	if err != nil {
		return nil, err
	}

	switch kind {
	case kindPipeline:
		p, _ := b.pipelines.Get(regionKey(region, name))

		return tagsToSortedSlice(p.Tags), nil
	case kindWebhook:
		wh, _ := b.webhooks.Get(regionKey(region, name))

		return tagsToSortedSlice(wh.Tags), nil
	default:
		return nil, fmt.Errorf("%w: ARN %q", ErrResourceNotFound, resourceARN)
	}
}

// TagResource adds or updates tags on a pipeline by ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	kind, name, err := b.resolveResourceARN(region, resourceARN)
	if err != nil {
		return err
	}

	switch kind {
	case kindPipeline:
		p, _ := b.pipelines.Get(regionKey(region, name))
		if p.Tags == nil {
			p.Tags = make(map[string]string)
		}

		for _, t := range tags {
			p.Tags[t.Key] = t.Value
		}
	case kindWebhook:
		wh, _ := b.webhooks.Get(regionKey(region, name))
		if wh.Tags == nil {
			wh.Tags = make(map[string]string)
		}

		for _, t := range tags {
			wh.Tags[t.Key] = t.Value
		}
	default:
		return fmt.Errorf("%w: ARN %q is not a taggable resource", ErrResourceNotFound, resourceARN)
	}

	return nil
}

// UntagResource removes tags from a pipeline by ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	kind, name, err := b.resolveResourceARN(region, resourceARN)
	if err != nil {
		return err
	}

	switch kind {
	case kindPipeline:
		p, _ := b.pipelines.Get(regionKey(region, name))
		for _, k := range tagKeys {
			delete(p.Tags, k)
		}
	case kindWebhook:
		wh, _ := b.webhooks.Get(regionKey(region, name))
		for _, k := range tagKeys {
			delete(wh.Tags, k)
		}
	default:
		return fmt.Errorf("%w: ARN %q is not a taggable resource", ErrResourceNotFound, resourceARN)
	}

	return nil
}

// tagsToSortedSlice converts a tag map to a deterministically-sorted slice of Tag.
func tagsToSortedSlice(kv map[string]string) []Tag {
	keys := collections.SortedKeys(kv)

	tags := make([]Tag, 0, len(kv))
	for _, k := range keys {
		tags = append(tags, Tag{Key: k, Value: kv[k]})
	}

	return tags
}
