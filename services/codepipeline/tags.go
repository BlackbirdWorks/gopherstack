package codepipeline

import (
	"context"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// resolveResourceARN looks up a resource by ARN within the given region, returning
// its type ("pipeline", "webhook", or "actiontype") and name. The ARN's region
// segment is used when present so callers cannot resolve resources outside
// their region. Returns ErrNotFound if unknown. Callers must hold b.mu.
func (b *InMemoryBackend) resolveResourceARN(region, resourceARN string) (string, string, error) {
	if matches := b.pipelinesByARN.Get(regionKey(region, resourceARN)); len(matches) > 0 {
		return kindPipeline, matches[0].Declaration.Name, nil
	}

	if matches := b.webhooksByARN.Get(regionKey(region, resourceARN)); len(matches) > 0 {
		return kindWebhook, matches[0].Name, nil
	}

	if matches := b.customActionTypesByARN.Get(regionKey(region, resourceARN)); len(matches) > 0 {
		key := customActionTypeKey{
			Category: matches[0].Category,
			Provider: matches[0].Provider,
			Version:  matches[0].Version,
		}

		return kindActionType, key.String(), nil
	}

	return "", "", ErrNotFound
}

// ListTagsForResource returns the sorted tags for a pipeline by ARN.
// Returns ResourceNotFoundException when the ARN refers to a non-pipeline resource.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceARN string,
) ([]Tag, error) {
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
	case kindActionType:
		cat, _ := b.customActionTypes.Get(regionKey(region, name))

		return tagsToSortedSlice(cat.Tags), nil
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
	case kindActionType:
		cat, _ := b.customActionTypes.Get(regionKey(region, name))
		if cat.Tags == nil {
			cat.Tags = make(map[string]string)
		}

		for _, t := range tags {
			cat.Tags[t.Key] = t.Value
		}
	default:
		return fmt.Errorf("%w: ARN %q is not a taggable resource", ErrResourceNotFound, resourceARN)
	}

	return nil
}

// UntagResource removes tags from a pipeline by ARN.
func (b *InMemoryBackend) UntagResource(
	ctx context.Context,
	resourceARN string,
	tagKeys []string,
) error {
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
	case kindActionType:
		cat, _ := b.customActionTypes.Get(regionKey(region, name))
		for _, k := range tagKeys {
			delete(cat.Tags, k)
		}
	default:
		return fmt.Errorf("%w: ARN %q is not a taggable resource", ErrResourceNotFound, resourceARN)
	}

	return nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every pipeline and webhook ARN that currently has
// at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	pipelines := b.pipelines.All()
	webhooks := b.webhooks.All()
	actionTypes := b.customActionTypes.All()
	out := make([]TaggedEntry, 0, len(pipelines)+len(webhooks)+len(actionTypes))

	for _, p := range pipelines {
		if len(p.Tags) == 0 {
			continue
		}

		cp := make(map[string]string, len(p.Tags))
		maps.Copy(cp, p.Tags)
		out = append(out, TaggedEntry{ARN: p.Metadata.PipelineArn, Tags: cp})
	}

	for _, wh := range webhooks {
		if len(wh.Tags) == 0 {
			continue
		}

		cp := make(map[string]string, len(wh.Tags))
		maps.Copy(cp, wh.Tags)
		out = append(out, TaggedEntry{ARN: wh.ARN, Tags: cp})
	}

	for _, cat := range actionTypes {
		if len(cat.Tags) == 0 {
			continue
		}

		cp := make(map[string]string, len(cat.Tags))
		maps.Copy(cp, cat.Tags)
		out = append(out, TaggedEntry{ARN: cat.arn, Tags: cp})
	}

	return out
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
