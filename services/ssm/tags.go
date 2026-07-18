package ssm

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (b *InMemoryBackend) tagsStore(region string) map[string]*tags.Tags {
	return b.tags[region]
}
func (b *InMemoryBackend) miscResourceTagsStore(region string) map[string]map[string]string {
	return b.miscResourceTags[region]
}

// AddTagsToResource adds or updates tags for a resource.
func (b *InMemoryBackend) AddTagsToResource(
	ctx context.Context,
	input *AddTagsToResourceInput,
) error {
	region := getRegion(ctx)

	if input.ResourceType == resourceTypeParameter || input.ResourceType == "" {
		b.mu.Lock("AddTagsToResource")
		defer b.mu.Unlock()

		params := b.parametersStore(region)
		name := input.ResourceID
		if !params.Has(name) {
			return ErrParameterNotFound
		}
		if b.tags[region] == nil {
			b.tags[region] = make(map[string]*tags.Tags)
		}
		tagsStore := b.tagsStore(region)
		if tagsStore[name] == nil {
			tagsStore[name] = tags.New("ssm." + name + ".tags")
		}
		for _, t := range input.Tags {
			tagsStore[name].Set(t.Key, t.Value)
		}

		return nil
	}

	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	if b.miscResourceTags[region] == nil {
		b.miscResourceTags[region] = make(map[string]map[string]string)
	}
	miscTags := b.miscResourceTagsStore(region)
	if miscTags[input.ResourceID] == nil {
		miscTags[input.ResourceID] = make(map[string]string)
	}
	for _, t := range input.Tags {
		miscTags[input.ResourceID][t.Key] = t.Value
	}

	return nil
}

// RemoveTagsFromResource removes tags from a resource.
func (b *InMemoryBackend) RemoveTagsFromResource(
	ctx context.Context,
	input *RemoveTagsFromResourceInput,
) error {
	region := getRegion(ctx)

	if input.ResourceType == resourceTypeParameter || input.ResourceType == "" {
		b.mu.Lock("RemoveTagsFromResource")
		defer b.mu.Unlock()

		params := b.parametersStore(region)
		name := input.ResourceID
		if !params.Has(name) {
			return ErrParameterNotFound
		}
		tagsStore := b.tagsStore(region)
		if tagsStore[name] != nil {
			tagsStore[name].DeleteKeys(input.TagKeys)
		}

		return nil
	}

	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	miscTags := b.miscResourceTagsStore(region)
	if rt := miscTags[input.ResourceID]; rt != nil {
		for _, k := range input.TagKeys {
			delete(rt, k)
		}
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	input *ListTagsForResourceInput,
) (*ListTagsForResourceOutput, error) {
	region := getRegion(ctx)

	if input.ResourceType == resourceTypeParameter || input.ResourceType == "" {
		b.mu.RLock("ListTagsForResource")
		defer b.mu.RUnlock()

		params := b.parametersStore(region)
		name := input.ResourceID
		if !params.Has(name) {
			return nil, ErrParameterNotFound
		}
		var tagList []Tag
		tagsStore := b.tagsStore(region)
		if tagsStore[name] != nil {
			for k, v := range tagsStore[name].Clone() {
				tagList = append(tagList, Tag{Key: k, Value: v})
			}
		}
		sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

		return &ListTagsForResourceOutput{TagList: tagList}, nil
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	miscTags := b.miscResourceTagsStore(region)
	var tagList []Tag
	for k, v := range miscTags[input.ResourceID] {
		tagList = append(tagList, Tag{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	return &ListTagsForResourceOutput{TagList: tagList}, nil
}
