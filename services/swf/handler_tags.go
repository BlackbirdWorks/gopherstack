package swf

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// --- ListTagsForResource ---

type handleListTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type resourceTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type listTagsForResourceOutput struct {
	Tags []resourceTag `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *handleListTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tagMap, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	keys := collections.SortedKeys(tagMap)
	tags := make([]resourceTag, 0, len(keys))
	for _, k := range keys {
		tags = append(tags, resourceTag{Key: k, Value: tagMap[k]})
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

// --- TagResource ---

type handleTagResourceInput struct {
	ResourceArn string        `json:"resourceArn"`
	Tags        []resourceTag `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *handleTagResourceInput,
) (*tagResourceOutput, error) {
	tagMap := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}
	if err := h.Backend.TagResource(in.ResourceArn, tagMap); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

// --- UntagResource ---

type handleUntagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *handleUntagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}
