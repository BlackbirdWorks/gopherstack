package ecr

import (
	"context"
)

// listTagsForResourceInput is the request body for ListTagsForResource.
type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

// tagView is a key-value tag pair.
type tagView struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type listTagsForResourceOutput struct {
	Tags []tagView `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	keys := sortedTagKeys(tags)
	result := toTagViewsForKeys(tags, keys)

	return &listTagsForResourceOutput{Tags: result}, nil
}

// tagResourceInput is the request body for TagResource.
type tagResourceInput struct {
	ResourceArn string    `json:"resourceArn"`
	Tags        []tagView `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	tagMap := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(ctx, in.ResourceArn, tagMap); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

// untagResourceInput is the request body for UntagResource.
type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

func toTagViews(tags map[string]string) []tagView {
	keys := sortedTagKeys(tags)

	return toTagViewsForKeys(tags, keys)
}

func toTagViewsForKeys(tags map[string]string, keys []string) []tagView {
	out := make([]tagView, 0, len(keys))
	for _, key := range keys {
		out = append(out, tagView{Key: key, Value: tags[key]})
	}

	return out
}
