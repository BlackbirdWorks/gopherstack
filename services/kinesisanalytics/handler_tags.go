package kinesisanalytics

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceARN == "" {
		return nil, errResourceARN
	}

	tagMap, err := h.Backend.ListTagsForResource(ctx, in.ResourceARN)
	if err != nil {
		return nil, err
	}

	keys := collections.SortedKeys(tagMap)

	entries := make([]tagEntry, 0, len(keys))

	for _, k := range keys {
		entries = append(entries, tagEntry{Key: k, Value: tagMap[k]})
	}

	return &listTagsForResourceOutput{Tags: entries}, nil
}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*struct{}, error) {
	if in.ResourceARN == "" {
		return nil, errResourceARN
	}

	tagMap := make(map[string]string, len(in.Tags))

	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(ctx, in.ResourceARN, tagMap); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*struct{}, error) {
	if in.ResourceARN == "" {
		return nil, errResourceARN
	}

	if err := h.Backend.UntagResource(ctx, in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
