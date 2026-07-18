package codeconnections

import "context"

type tagResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        []tag  `json:"Tags"`
}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*emptyOutput, error) {
	if err := h.Backend.TagResource(ctx, in.ResourceArn, tagsFromArray(in.Tags)); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*emptyOutput, error) {
	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []tag `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tagsToSortedArray(tags)}, nil
}
