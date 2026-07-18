package scheduler

import "context"

type handleTagResourceInput struct {
	ResourceArn string        `json:"ResourceArn"`
	Tags        []resourceTag `json:"Tags"`
}

func (h *Handler) handleTagResource(ctx context.Context, in *handleTagResourceInput) (*emptyOutput, error) {
	return voidOp(func() error { return h.Backend.TagResource(ctx, in.ResourceArn, tagsFromWire(in.Tags)) })
}

type handleListTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []resourceTag `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *handleListTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	kv, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tagsToWire(kv)}, nil
}

// handleUntagResource removes the specified tag keys from a resource.
type handleUntagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(ctx context.Context, in *handleUntagResourceInput) (*emptyOutput, error) {
	return voidOp(func() error { return h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys) })
}
