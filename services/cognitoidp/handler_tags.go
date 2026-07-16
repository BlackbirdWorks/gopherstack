package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags := h.Backend.ListTagsForResource(in.ResourceArn)
	if tags == nil {
		tags = map[string]string{}
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	h.Backend.TagResource(in.ResourceArn, in.Tags)

	return &tagResourceOutput{}, nil
}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*untagResourceOutput, error) {
	h.Backend.UntagResource(in.ResourceArn, in.TagKeys)

	return &untagResourceOutput{}, nil
}

func (h *Handler) tagsOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
	}
}
