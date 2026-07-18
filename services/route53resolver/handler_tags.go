package route53resolver

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []svcTags.KV `json:"Tags"`
}

// handleListTagsForResource returns tags for the given resource ARN.
func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	kvs := h.Backend.ListTagsForResource(ctx, in.ResourceArn)

	return &listTagsForResourceOutput{Tags: kvs}, nil
}

type tagResourceInput struct {
	ResourceArn string       `json:"ResourceArn"`
	Tags        []svcTags.KV `json:"Tags"`
}

type tagResourceOutput struct{}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(ctx, in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// --- New operations ---

func (h *Handler) opsTags() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
	}
}
