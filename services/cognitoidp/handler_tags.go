package cognitoidp

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	resourceTags := h.Backend.ListTagsForResource(in.ResourceArn)
	if resourceTags == nil {
		resourceTags = map[string]string{}
	}

	return &listTagsForResourceOutput{Tags: resourceTags}, nil
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	if err := rejectPinnedIDTag(in.Tags); err != nil {
		return nil, err
	}

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

func rejectPinnedIDTag(m map[string]string) error {
	if _, ok := m[tags.PinnedIDKey]; ok {
		return fmt.Errorf("%w: tag %q is only valid on CreateUserPool", ErrInvalidParameter, tags.PinnedIDKey)
	}

	return nil
}
