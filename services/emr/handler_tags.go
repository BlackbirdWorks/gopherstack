package emr

import (
	"context"
)

// --- AddTags ---

type addTagsInput struct {
	ResourceID string `json:"ResourceId"`
	Tags       []Tag  `json:"Tags"`
}

func (h *Handler) handleAddTags(ctx context.Context, in *addTagsInput) (*emptyOutput, error) {
	if err := h.Backend.AddTags(ctx, in.ResourceID, in.Tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- RemoveTags ---

type removeTagsInput struct {
	ResourceID string   `json:"ResourceId"`
	TagKeys    []string `json:"TagKeys"`
}

func (h *Handler) handleRemoveTags(ctx context.Context, in *removeTagsInput) (*emptyOutput, error) {
	if err := h.Backend.RemoveTags(ctx, in.ResourceID, in.TagKeys); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- ListTagsForResource ---

type listTagsForResourceInput struct {
	ResourceID string `json:"ResourceId"`
}

type listTagsForResourceOutput struct {
	Tags []Tag `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceID)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}
