package textract

import (
	"context"
	"fmt"
)

// tagResourceInput is the input for TagResource.
type tagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceARN string            `json:"ResourceARN"`
}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*emptyResponse, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(ctx, in.ResourceARN, in.Tags); err != nil {
		return nil, err
	}

	return &emptyResponse{}, nil
}

// untagResourceInput is the input for UntagResource.
type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*emptyResponse, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(ctx, in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &emptyResponse{}, nil
}

// listTagsForResourceInput is the input for ListTagsForResource.
type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
}

// listTagsForResourceResponse is the response for ListTagsForResource.
type listTagsForResourceResponse struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceResponse, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceResponse{Tags: tags}, nil
}
