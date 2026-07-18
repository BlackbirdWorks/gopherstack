package verifiedpermissions

import (
	"context"
	"fmt"
)

type tagResourceInput struct {
	Tags        map[string]string `json:"tags"`
	ResourceArn string            `json:"resourceArn"`
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*struct{}, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*struct{}, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}
