package rekognition

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) tagOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
	}
}

// --- Tag requests ---

type tagResourceReq struct {
	Tags        map[string]string `json:"Tags"`
	ResourceArn string            `json:"ResourceArn"`
}

func (h *Handler) handleTagResource(_ context.Context, req *tagResourceReq) (*struct{}, error) {
	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	if err := h.Backend.TagResource(req.ResourceArn, req.Tags); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type untagResourceReq struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, req *untagResourceReq) (*struct{}, error) {
	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	if err := h.Backend.UntagResource(req.ResourceArn, req.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listTagsForResourceReq struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceResp struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	req *listTagsForResourceReq,
) (*listTagsForResourceResp, error) {
	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceArn)
	if err != nil {
		return nil, err
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return &listTagsForResourceResp{Tags: tags}, nil
}
