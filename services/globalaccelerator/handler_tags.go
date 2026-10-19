package globalaccelerator

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func registerTagOps(ops map[string]opFunc) {
	ops["TagResource"] = bind((*Handler).tagResource)
	ops["UntagResource"] = bind((*Handler).untagResource)
	ops["ListTagsForResource"] = bind((*Handler).listTagsForResource)
}

func (h *Handler) tagResource(_ context.Context, in *arnTagsReq) (*emptyResp, error) {
	m, err := validateTags(in.Tags)
	if err != nil {
		return nil, err
	}

	return &emptyResp{}, h.Backend.TagResource(in.ResourceArn, m)
}

func (h *Handler) untagResource(_ context.Context, in *arnTagsReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UntagResource(in.ResourceArn, in.TagKeys)
}

func (h *Handler) listTagsForResource(_ context.Context, in *arnTagsReq) (*listTagsResp, error) {
	m, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsResp{Tags: tags.MapToKV(m)}, nil
}
