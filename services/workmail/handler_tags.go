package workmail

import (
	"context"
)

// ---- Tags ----

type tagReq struct {
	ResourceARN string `json:"ResourceARN"`
	Tags        []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, req *tagReq) (*emptyResp, error) {
	tags := make([]Tag, 0, len(req.Tags))
	for _, t := range req.Tags {
		tags = append(tags, Tag{Key: t.Key, Value: t.Value})
	}
	if err := h.Backend.TagResource(req.ResourceARN, tags); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type untagReq struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, req *untagReq) (*emptyResp, error) {
	if err := h.Backend.UntagResource(req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listTagsReq struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsResp struct {
	Tags []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, req *listTagsReq) (*listTagsResp, error) {
	tags, err := h.Backend.ListTagsForResource(req.ResourceARN)
	if err != nil {
		return nil, err
	}

	tresp := make([]struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	}, 0, len(tags))
	for _, t := range tags {
		tresp = append(tresp, struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		}{Key: t.Key, Value: t.Value})
	}

	return &listTagsResp{Tags: tresp}, nil
}
