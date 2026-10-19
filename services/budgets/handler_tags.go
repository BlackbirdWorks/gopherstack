package budgets

import (
	"cmp"
	"context"
	"slices"
)

type tagsReq struct {
	ResourceARN     string        `json:"ResourceARN"`
	ResourceTags    []ResourceTag `json:"ResourceTags"`
	ResourceTagKeys []string      `json:"ResourceTagKeys"`
}

type listTagsResp struct {
	ResourceTags []ResourceTag `json:"ResourceTags"`
}

func registerTagOps(ops map[string]opFunc) {
	ops["TagResource"] = bind((*Handler).tagResource)
	ops["UntagResource"] = bind((*Handler).untagResource)
	ops["ListTagsForResource"] = bind((*Handler).listTagsForResource)
}

func (h *Handler) tagResource(_ context.Context, in *tagsReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.TagResource(in.ResourceARN, in.ResourceTags)
}

func (h *Handler) untagResource(_ context.Context, in *tagsReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UntagResource(in.ResourceARN, in.ResourceTagKeys)
}

func (h *Handler) listTagsForResource(_ context.Context, in *tagsReq) (*listTagsResp, error) {
	m, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	out := make([]ResourceTag, 0, len(m))
	for k, v := range m {
		out = append(out, ResourceTag{Key: k, Value: v})
	}

	slices.SortFunc(out, func(a, b ResourceTag) int { return cmp.Compare(a.Key, b.Key) })

	return &listTagsResp{ResourceTags: out}, nil
}
