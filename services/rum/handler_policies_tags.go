package rum

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const keyPolicyRevisionID = "PolicyRevisionId"

func (h *Handler) policyOps(ops map[string]restroute.OpFunc) {
	ops["PutResourcePolicy"] = h.putResourcePolicy
	ops["GetResourcePolicy"] = h.getResourcePolicy
	ops["DeleteResourcePolicy"] = h.deleteResourcePolicy
}

func (h *Handler) tagOps(ops map[string]restroute.OpFunc) {
	ops["TagResource"] = h.tagResource
	ops["UntagResource"] = h.untagResource
	ops["ListTagsForResource"] = h.listTags
}

func (h *Handler) putResourcePolicy(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		PolicyDocument   string `json:"PolicyDocument"`
		PolicyRevisionID string `json:"PolicyRevisionId"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	doc, rev, err := h.Backend.PutResourcePolicy(ctx, req.Params[paramName], in.PolicyDocument, in.PolicyRevisionID)
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{"PolicyDocument": doc, keyPolicyRevisionID: rev}, 0, nil
}

func (h *Handler) getResourcePolicy(ctx context.Context, req *restroute.Request) (any, int, error) {
	doc, rev, err := h.Backend.GetResourcePolicy(ctx, req.Params[paramName])
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{"PolicyDocument": doc, keyPolicyRevisionID: rev}, 0, nil
}

func (h *Handler) deleteResourcePolicy(ctx context.Context, req *restroute.Request) (any, int, error) {
	rev, err := h.Backend.DeleteResourcePolicy(ctx, req.Params[paramName], req.Query.Get("policyRevisionId"))
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{keyPolicyRevisionID: rev}, 0, nil
}

func (h *Handler) tagResource(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Tags map[string]string `json:"Tags"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	return nil, 0, h.Backend.TagResource(ctx, req.Params[paramResourceARN], in.Tags)
}

func (h *Handler) untagResource(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.UntagResource(ctx, req.Params[paramResourceARN], req.Query["tagKeys"])
}

func (h *Handler) listTags(ctx context.Context, req *restroute.Request) (any, int, error) {
	tags, err := h.Backend.ListTagsForResource(ctx, req.Params[paramResourceARN])
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"ResourceArn": req.Params[paramResourceARN], "Tags": tags}, 0, nil
}
