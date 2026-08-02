package networkmanager

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// taggingRoutes wires PARITY.md family X (3 ops) -- the generic
// /tags/{ResourceArn} path shared by all 9 taggable resource kinds.
func (h *Handler) taggingRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segTags, paramResourceArn},
			op:      "TagResource",
			fn:      h.dispatchTagResource,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segTags, paramResourceArn},
			op:      "UntagResource",
			fn:      h.dispatchUntagResource,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segTags, paramResourceArn},
			op:      "ListTagsForResource",
			fn:      h.dispatchListTagsForResource,
		},
	}
}

func (h *Handler) dispatchTagResource(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req tagResourceReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(params["ResourceArn"], tags.MapFromKV(req.Tags)); err != nil {
		return nil, err
	}

	return []byte("{}"), nil
}

// dispatchUntagResource reads TagKeys from the repeated "tagKeys" query
// parameter, NOT the JSON body -- confirmed by direct SDK read
// (awsRestjson1_serializeOpHttpBindingsUntagResourceInput uses
// encoder.AddQuery, matching DisassociateLink's identical query-not-body
// trap in handler_globalnetworks.go). Caught by the round-trip test.
func (h *Handler) dispatchUntagResource(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	if err := h.Backend.UntagResource(params["ResourceArn"], r.URL.Query()["tagKeys"]); err != nil {
		return nil, err
	}

	return []byte("{}"), nil
}

func (h *Handler) dispatchListTagsForResource(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	m, err := h.Backend.ListTagsForResource(params["ResourceArn"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(listTagsForResourceResponse{TagList: tags.MapToKV(m)})
}
