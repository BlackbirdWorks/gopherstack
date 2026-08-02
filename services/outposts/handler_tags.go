package outposts

import (
	"context"
	"net/http"
)

func (h *Handler) handleListTagsForResource(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	tags, err := h.Backend.ListTagsForResource(segs[1])
	if err != nil {
		return nil, err
	}

	return marshalResponse(listTagsForResourceResponse{Tags: tags})
}

func (h *Handler) handleTagResource(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req tagResourceRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(ctx, segs[1], req.Tags); err != nil {
		return nil, err
	}

	return nil, nil // void response, matches services/grafana precedent
}

// handleUntagResource reads TagKeys from the repeated lowerCamel "tagKeys"
// query parameter -- the one exception to this service's otherwise
// PascalCase query-string casing (serializers.go:3235, verified twice --
// see wire.go's doc comment).
func (h *Handler) handleUntagResource(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	tagKeys := r.URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(ctx, segs[1], tagKeys); err != nil {
		return nil, err
	}

	return nil, nil // void response, matches services/grafana precedent
}
