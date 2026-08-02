package grafana

import (
	"context"
	"encoding/json"
	"net/http"
)

// tagResourceRequest mirrors TagResourceInput's JSON body.
type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	resourceArn := segs[1]

	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"tags": tags})
}

func (h *Handler) handleTagResource(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	resourceArn := segs[1]

	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, validationError("invalid request body: " + err.Error())
	}

	if err := h.Backend.TagResource(ctx, resourceArn, req.Tags); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleUntagResource(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	resourceArn := segs[1]
	tagKeys := r.URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(ctx, resourceArn, tagKeys); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}
