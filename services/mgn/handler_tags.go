package mgn

import (
	"context"
	"net/http"
)

const tagsPathSegs = 2

func (h *Handler) handleTagResource(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < tagsPathSegs {
		return nil, validationError("resourceArn is required")
	}

	var req tagResourceRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(ctx, segs[1], req.Tags); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleUntagResource(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < tagsPathSegs {
		return nil, validationError("resourceArn is required")
	}

	tagKeys := r.URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(ctx, segs[1], tagKeys); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleListTagsForResource(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < tagsPathSegs {
		return nil, validationError("resourceArn is required")
	}

	tags, err := h.Backend.ListTagsForResource(segs[1])
	if err != nil {
		return nil, err
	}

	return marshalResponse(listTagsForResourceResponse{Tags: tags})
}
