package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleListTagsForResource(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing resourceArn", errInvalidRequest)
	}

	resourceArn := segs[1]

	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: listed tags for resource", "resourceArn", resourceArn)

	return json.Marshal(map[string]any{
		"tags": tags,
	})
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleTagResource(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing resourceArn", errInvalidRequest)
	}

	resourceArn := segs[1]

	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.TagResource(resourceArn, req.Tags); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: tagged resource", "resourceArn", resourceArn)

	return nil, nil
}

func (h *Handler) handleUntagResource(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing resourceArn", errInvalidRequest)
	}

	resourceArn := segs[1]
	tagKeys := r.URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, tagKeys); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: untagged resource", "resourceArn", resourceArn)

	return nil, nil
}
