package rolesanywhere

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
)

// ---- Tag handlers ----

func (h *Handler) handleTagResource(ctx context.Context, body []byte) (any, int, error) {
	var req struct {
		ResourceArn string     `json:"resourceArn"`
		Tags        []TagEntry `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if req.ResourceArn == "" || req.Tags == nil {
		return nil, 0, ErrValidation
	}

	if err := h.Backend.TagResource(ctx, req.ResourceArn, req.Tags); err != nil {
		return nil, 0, err
	}

	// Real AWS's TagResource responds 201 Created (per the service model's
	// http.responseCode: 201 on the TagResource operation), not 200 --
	// unlike every other void-result op in this service.
	return nil, http.StatusCreated, nil
}

func (h *Handler) handleUntagResource(ctx context.Context, body []byte) (any, int, error) {
	var req struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if req.ResourceArn == "" || req.TagKeys == nil {
		return nil, 0, ErrValidation
	}

	if err := h.Backend.UntagResource(ctx, req.ResourceArn, req.TagKeys); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleListTagsForResource(ctx context.Context, query string) (any, int, error) {
	// ListTagsForResource is a real-AWS query-string-bound GET request (see
	// rolesanywhere's ListTagsForResourceInput.ResourceArn httpQuery binding),
	// and query is the raw, still percent-encoded URL.RawQuery. Every real SDK
	// client percent-encodes ':' and '/' in the ARN when placing it in a query
	// value, so this MUST go through url.ParseQuery (which percent-decodes)
	// rather than a raw substring split -- splitting without decoding left
	// resourceARN holding the percent-encoded string, which never matches any
	// stored ARN and made every ListTagsForResource call 404.
	values, err := url.ParseQuery(query)
	if err != nil {
		return nil, 0, ErrValidation
	}

	resourceARN := values.Get("resourceArn")
	if resourceARN == "" {
		return nil, 0, ErrValidation
	}

	tags, err := h.Backend.ListTagsForResource(ctx, resourceARN)
	if err != nil {
		return nil, 0, err
	}

	if tags == nil {
		tags = []TagEntry{}
	}

	return map[string]any{keyTags: tags}, http.StatusOK, nil
}
