package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleTagResource handles TagResource requests.
func (h *Handler) handleTagResource(_ context.Context, body []byte) (any, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		ResourceArn string            `json:"ResourceArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.TagResource(req.ResourceArn, req.Tags); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleUntagResource handles UntagResource requests.
func (h *Handler) handleUntagResource(_ context.Context, body []byte) (any, error) {
	var req struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UntagResource(req.ResourceArn, req.TagKeys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleListTags handles ListTags requests.
func (h *Handler) handleListTags(_ context.Context, body []byte) (any, error) {
	var req struct {
		ResourceArn string `json:"ResourceArn"`
		NextToken   string `json:"NextToken"`
		MaxResults  int32  `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tags, nextToken, err := h.Backend.ListTags(req.ResourceArn, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{"Tags": tags}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return resp, nil
}
