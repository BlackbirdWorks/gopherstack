package ram

import (
	"context"
	"encoding/json"
	"fmt"
)

type tagResourceRequest struct {
	ResourceShareArn string      `json:"resourceShareArn"`
	ResourceArn      string      `json:"resourceArn"`
	Tags             []tagObject `json:"tags"`
}

func (h *Handler) handleTagResource(_ context.Context, body []byte) error {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	shareARN := req.ResourceShareArn
	if shareARN == "" {
		shareARN = req.ResourceArn
	}

	if shareARN == "" {
		return fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(shareARN, fromTagObjects(req.Tags)); err != nil {
		return err
	}

	return nil
}

type untagResourceRequest struct {
	ResourceShareArn string   `json:"resourceShareArn"`
	ResourceArn      string   `json:"resourceArn"`
	TagKeys          []string `json:"tagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, body []byte) error {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	shareARN := req.ResourceShareArn
	if shareARN == "" {
		shareARN = req.ResourceArn
	}

	if shareARN == "" {
		return fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(shareARN, req.TagKeys); err != nil {
		return err
	}

	return nil
}

type listTagsForResourceRequest struct {
	ResourceShareArn string `json:"resourceShareArn"`
	ResourceArn      string `json:"resourceArn"`
}

type listTagsForResourceResponse struct {
	Tags []tagObject `json:"tags"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, body []byte) ([]byte, error) {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	shareARN := req.ResourceShareArn
	if shareARN == "" {
		shareARN = req.ResourceArn
	}

	if shareARN == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(shareARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listTagsForResourceResponse{Tags: toTagObjects(tags)})
}
