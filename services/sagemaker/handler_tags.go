package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// addTagsRequest is the request body for AddTags.
type addTagsRequest struct {
	ResourceArn string      `json:"ResourceArn"`
	Tags        []tagObject `json:"Tags"`
}

// listTagsRequest is the request body for ListTags.
type listTagsRequest struct {
	ResourceArn string `json:"ResourceArn"`
	NextToken   string `json:"NextToken"`
	MaxResults  int32  `json:"MaxResults,omitempty"`
}

// deleteTagsRequest is the request body for DeleteTags.
type deleteTagsRequest struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleAddTags(ctx context.Context, body []byte) ([]byte, error) {
	var req addTagsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if len(req.Tags) == 0 {
		return nil, fmt.Errorf("%w: Tags is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	if err := h.Backend.AddTags(ctx, req.ResourceArn, tags); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: added tags", "resource", req.ResourceArn)

	allTags, err := h.Backend.ListTags(ctx, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Tags": toTagObjects(allTags)})
}

func (h *Handler) handleListTags(ctx context.Context, body []byte) ([]byte, error) {
	var req listTagsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTags(ctx, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	page, outToken := paginateSlice(toTagObjects(tags), req.NextToken, req.MaxResults)

	resp := map[string]any{"Tags": page}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteTags(ctx context.Context, body []byte) error {
	var req deleteTagsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if len(req.TagKeys) == 0 {
		return fmt.Errorf("%w: TagKeys is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTags(ctx, req.ResourceArn, req.TagKeys); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted tags", "resource", req.ResourceArn)

	return nil
}
