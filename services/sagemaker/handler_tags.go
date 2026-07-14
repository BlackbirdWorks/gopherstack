package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleAddTags(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string      `json:"ResourceArn"`
		Tags        []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	if err := h.Backend.AddTags(ctx, req.ResourceArn, tags); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: added tags", "resource", req.ResourceArn)

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleListTags(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string `json:"ResourceArn"`
		NextToken   string `json:"NextToken"`
	}

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

	allTags := toTagObjects(tags)
	startIdx := parseNextToken(req.NextToken)
	if startIdx >= len(allTags) {
		return json.Marshal(map[string]any{"Tags": []tagObject{}})
	}
	end := startIdx + sagemakerDefaultPageSize
	var outToken string
	if end < len(allTags) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(allTags)
	}

	resp := map[string]any{"Tags": allTags[startIdx:end]}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteTags(ctx context.Context, body []byte) error {
	var req struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTags(ctx, req.ResourceArn, req.TagKeys); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted tags", "resource", req.ResourceArn)

	return nil
}
