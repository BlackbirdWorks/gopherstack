package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// AppImageConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AppImageConfigName string      `json:"AppImageConfigName"`
		Tags               []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateAppImageConfig(ctx, req.AppImageConfigName, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyAppImageConfigArn: result.AppImageConfigArn})
}

func (h *Handler) handleDescribeAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeAppImageConfig(ctx, req.AppImageConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteAppImageConfig(ctx context.Context, body []byte) error {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	return h.Backend.DeleteAppImageConfig(ctx, req.AppImageConfigName)
}

func (h *Handler) handleUpdateAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateAppImageConfig(ctx, req.AppImageConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyAppImageConfigArn: result.AppImageConfigArn})
}

func (h *Handler) handleListAppImageConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListAppImageConfigs(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		items = append(items, map[string]any{
			"AppImageConfigName": c.AppImageConfigName,
			"AppImageConfigArn":  c.AppImageConfigArn,
			keyCreationTime:      epochSeconds(c.CreationTime),
			keyLastModifiedTime:  epochSeconds(c.LastModifiedTime),
		})
	}

	return listResp("AppImageConfigs", items, nextToken)
}
