package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// StudioLifecycleConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateStudioLifecycleConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                         map[string]string `json:"Tags"`
		StudioLifecycleConfigName    string            `json:"StudioLifecycleConfigName"`
		StudioLifecycleConfigAppType string            `json:"StudioLifecycleConfigAppType"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateStudioLifecycleConfig(ctx,
		req.StudioLifecycleConfigName, req.StudioLifecycleConfigAppType, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyStudioLifecycleConfigArn: result.StudioLifecycleConfigArn})
}

func (h *Handler) handleDescribeStudioLifecycleConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		StudioLifecycleConfigName string `json:"StudioLifecycleConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeStudioLifecycleConfig(ctx, req.StudioLifecycleConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteStudioLifecycleConfig(ctx context.Context, body []byte) error {
	var req struct {
		StudioLifecycleConfigName string `json:"StudioLifecycleConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	return h.Backend.DeleteStudioLifecycleConfig(ctx, req.StudioLifecycleConfigName)
}

func (h *Handler) handleListStudioLifecycleConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListStudioLifecycleConfigs(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		items = append(items, map[string]any{
			"StudioLifecycleConfigName": c.StudioLifecycleConfigName,
			"StudioLifecycleConfigArn":  c.StudioLifecycleConfigArn,
			keyCreationTime:             epochSeconds(c.CreationTime),
			keyLastModifiedTime:         epochSeconds(c.LastModifiedTime),
		})
	}

	return listResp("StudioLifecycleConfigs", items, nextToken)
}
