package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// AppImageConfig handlers
// ---------------------------------------------------------------------------

// createAppImageConfigInput mirrors CreateAppImageConfigInput
// (api_op_CreateAppImageConfig.go:28-52).
type createAppImageConfigInput struct {
	AppImageConfigName       string          `json:"AppImageConfigName"`
	KernelGatewayImageConfig json.RawMessage `json:"KernelGatewayImageConfig"`
	JupyterLabAppImageConfig json.RawMessage `json:"JupyterLabAppImageConfig"`
	CodeEditorAppImageConfig json.RawMessage `json:"CodeEditorAppImageConfig"`
	Tags                     []tagObject     `json:"Tags"`
}

func (h *Handler) handleCreateAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req createAppImageConfigInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateAppImageConfig(ctx, CreateAppImageConfigOptions{
		AppImageConfigName:       req.AppImageConfigName,
		Tags:                     fromTagObjects(req.Tags),
		KernelGatewayImageConfig: req.KernelGatewayImageConfig,
		JupyterLabAppImageConfig: req.JupyterLabAppImageConfig,
		CodeEditorAppImageConfig: req.CodeEditorAppImageConfig,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyAppImageConfigArn: result.AppImageConfigArn})
}

// describeAppImageConfigInput mirrors DescribeAppImageConfigInput
// (api_op_DescribeAppImageConfig.go:27-32).
type describeAppImageConfigInput struct {
	AppImageConfigName string `json:"AppImageConfigName"`
}

func (h *Handler) handleDescribeAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req describeAppImageConfigInput

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

// deleteAppImageConfigInput mirrors DeleteAppImageConfigInput
// (api_op_DeleteAppImageConfig.go:27-32).
type deleteAppImageConfigInput struct {
	AppImageConfigName string `json:"AppImageConfigName"`
}

func (h *Handler) handleDeleteAppImageConfig(ctx context.Context, body []byte) error {
	var req deleteAppImageConfigInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	return h.Backend.DeleteAppImageConfig(ctx, req.AppImageConfigName)
}

// updateAppImageConfigInput mirrors UpdateAppImageConfigInput
// (api_op_UpdateAppImageConfig.go:28-45).
type updateAppImageConfigInput struct {
	AppImageConfigName       string          `json:"AppImageConfigName"`
	KernelGatewayImageConfig json.RawMessage `json:"KernelGatewayImageConfig"`
	JupyterLabAppImageConfig json.RawMessage `json:"JupyterLabAppImageConfig"`
	CodeEditorAppImageConfig json.RawMessage `json:"CodeEditorAppImageConfig"`
}

func (h *Handler) handleUpdateAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req updateAppImageConfigInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateAppImageConfig(ctx, req.AppImageConfigName, UpdateAppImageConfigOptions{
		KernelGatewayImageConfig: req.KernelGatewayImageConfig,
		JupyterLabAppImageConfig: req.JupyterLabAppImageConfig,
		CodeEditorAppImageConfig: req.CodeEditorAppImageConfig,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyAppImageConfigArn: result.AppImageConfigArn})
}

// listAppImageConfigsInput mirrors ListAppImageConfigsInput
// (api_op_ListAppImageConfigs.go:31-64).
type listAppImageConfigsInput struct {
	CreationTimeAfter  *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore *float64 `json:"CreationTimeBefore"`
	ModifiedTimeAfter  *float64 `json:"ModifiedTimeAfter"`
	ModifiedTimeBefore *float64 `json:"ModifiedTimeBefore"`
	NameContains       string   `json:"NameContains"`
	NextToken          string   `json:"NextToken"`
	SortBy             string   `json:"SortBy"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListAppImageConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req listAppImageConfigsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListAppImageConfigs(ctx, req.NextToken, ListAppImageConfigsFilter{
		CreationTimeAfter:  epochPtr(req.CreationTimeAfter),
		CreationTimeBefore: epochPtr(req.CreationTimeBefore),
		ModifiedTimeAfter:  epochPtr(req.ModifiedTimeAfter),
		ModifiedTimeBefore: epochPtr(req.ModifiedTimeBefore),
		NameContains:       req.NameContains,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		MaxResults:         req.MaxResults,
	})

	items := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		item := map[string]any{
			"AppImageConfigName": c.AppImageConfigName,
			"AppImageConfigArn":  c.AppImageConfigArn,
			keyCreationTime:      epochSeconds(c.CreationTime),
			keyLastModifiedTime:  epochSeconds(c.LastModifiedTime),
		}

		if len(c.KernelGatewayImageConfig) > 0 {
			item["KernelGatewayImageConfig"] = c.KernelGatewayImageConfig
		}

		if len(c.JupyterLabAppImageConfig) > 0 {
			item["JupyterLabAppImageConfig"] = c.JupyterLabAppImageConfig
		}

		if len(c.CodeEditorAppImageConfig) > 0 {
			item["CodeEditorAppImageConfig"] = c.CodeEditorAppImageConfig
		}

		items = append(items, item)
	}

	return listResp("AppImageConfigs", items, nextToken)
}
