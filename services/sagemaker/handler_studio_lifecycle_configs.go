package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// StudioLifecycleConfig handlers
// ---------------------------------------------------------------------------

// createStudioLifecycleConfigInput mirrors CreateStudioLifecycleConfigInput
// (api_op_CreateStudioLifecycleConfig.go:28-52).
type createStudioLifecycleConfigInput struct {
	StudioLifecycleConfigName    string      `json:"StudioLifecycleConfigName"`
	StudioLifecycleConfigAppType string      `json:"StudioLifecycleConfigAppType"`
	StudioLifecycleConfigContent string      `json:"StudioLifecycleConfigContent"`
	Tags                         []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateStudioLifecycleConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req createStudioLifecycleConfigInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	if req.StudioLifecycleConfigAppType == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigAppType is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateStudioLifecycleConfig(ctx,
		req.StudioLifecycleConfigName, req.StudioLifecycleConfigAppType, req.StudioLifecycleConfigContent,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyStudioLifecycleConfigArn: result.StudioLifecycleConfigArn})
}

// describeStudioLifecycleConfigInput mirrors DescribeStudioLifecycleConfigInput
// (api_op_DescribeStudioLifecycleConfig.go:29-37).
type describeStudioLifecycleConfigInput struct {
	StudioLifecycleConfigName string `json:"StudioLifecycleConfigName"`
}

func (h *Handler) handleDescribeStudioLifecycleConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req describeStudioLifecycleConfigInput

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

// deleteStudioLifecycleConfigInput mirrors DeleteStudioLifecycleConfigInput
// (api_op_DeleteStudioLifecycleConfig.go:30-38).
type deleteStudioLifecycleConfigInput struct {
	StudioLifecycleConfigName string `json:"StudioLifecycleConfigName"`
}

func (h *Handler) handleDeleteStudioLifecycleConfig(ctx context.Context, body []byte) error {
	var req deleteStudioLifecycleConfigInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	return h.Backend.DeleteStudioLifecycleConfig(ctx, req.StudioLifecycleConfigName)
}

// listStudioLifecycleConfigsInput mirrors ListStudioLifecycleConfigsInput
// (api_op_ListStudioLifecycleConfigs.go:31-75). Previously this decoded only
// NextToken and dropped every filter and sort control the op's own request
// shape declares.
type listStudioLifecycleConfigsInput struct {
	AppTypeEquals      string   `json:"AppTypeEquals"`
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

func (h *Handler) handleListStudioLifecycleConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req listStudioLifecycleConfigsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListStudioLifecycleConfigs(ctx, req.NextToken, ListStudioLifecycleConfigsFilter{
		AppTypeEquals:      req.AppTypeEquals,
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
		items = append(items, map[string]any{
			"StudioLifecycleConfigName":    c.StudioLifecycleConfigName,
			"StudioLifecycleConfigArn":     c.StudioLifecycleConfigArn,
			"StudioLifecycleConfigAppType": c.StudioLifecycleConfigAppType,
			keyCreationTime:                epochSeconds(c.CreationTime),
			keyLastModifiedTime:            epochSeconds(c.LastModifiedTime),
		})
	}

	return listResp("StudioLifecycleConfigs", items, nextToken)
}
