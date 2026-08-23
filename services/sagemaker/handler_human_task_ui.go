package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// HumanTaskUI handlers
// ---------------------------------------------------------------------------

// createHumanTaskUIInput mirrors CreateHumanTaskUiInput
// (api_op_CreateHumanTaskUi.go:29-46). Previously UiTemplate -- the required
// Liquid template content -- was entirely absent from decode: a client's
// worker-facing template was silently discarded and never read at all.
type createHumanTaskUIInput struct {
	HumanTaskUIName string `json:"HumanTaskUiName"`
	UITemplate      *struct {
		Content string `json:"Content"`
	} `json:"UiTemplate"`
	Tags []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateHumanTaskUI(ctx context.Context, body []byte) ([]byte, error) {
	var req createHumanTaskUIInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	if req.UITemplate == nil || req.UITemplate.Content == "" {
		return nil, fmt.Errorf("%w: UiTemplate.Content is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateHumanTaskUI(
		ctx, req.HumanTaskUIName, req.UITemplate.Content, fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyHumanTaskUIArn: result.HumanTaskUIArn})
}

// describeHumanTaskUIInput mirrors DescribeHumanTaskUiInput
// (api_op_DescribeHumanTaskUi.go:30-39).
type describeHumanTaskUIInput struct {
	HumanTaskUIName string `json:"HumanTaskUiName"`
}

func (h *Handler) handleDescribeHumanTaskUI(ctx context.Context, body []byte) ([]byte, error) {
	var req describeHumanTaskUIInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeHumanTaskUI(ctx, req.HumanTaskUIName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// deleteHumanTaskUIInput mirrors DeleteHumanTaskUiInput
// (api_op_DeleteHumanTaskUi.go:33-42).
type deleteHumanTaskUIInput struct {
	HumanTaskUIName string `json:"HumanTaskUiName"`
}

func (h *Handler) handleDeleteHumanTaskUI(ctx context.Context, body []byte) error {
	var req deleteHumanTaskUIInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	return h.Backend.DeleteHumanTaskUI(ctx, req.HumanTaskUIName)
}

// listHumanTaskUIsInput mirrors ListHumanTaskUisInput
// (api_op_ListHumanTaskUis.go:30-53). Previously this decoded only
// NextToken and dropped CreationTimeAfter/Before, MaxResults and SortOrder
// entirely -- see [ListHumanTaskUIsFilter] for the sort-key rationale.
type listHumanTaskUIsInput struct {
	CreationTimeAfter  *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore *float64 `json:"CreationTimeBefore"`
	NextToken          string   `json:"NextToken"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListHumanTaskUIs(ctx context.Context, body []byte) ([]byte, error) {
	var req listHumanTaskUIsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	uis, nextToken := h.Backend.ListHumanTaskUIs(ctx, req.NextToken, ListHumanTaskUIsFilter{
		CreationTimeAfter:  epochPtr(req.CreationTimeAfter),
		CreationTimeBefore: epochPtr(req.CreationTimeBefore),
		SortOrder:          req.SortOrder,
		MaxResults:         req.MaxResults,
	})

	items := make([]map[string]any, 0, len(uis))
	for _, u := range uis {
		items = append(items, map[string]any{
			"HumanTaskUiName": u.HumanTaskUIName,
			"HumanTaskUiArn":  u.HumanTaskUIArn,
			keyCreationTime:   epochSeconds(u.CreationTime),
		})
	}

	return listResp("HumanTaskUiSummaries", items, nextToken)
}
