package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// HumanTaskUI handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateHumanTaskUI(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags            map[string]string `json:"Tags"`
		HumanTaskUIName string            `json:"HumanTaskUiName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateHumanTaskUI(ctx, req.HumanTaskUIName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyHumanTaskUIArn: result.HumanTaskUIArn})
}

func (h *Handler) handleDescribeHumanTaskUI(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		HumanTaskUIName string `json:"HumanTaskUiName"`
	}

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

func (h *Handler) handleDeleteHumanTaskUI(ctx context.Context, body []byte) error {
	var req struct {
		HumanTaskUIName string `json:"HumanTaskUiName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	return h.Backend.DeleteHumanTaskUI(ctx, req.HumanTaskUIName)
}

func (h *Handler) handleListHumanTaskUIs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	uis, nextToken := h.Backend.ListHumanTaskUIs(ctx, req.NextToken)

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
