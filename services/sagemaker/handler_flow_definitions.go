package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// FlowDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateFlowDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string `json:"Tags"`
		FlowDefinitionName string            `json:"FlowDefinitionName"`
		RoleArn            string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateFlowDefinition(ctx, req.FlowDefinitionName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyFlowDefinitionArn: result.FlowDefinitionArn})
}

func (h *Handler) handleDescribeFlowDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FlowDefinitionName string `json:"FlowDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeFlowDefinition(ctx, req.FlowDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteFlowDefinition(ctx context.Context, body []byte) error {
	var req struct {
		FlowDefinitionName string `json:"FlowDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteFlowDefinition(ctx, req.FlowDefinitionName)
}

func (h *Handler) handleListFlowDefinitions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	defs, nextToken := h.Backend.ListFlowDefinitions(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(defs))
	for _, d := range defs {
		items = append(items, map[string]any{
			"FlowDefinitionName":   d.FlowDefinitionName,
			"FlowDefinitionArn":    d.FlowDefinitionArn,
			"FlowDefinitionStatus": d.FlowDefinitionStatus,
			keyCreationTime:        epochSeconds(d.CreationTime),
		})
	}

	return listResp("FlowDefinitionSummaries", items, nextToken)
}
