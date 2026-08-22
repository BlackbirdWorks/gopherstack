package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// aiWorkloadConfigOpsSupported returns the operations handled by
// dispatchAIWorkloadConfigOps.
func aiWorkloadConfigOpsSupported() []string {
	return []string{
		"CreateAIWorkloadConfig",
		"DescribeAIWorkloadConfig",
		"DeleteAIWorkloadConfig",
		"ListAIWorkloadConfigs",
	}
}

// dispatchAIWorkloadConfigOps handles the AIWorkloadConfig operation family.
func (h *Handler) dispatchAIWorkloadConfigOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateAIWorkloadConfig":
		r, err := h.handleCreateAIWorkloadConfig(ctx, body)

		return r, true, err
	case "DescribeAIWorkloadConfig":
		r, err := h.handleDescribeAIWorkloadConfig(ctx, body)

		return r, true, err
	case "DeleteAIWorkloadConfig":
		r, err := h.handleDeleteAIWorkloadConfig(ctx, body)

		return r, true, err
	case "ListAIWorkloadConfigs":
		r, err := h.handleListAIWorkloadConfigs(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

type createAIWorkloadConfigRequest struct {
	WorkloadSpec         json.RawMessage `json:"AIWorkloadConfigs"`
	DatasetConfig        json.RawMessage `json:"DatasetConfig"`
	AIWorkloadConfigName string          `json:"AIWorkloadConfigName"`
	Tags                 []tagObject     `json:"Tags"`
}

func (h *Handler) handleCreateAIWorkloadConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req createAIWorkloadConfigRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AIWorkloadConfigName == "" {
		return nil, fmt.Errorf("%w: AIWorkloadConfigName is required", errInvalidRequest)
	}

	c, err := h.Backend.CreateAIWorkloadConfig(ctx, CreateAIWorkloadConfigOptions{
		AIWorkloadConfigName: req.AIWorkloadConfigName,
		WorkloadSpec:         req.WorkloadSpec,
		DatasetConfig:        req.DatasetConfig,
		Tags:                 fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAIWorkloadConfigArn: c.AIWorkloadConfigArn})
}

type describeAIWorkloadConfigRequest struct {
	AIWorkloadConfigName string `json:"AIWorkloadConfigName"`
}

func (h *Handler) handleDescribeAIWorkloadConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req describeAIWorkloadConfigRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AIWorkloadConfigName == "" {
		return nil, fmt.Errorf("%w: AIWorkloadConfigName is required", errInvalidRequest)
	}

	c, err := h.Backend.DescribeAIWorkloadConfig(ctx, req.AIWorkloadConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(c)
}

type deleteAIWorkloadConfigRequest struct {
	AIWorkloadConfigName string `json:"AIWorkloadConfigName"`
}

func (h *Handler) handleDeleteAIWorkloadConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteAIWorkloadConfigRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AIWorkloadConfigName == "" {
		return nil, fmt.Errorf("%w: AIWorkloadConfigName is required", errInvalidRequest)
	}

	configARN, err := h.Backend.DeleteAIWorkloadConfig(ctx, req.AIWorkloadConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAIWorkloadConfigArn: configARN})
}

type listAIWorkloadConfigsRequest struct {
	CreationTimeAfter  *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore *float64 `json:"CreationTimeBefore"`
	NameContains       string   `json:"NameContains"`
	NextToken          string   `json:"NextToken"`
	SortBy             string   `json:"SortBy"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListAIWorkloadConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req listAIWorkloadConfigsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListAIWorkloadConfigs(ctx, ListAIWorkloadConfigsParams{
		CreationTimeAfter:  epochPtr(req.CreationTimeAfter),
		CreationTimeBefore: epochPtr(req.CreationTimeBefore),
		NameContains:       req.NameContains,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		NextToken:          req.NextToken,
		MaxResults:         req.MaxResults,
	})

	items := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		items = append(items, map[string]any{
			keyAIWorkloadConfigArn: c.AIWorkloadConfigArn,
			"AIWorkloadConfigName": c.AIWorkloadConfigName,
			keyCreationTime:        epochSeconds(c.CreationTime),
		})
	}

	return listResp("AIWorkloadConfigs", items, nextToken)
}
