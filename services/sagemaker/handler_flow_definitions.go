package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// FlowDefinition handlers
// ---------------------------------------------------------------------------

// createFlowDefinitionInput mirrors CreateFlowDefinitionInput
// (api_op_CreateFlowDefinition.go:28-66). Previously OutputConfig -- required
// -- and HumanLoopActivationConfig/HumanLoopConfig/HumanLoopRequestSource
// were entirely absent from decode: only FlowDefinitionName and RoleArn were
// ever read, so every real flow-definition-specific setting (where human
// review output lands, what triggers a human loop, who reviews and what
// they see) was silently discarded.
type createFlowDefinitionInput struct {
	FlowDefinitionName string `json:"FlowDefinitionName"`
	RoleArn            string `json:"RoleArn"`
	OutputConfig       *struct {
		S3OutputPath string `json:"S3OutputPath"`
		KmsKeyID     string `json:"KmsKeyId"`
	} `json:"OutputConfig"`
	HumanLoopActivationConfig *struct {
		HumanLoopActivationConditionsConfig *struct {
			HumanLoopActivationConditions string `json:"HumanLoopActivationConditions"`
		} `json:"HumanLoopActivationConditionsConfig"`
	} `json:"HumanLoopActivationConfig"`
	HumanLoopConfig *struct {
		HumanTaskUIArn                    string   `json:"HumanTaskUiArn"`
		WorkteamArn                       string   `json:"WorkteamArn"`
		TaskTitle                         string   `json:"TaskTitle"`
		TaskDescription                   string   `json:"TaskDescription"`
		TaskKeywords                      []string `json:"TaskKeywords"`
		TaskCount                         int32    `json:"TaskCount"`
		TaskAvailabilityLifetimeInSeconds int32    `json:"TaskAvailabilityLifetimeInSeconds"`
		TaskTimeLimitInSeconds            int32    `json:"TaskTimeLimitInSeconds"`
	} `json:"HumanLoopConfig"`
	HumanLoopRequestSource *struct {
		AwsManagedHumanLoopRequestSource string `json:"AwsManagedHumanLoopRequestSource"`
	} `json:"HumanLoopRequestSource"`
	Tags []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateFlowDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req createFlowDefinitionInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	if req.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	if req.OutputConfig == nil || req.OutputConfig.S3OutputPath == "" {
		return nil, fmt.Errorf("%w: OutputConfig.S3OutputPath is required", errInvalidRequest)
	}

	opts := FlowDefinitionOptions{
		S3OutputPath: req.OutputConfig.S3OutputPath,
		KmsKeyID:     req.OutputConfig.KmsKeyID,
	}

	if hlac := req.HumanLoopActivationConfig; hlac != nil && hlac.HumanLoopActivationConditionsConfig != nil {
		opts.HumanLoopActivationConditions = hlac.HumanLoopActivationConditionsConfig.HumanLoopActivationConditions
	}

	if hlc := req.HumanLoopConfig; hlc != nil {
		opts.HumanLoopConfig = &HumanLoopConfig{
			HumanTaskUIArn:                    hlc.HumanTaskUIArn,
			WorkteamArn:                       hlc.WorkteamArn,
			TaskTitle:                         hlc.TaskTitle,
			TaskDescription:                   hlc.TaskDescription,
			TaskCount:                         hlc.TaskCount,
			TaskAvailabilityLifetimeInSeconds: hlc.TaskAvailabilityLifetimeInSeconds,
			TaskTimeLimitInSeconds:            hlc.TaskTimeLimitInSeconds,
			TaskKeywords:                      hlc.TaskKeywords,
		}
	}

	if hlrs := req.HumanLoopRequestSource; hlrs != nil {
		opts.AwsManagedHumanLoopRequestSource = hlrs.AwsManagedHumanLoopRequestSource
	}

	result, err := h.Backend.CreateFlowDefinition(
		ctx, req.FlowDefinitionName, req.RoleArn, fromTagObjects(req.Tags), opts,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyFlowDefinitionArn: result.FlowDefinitionArn})
}

// describeFlowDefinitionInput mirrors DescribeFlowDefinitionInput
// (api_op_DescribeFlowDefinition.go:29-37).
type describeFlowDefinitionInput struct {
	FlowDefinitionName string `json:"FlowDefinitionName"`
}

func (h *Handler) handleDescribeFlowDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req describeFlowDefinitionInput

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

// deleteFlowDefinitionInput mirrors DeleteFlowDefinitionInput
// (api_op_DeleteFlowDefinition.go:27-35).
type deleteFlowDefinitionInput struct {
	FlowDefinitionName string `json:"FlowDefinitionName"`
}

func (h *Handler) handleDeleteFlowDefinition(ctx context.Context, body []byte) error {
	var req deleteFlowDefinitionInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteFlowDefinition(ctx, req.FlowDefinitionName)
}

// listFlowDefinitionsInput mirrors ListFlowDefinitionsInput
// (api_op_ListFlowDefinitions.go:30-52). Previously this decoded only
// NextToken and dropped CreationTimeAfter/Before, MaxResults and SortOrder
// entirely. Like ListHumanTaskUis, the op declares no SortBy field, so
// SortOrder is applied to this backend's own pre-existing default order
// (ascending by name) rather than an invented dimension the doc does not
// support.
type listFlowDefinitionsInput struct {
	CreationTimeAfter  *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore *float64 `json:"CreationTimeBefore"`
	NextToken          string   `json:"NextToken"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListFlowDefinitions(ctx context.Context, body []byte) ([]byte, error) {
	var req listFlowDefinitionsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	defs, nextToken := h.Backend.ListFlowDefinitions(ctx, req.NextToken, ListFlowDefinitionsFilter{
		CreationTimeAfter:  epochPtr(req.CreationTimeAfter),
		CreationTimeBefore: epochPtr(req.CreationTimeBefore),
		SortOrder:          req.SortOrder,
		MaxResults:         req.MaxResults,
	})

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
