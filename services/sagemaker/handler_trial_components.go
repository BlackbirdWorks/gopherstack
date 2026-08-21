package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// TrialComponent handlers
// ---------------------------------------------------------------------------

// createTrialComponentInput mirrors CreateTrialComponentInput
// (api_op_CreateTrialComponent.go:44-91): TrialComponentName is required,
// every other member optional.
type createTrialComponentInput struct {
	StartTime          *float64                          `json:"StartTime,omitempty"`
	EndTime            *float64                          `json:"EndTime,omitempty"`
	Status             *TrialComponentStatus             `json:"Status,omitempty"`
	Parameters         map[string]TrialComponentValue    `json:"Parameters,omitempty"`
	InputArtifacts     map[string]TrialComponentArtifact `json:"InputArtifacts,omitempty"`
	OutputArtifacts    map[string]TrialComponentArtifact `json:"OutputArtifacts,omitempty"`
	MetadataProperties *MetadataProperties               `json:"MetadataProperties,omitempty"`
	TrialComponentName string                            `json:"TrialComponentName"`
	DisplayName        string                            `json:"DisplayName,omitempty"`
	Tags               []tagObject                       `json:"Tags"`
}

func (h *Handler) handleCreateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req createTrialComponentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	tc, err := h.Backend.CreateTrialComponent(ctx, CreateTrialComponentOptions{
		TrialComponentName: req.TrialComponentName,
		DisplayName:        req.DisplayName,
		StartTime:          timeFromEpochSecondsPtr(req.StartTime),
		EndTime:            timeFromEpochSecondsPtr(req.EndTime),
		Status:             req.Status,
		Parameters:         req.Parameters,
		InputArtifacts:     req.InputArtifacts,
		OutputArtifacts:    req.OutputArtifacts,
		MetadataProperties: req.MetadataProperties,
		Tags:               fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: created trial component", "name", tc.TrialComponentName)

	return json.Marshal(map[string]string{keyTrialComponentArn: tc.TrialComponentArn})
}

// describeTrialComponentInput mirrors DescribeTrialComponentInput
// (api_op_DescribeTrialComponent.go:24-33): TrialComponentName is its sole,
// required member.
type describeTrialComponentInput struct {
	TrialComponentName string `json:"TrialComponentName"`
}

// Disclosed, not modeled, on DescribeTrialComponentOutput
// (api_op_DescribeTrialComponent.go:36-83): CreatedBy/LastModifiedBy
// (types.UserContext) — this service models no caller-identity concept
// anywhere, the same gap disclosed repeatedly in prior passes; Metrics
// ([]types.TrialComponentMetricSummary) — populated only by the separate
// sagemaker-metrics service's BatchPutMetrics, not implemented here; Source/
// Sources (types.TrialComponentSource) — CreateTrialComponentInput has no
// Source field at all, and this backend never auto-tracks a processing/
// training job into a trial component, so no trial component ever has one.
func (h *Handler) handleDescribeTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req describeTrialComponentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	tc, err := h.Backend.DescribeTrialComponent(ctx, req.TrialComponentName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"TrialComponentName": tc.TrialComponentName,
		keyTrialComponentArn: tc.TrialComponentArn,
		keyCreationTime:      epochSeconds(tc.CreationTime),
		keyLastModifiedTime:  epochSeconds(tc.LastModifiedTime),
		"LineageGroupArn":    h.Backend.trialComponentLineageGroupArn(ctx),
	}
	if tc.DisplayName != "" {
		resp["DisplayName"] = tc.DisplayName
	}
	if tc.Status != nil {
		resp["Status"] = tc.Status
	}
	if tc.StartTime != nil {
		resp["StartTime"] = epochSeconds(*tc.StartTime)
	}
	if tc.EndTime != nil {
		resp["EndTime"] = epochSeconds(*tc.EndTime)
	}
	if len(tc.Parameters) > 0 {
		resp["Parameters"] = tc.Parameters
	}
	if len(tc.InputArtifacts) > 0 {
		resp["InputArtifacts"] = tc.InputArtifacts
	}
	if len(tc.OutputArtifacts) > 0 {
		resp["OutputArtifacts"] = tc.OutputArtifacts
	}
	if tc.MetadataProperties != nil {
		resp["MetadataProperties"] = tc.MetadataProperties
	}

	return json.Marshal(resp)
}

// deleteTrialComponentInput mirrors DeleteTrialComponentInput
// (api_op_DeleteTrialComponent.go:24-33): TrialComponentName is its sole,
// required member.
type deleteTrialComponentInput struct {
	TrialComponentName string `json:"TrialComponentName"`
}

func (h *Handler) handleDeleteTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteTrialComponentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	tc, err := h.Backend.DeleteTrialComponent(ctx, req.TrialComponentName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: deleted trial component", "name", req.TrialComponentName)

	return json.Marshal(map[string]string{keyTrialComponentArn: tc.TrialComponentArn})
}

// ---------------------------------------------------------------------------
// TrialComponent association extras
// ---------------------------------------------------------------------------

// disassociateTrialComponentInput mirrors DisassociateTrialComponentInput
// (api_op_DisassociateTrialComponent.go:26-38): both TrialComponentName and
// TrialName are required.
type disassociateTrialComponentInput struct {
	TrialComponentName string `json:"TrialComponentName"`
	TrialName          string `json:"TrialName"`
}

func (h *Handler) handleDisassociateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req disassociateTrialComponentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	trialArn, trialComponentArn, err := h.Backend.DisassociateTrialComponent(
		ctx, req.TrialName, req.TrialComponentName,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{
		keyTrialArn:          trialArn,
		keyTrialComponentArn: trialComponentArn,
	})
}

// listTrialComponentsInput mirrors ListTrialComponentsInput
// (api_op_ListTrialComponents.go:30-71), all members optional. SourceArn is
// decoded for wire-shape fidelity but is a disclosed no-op — see
// ListTrialComponentsParams' doc comment in trial_components.go.
type listTrialComponentsInput struct {
	CreatedAfter   *float64 `json:"CreatedAfter,omitempty"`
	CreatedBefore  *float64 `json:"CreatedBefore,omitempty"`
	ExperimentName string   `json:"ExperimentName,omitempty"`
	TrialName      string   `json:"TrialName,omitempty"`
	SourceArn      string   `json:"SourceArn,omitempty"`
	SortBy         string   `json:"SortBy,omitempty"`
	SortOrder      string   `json:"SortOrder,omitempty"`
	NextToken      string   `json:"NextToken,omitempty"`
	MaxResults     int32    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListTrialComponents(ctx context.Context, body []byte) ([]byte, error) {
	var req listTrialComponentsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, nextToken := h.Backend.ListTrialComponents(ctx, ListTrialComponentsParams{
		ExperimentName: req.ExperimentName,
		TrialName:      req.TrialName,
		CreatedAfter:   timeFromEpochSecondsPtr(req.CreatedAfter),
		CreatedBefore:  timeFromEpochSecondsPtr(req.CreatedBefore),
		SortBy:         req.SortBy,
		SortOrder:      req.SortOrder,
		NextToken:      req.NextToken,
		MaxResults:     req.MaxResults,
	})

	summaries := make([]map[string]any, 0, len(items))

	for _, tc := range items {
		summary := map[string]any{
			keyTrialComponentArn: tc.TrialComponentArn,
			"TrialComponentName": tc.TrialComponentName,
			keyCreationTime:      epochSeconds(tc.CreationTime),
			keyLastModifiedTime:  epochSeconds(tc.LastModifiedTime),
		}
		if tc.DisplayName != "" {
			summary["DisplayName"] = tc.DisplayName
		}
		if tc.Status != nil {
			summary["Status"] = tc.Status
		}
		if tc.StartTime != nil {
			summary["StartTime"] = epochSeconds(*tc.StartTime)
		}
		if tc.EndTime != nil {
			summary["EndTime"] = epochSeconds(*tc.EndTime)
		}

		summaries = append(summaries, summary)
	}

	return listResp("TrialComponentSummaries", summaries, nextToken)
}

// updateTrialComponentInput mirrors UpdateTrialComponentInput
// (api_op_UpdateTrialComponent.go:28-70): TrialComponentName is required,
// every other member optional.
type updateTrialComponentInput struct {
	StartTime               *float64                          `json:"StartTime,omitempty"`
	EndTime                 *float64                          `json:"EndTime,omitempty"`
	Status                  *TrialComponentStatus             `json:"Status,omitempty"`
	Parameters              map[string]TrialComponentValue    `json:"Parameters,omitempty"`
	InputArtifacts          map[string]TrialComponentArtifact `json:"InputArtifacts,omitempty"`
	OutputArtifacts         map[string]TrialComponentArtifact `json:"OutputArtifacts,omitempty"`
	TrialComponentName      string                            `json:"TrialComponentName"`
	DisplayName             string                            `json:"DisplayName,omitempty"`
	ParametersToRemove      []string                          `json:"ParametersToRemove,omitempty"`
	InputArtifactsToRemove  []string                          `json:"InputArtifactsToRemove,omitempty"`
	OutputArtifactsToRemove []string                          `json:"OutputArtifactsToRemove,omitempty"`
}

func (h *Handler) handleUpdateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req updateTrialComponentInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	opts := UpdateTrialComponentOptions{
		DisplayName:             req.DisplayName,
		Status:                  req.Status,
		StartTime:               timeFromEpochSecondsPtr(req.StartTime),
		EndTime:                 timeFromEpochSecondsPtr(req.EndTime),
		Parameters:              req.Parameters,
		InputArtifacts:          req.InputArtifacts,
		OutputArtifacts:         req.OutputArtifacts,
		ParametersToRemove:      req.ParametersToRemove,
		InputArtifactsToRemove:  req.InputArtifactsToRemove,
		OutputArtifactsToRemove: req.OutputArtifactsToRemove,
	}

	tc, err := h.Backend.UpdateTrialComponent(ctx, req.TrialComponentName, opts)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(
		ctx,
		"sagemaker: updated trial component",
		"name",
		tc.TrialComponentName,
	)

	return json.Marshal(map[string]string{keyTrialComponentArn: tc.TrialComponentArn})
}
