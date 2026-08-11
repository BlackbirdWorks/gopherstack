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

func (h *Handler) handleCreateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		StartTime          *float64                          `json:"StartTime,omitempty"`
		EndTime            *float64                          `json:"EndTime,omitempty"`
		Status             *TrialComponentStatus             `json:"Status,omitempty"`
		Parameters         map[string]TrialComponentValue    `json:"Parameters,omitempty"`
		InputArtifacts     map[string]TrialComponentArtifact `json:"InputArtifacts,omitempty"`
		OutputArtifacts    map[string]TrialComponentArtifact `json:"OutputArtifacts,omitempty"`
		TrialComponentName string                            `json:"TrialComponentName"`
		DisplayName        string                            `json:"DisplayName,omitempty"`
		Tags               []tagObject                       `json:"Tags"`
	}

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
		Tags:               fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: created trial component", "name", tc.TrialComponentName)

	return json.Marshal(map[string]string{keyTrialComponentArn: tc.TrialComponentArn})
}

func (h *Handler) handleDescribeTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialComponentName string `json:"TrialComponentName"`
	}

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

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialComponentName string `json:"TrialComponentName"`
	}

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

func (h *Handler) handleDisassociateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialComponentName string `json:"TrialComponentName"`
		TrialName          string `json:"TrialName"`
	}

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

func (h *Handler) handleListTrialComponents(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ExperimentName string `json:"ExperimentName,omitempty"`
		TrialName      string `json:"TrialName,omitempty"`
		NextToken      string `json:"NextToken,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, nextToken := h.Backend.ListTrialComponents(ctx, req.ExperimentName, req.TrialName, req.NextToken)

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

func (h *Handler) handleUpdateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		StartTime          *float64                          `json:"StartTime,omitempty"`
		EndTime            *float64                          `json:"EndTime,omitempty"`
		Status             *TrialComponentStatus             `json:"Status,omitempty"`
		Parameters         map[string]TrialComponentValue    `json:"Parameters,omitempty"`
		InputArtifacts     map[string]TrialComponentArtifact `json:"InputArtifacts,omitempty"`
		OutputArtifacts    map[string]TrialComponentArtifact `json:"OutputArtifacts,omitempty"`
		TrialComponentName string                            `json:"TrialComponentName"`
		DisplayName        string                            `json:"DisplayName,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	opts := UpdateTrialComponentOptions{
		DisplayName:     req.DisplayName,
		Status:          req.Status,
		StartTime:       timeFromEpochSecondsPtr(req.StartTime),
		EndTime:         timeFromEpochSecondsPtr(req.EndTime),
		Parameters:      req.Parameters,
		InputArtifacts:  req.InputArtifacts,
		OutputArtifacts: req.OutputArtifacts,
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
