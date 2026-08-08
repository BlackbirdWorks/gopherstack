package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// Experiment handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ExperimentName string      `json:"ExperimentName"`
		DisplayName    string      `json:"DisplayName,omitempty"`
		Description    string      `json:"Description,omitempty"`
		Tags           []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ExperimentName == "" {
		return nil, fmt.Errorf("%w: ExperimentName is required", errInvalidRequest)
	}

	e, err := h.Backend.CreateExperiment(
		ctx, req.ExperimentName, req.DisplayName, req.Description, fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created experiment", "name", e.ExperimentName)

	return json.Marshal(map[string]string{keyExperimentArn: e.ExperimentArn})
}

func (h *Handler) handleDescribeExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ExperimentName string `json:"ExperimentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ExperimentName == "" {
		return nil, fmt.Errorf("%w: ExperimentName is required", errInvalidRequest)
	}

	e, err := h.Backend.DescribeExperiment(ctx, req.ExperimentName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyExperimentName:   e.ExperimentName,
		keyExperimentArn:    e.ExperimentArn,
		keyCreationTime:     epochSeconds(e.CreationTime),
		keyLastModifiedTime: epochSeconds(e.LastModifiedTime),
	}
	if e.DisplayName != "" {
		resp["DisplayName"] = e.DisplayName
	}
	if e.Description != "" {
		resp["Description"] = e.Description
	}

	return json.Marshal(resp)
}

type experimentSummary struct {
	ExperimentName   string  `json:"ExperimentName"`
	ExperimentArn    string  `json:"ExperimentArn"`
	DisplayName      string  `json:"DisplayName,omitempty"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListExperiments(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	exps, nextToken := h.Backend.ListExperiments(ctx, req.NextToken)
	summaries := make([]experimentSummary, 0, len(exps))

	for _, e := range exps {
		summaries = append(summaries, experimentSummary{
			ExperimentName:   e.ExperimentName,
			ExperimentArn:    e.ExperimentArn,
			DisplayName:      e.DisplayName,
			CreationTime:     epochSeconds(e.CreationTime),
			LastModifiedTime: epochSeconds(e.LastModifiedTime),
		})
	}

	resp := map[string]any{"ExperimentSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ExperimentName string `json:"ExperimentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ExperimentName == "" {
		return nil, fmt.Errorf("%w: ExperimentName is required", errInvalidRequest)
	}

	e, err := h.Backend.DeleteExperiment(ctx, req.ExperimentName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted experiment", "name", req.ExperimentName)

	return json.Marshal(map[string]string{keyExperimentArn: e.ExperimentArn})
}

// ---------------------------------------------------------------------------
// UpdateExperiment, UpdateTrial, UpdateTrialComponent handlers (gap #26)
// ---------------------------------------------------------------------------

func (h *Handler) handleUpdateExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ExperimentName string `json:"ExperimentName"`
		DisplayName    string `json:"DisplayName,omitempty"`
		Description    string `json:"Description,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ExperimentName == "" {
		return nil, fmt.Errorf("%w: ExperimentName is required", errInvalidRequest)
	}

	e, err := h.Backend.UpdateExperiment(ctx, req.ExperimentName, req.DisplayName, req.Description)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated experiment", "name", e.ExperimentName)

	return json.Marshal(map[string]string{keyExperimentArn: e.ExperimentArn})
}
