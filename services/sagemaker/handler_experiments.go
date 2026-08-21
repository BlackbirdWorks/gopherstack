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

// createExperimentInput mirrors CreateExperimentInput
// (api_op_CreateExperiment.go:61-83).
type createExperimentInput struct {
	ExperimentName string      `json:"ExperimentName"`
	DisplayName    string      `json:"DisplayName,omitempty"`
	Description    string      `json:"Description,omitempty"`
	Tags           []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req createExperimentInput

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

// describeExperimentInput mirrors DescribeExperimentInput
// (api_op_DescribeExperiment.go:29-37).
type describeExperimentInput struct {
	ExperimentName string `json:"ExperimentName"`
}

func (h *Handler) handleDescribeExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req describeExperimentInput

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

// listExperimentsInput mirrors ListExperimentsInput
// (api_op_ListExperiments.go:32-55). Previously this decoded only NextToken.
type listExperimentsInput struct {
	CreatedAfter  *float64 `json:"CreatedAfter"`
	CreatedBefore *float64 `json:"CreatedBefore"`
	SortBy        string   `json:"SortBy"`
	SortOrder     string   `json:"SortOrder"`
	NextToken     string   `json:"NextToken"`
	MaxResults    int32    `json:"MaxResults"`
}

func (h *Handler) handleListExperiments(ctx context.Context, body []byte) ([]byte, error) {
	var req listExperimentsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	exps, nextToken := h.Backend.ListExperiments(ctx, req.NextToken, ListExperimentsFilter{
		CreatedAfter:  epochPtr(req.CreatedAfter),
		CreatedBefore: epochPtr(req.CreatedBefore),
		SortBy:        req.SortBy,
		SortOrder:     req.SortOrder,
		MaxResults:    req.MaxResults,
	})
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

// deleteExperimentInput mirrors DeleteExperimentInput
// (api_op_DeleteExperiment.go:31-39).
type deleteExperimentInput struct {
	ExperimentName string `json:"ExperimentName"`
}

func (h *Handler) handleDeleteExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteExperimentInput

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

// updateExperimentInput mirrors UpdateExperimentInput
// (api_op_UpdateExperiment.go:28-43): DisplayName/Description are *string
// so an explicit "" (clear) can be told apart from an omitted key (no
// change) -- see [InMemoryBackend.UpdateExperiment]'s doc for the bug this
// fixes.
type updateExperimentInput struct {
	DisplayName    *string `json:"DisplayName"`
	Description    *string `json:"Description"`
	ExperimentName string  `json:"ExperimentName"`
}

func (h *Handler) handleUpdateExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req updateExperimentInput

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
