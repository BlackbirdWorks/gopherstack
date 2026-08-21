package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// Trial handlers
// ---------------------------------------------------------------------------

// createTrialInput mirrors CreateTrialInput (api_op_CreateTrial.go:45-71).
type createTrialInput struct {
	MetadataProperties *MetadataProperties `json:"MetadataProperties"`
	TrialName          string              `json:"TrialName"`
	ExperimentName     string              `json:"ExperimentName"`
	DisplayName        string              `json:"DisplayName,omitempty"`
	Tags               []tagObject         `json:"Tags"`
}

func (h *Handler) handleCreateTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req createTrialInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	t, err := h.Backend.CreateTrial(
		ctx, req.TrialName, req.ExperimentName, req.DisplayName, req.MetadataProperties, fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created trial", "name", t.TrialName)

	return json.Marshal(map[string]string{keyTrialArn: t.TrialArn})
}

// describeTrialInput mirrors DescribeTrialInput (api_op_DescribeTrial.go:29-36).
type describeTrialInput struct {
	TrialName string `json:"TrialName"`
}

func (h *Handler) handleDescribeTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req describeTrialInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	t, err := h.Backend.DescribeTrial(ctx, req.TrialName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"TrialName":         t.TrialName,
		keyTrialArn:         t.TrialArn,
		keyExperimentName:   t.ExperimentName,
		keyCreationTime:     epochSeconds(t.CreationTime),
		keyLastModifiedTime: epochSeconds(t.LastModifiedTime),
	}
	if t.DisplayName != "" {
		resp["DisplayName"] = t.DisplayName
	}

	if t.MetadataProperties != nil {
		resp["MetadataProperties"] = t.MetadataProperties
	}

	return json.Marshal(resp)
}

type trialSummary struct {
	TrialName        string  `json:"TrialName"`
	TrialArn         string  `json:"TrialArn"`
	DisplayName      string  `json:"DisplayName,omitempty"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

// listTrialsInput mirrors ListTrialsInput (api_op_ListTrials.go:34-63).
type listTrialsInput struct {
	CreatedAfter       *float64 `json:"CreatedAfter,omitempty"`
	CreatedBefore      *float64 `json:"CreatedBefore,omitempty"`
	NextToken          string   `json:"NextToken"`
	ExperimentName     string   `json:"ExperimentName"`
	TrialComponentName string   `json:"TrialComponentName"`
	SortBy             string   `json:"SortBy"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListTrials(ctx context.Context, body []byte) ([]byte, error) {
	var req listTrialsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ts, nextToken := h.Backend.ListTrials(ctx, req.NextToken, ListTrialsFilter{
		ExperimentName:     req.ExperimentName,
		TrialComponentName: req.TrialComponentName,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		MaxResults:         req.MaxResults,
		CreatedAfter:       timeFromEpochSecondsPtr(req.CreatedAfter),
		CreatedBefore:      timeFromEpochSecondsPtr(req.CreatedBefore),
	})
	summaries := make([]trialSummary, 0, len(ts))

	for _, t := range ts {
		summaries = append(summaries, trialSummary{
			TrialName:        t.TrialName,
			TrialArn:         t.TrialArn,
			DisplayName:      t.DisplayName,
			CreationTime:     epochSeconds(t.CreationTime),
			LastModifiedTime: epochSeconds(t.LastModifiedTime),
		})
	}

	resp := map[string]any{"TrialSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// deleteTrialInput mirrors DeleteTrialInput (api_op_DeleteTrial.go:30-37).
type deleteTrialInput struct {
	TrialName string `json:"TrialName"`
}

func (h *Handler) handleDeleteTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteTrialInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	t, err := h.Backend.DeleteTrial(ctx, req.TrialName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted trial", "name", req.TrialName)

	return json.Marshal(map[string]string{keyTrialArn: t.TrialArn})
}

// updateTrialInput mirrors UpdateTrialInput (api_op_UpdateTrial.go:27-39).
type updateTrialInput struct {
	TrialName   string `json:"TrialName"`
	DisplayName string `json:"DisplayName,omitempty"`
}

func (h *Handler) handleUpdateTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req updateTrialInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	t, err := h.Backend.UpdateTrial(ctx, req.TrialName, req.DisplayName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated trial", "name", t.TrialName)

	return json.Marshal(map[string]string{keyTrialArn: t.TrialArn})
}
