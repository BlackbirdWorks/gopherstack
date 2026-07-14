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

func (h *Handler) handleCreateTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialName      string      `json:"TrialName"`
		ExperimentName string      `json:"ExperimentName"`
		Tags           []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	t, err := h.Backend.CreateTrial(ctx, req.TrialName, req.ExperimentName, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created trial", "name", t.TrialName)

	return json.Marshal(map[string]string{keyTrialArn: t.TrialArn})
}

func (h *Handler) handleDescribeTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialName string `json:"TrialName"`
	}

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

	return json.Marshal(resp)
}

type trialSummary struct {
	TrialName    string  `json:"TrialName"`
	TrialArn     string  `json:"TrialArn"`
	CreationTime float64 `json:"CreationTime"`
}

func (h *Handler) handleListTrials(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ts, nextToken := h.Backend.ListTrials(ctx, req.NextToken)
	summaries := make([]trialSummary, 0, len(ts))

	for _, t := range ts {
		summaries = append(summaries, trialSummary{
			TrialName:    t.TrialName,
			TrialArn:     t.TrialArn,
			CreationTime: epochSeconds(t.CreationTime),
		})
	}

	resp := map[string]any{"TrialSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialName string `json:"TrialName"`
	}

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

func (h *Handler) handleUpdateTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialName   string `json:"TrialName"`
		DisplayName string `json:"DisplayName,omitempty"`
	}

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
