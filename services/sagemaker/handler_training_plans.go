package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// TrainingPlan handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateTrainingPlan(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrainingPlanName                 string      `json:"TrainingPlanName"`
		TrainingPlanOfferingID           string      `json:"TrainingPlanOfferingId"`
		Tags                             []tagObject `json:"Tags"`
		SpareInstanceCountPerUltraServer int32       `json:"SpareInstanceCountPerUltraServer"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingPlanName == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateTrainingPlan(
		ctx, req.TrainingPlanName, req.TrainingPlanOfferingID,
		req.SpareInstanceCountPerUltraServer, fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrainingPlanArn: result.TrainingPlanArn})
}

func (h *Handler) handleDescribeTrainingPlan(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrainingPlanName string `json:"TrainingPlanName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingPlanName == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeTrainingPlan(ctx, req.TrainingPlanName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}
