package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// TrainingPlan handlers
// ---------------------------------------------------------------------------

// createTrainingPlanInput mirrors CreateTrainingPlanInput
// (api_op_CreateTrainingPlan.go): TrainingPlanName and TrainingPlanOfferingId
// are both "This member is required" -- only TrainingPlanName was previously
// enforced, so a request naming no offering silently created a minimal plan
// with no backing reserved capacity instead of being rejected.
type createTrainingPlanInput struct {
	TrainingPlanName                 string      `json:"TrainingPlanName"`
	TrainingPlanOfferingID           string      `json:"TrainingPlanOfferingId"`
	Tags                             []tagObject `json:"Tags"`
	SpareInstanceCountPerUltraServer int32       `json:"SpareInstanceCountPerUltraServer"`
}

func (h *Handler) handleCreateTrainingPlan(ctx context.Context, body []byte) ([]byte, error) {
	var req createTrainingPlanInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingPlanName == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", errInvalidRequest)
	}

	if req.TrainingPlanOfferingID == "" {
		return nil, fmt.Errorf("%w: TrainingPlanOfferingId is required", errInvalidRequest)
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

// describeTrainingPlanInput mirrors DescribeTrainingPlanInput
// (api_op_DescribeTrainingPlan.go): TrainingPlanName is its sole, required
// member.
type describeTrainingPlanInput struct {
	TrainingPlanName string `json:"TrainingPlanName"`
}

func (h *Handler) handleDescribeTrainingPlan(ctx context.Context, body []byte) ([]byte, error) {
	var req describeTrainingPlanInput

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
